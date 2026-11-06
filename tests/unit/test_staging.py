"""Unit tests for the staging row mapping (record → stg_generation tuple).

Tests that ``record_to_stage_row`` correctly maps ``GenerationRecord`` fields
to the ``raw.stg_generation`` columns, preserving null≠zero, negatives, and
tri-state classification (rollup flags).

These are pure-function tests — no database, no store, no network. The
integration path (real psycopg3 connection) is exercised by
``scripts/checkpoint.py`` during manual verification, not by this suite.
"""

from __future__ import annotations

from energy_pipeline.load import record_to_stage_row
from energy_pipeline.records import GenerationRecord

RUN_ID = "00000000-0000-4000-8000-000000000001"
CONTENT_HASH = "deadbeef"


def _record(
    period: str = "2026-07",
    location: str = "AK",
    sectorid: str = "1",
    fueltypeid: str = "NG",
    generation: float | None = 331.84,
) -> GenerationRecord:
    """Build a minimal GenerationRecord for staging tests."""
    return GenerationRecord(
        period=period,
        location=location,
        state_description="Alaska",
        sectorid=sectorid,
        sector_description="Electric Utility",
        fueltypeid=fueltypeid,
        fuel_type_description="Natural Gas",
        generation=generation,
        generation_units="thousand megawatthours",
    )


class TestRecordToStageRow:
    """Verify the 10-column tuple matches the stg_generation schema."""

    def test_column_order_matches_schema(self):
        """Columns: run_id, content_hash, period, location, sectorid,
        fueltypeid, generation, is_rollup_geography, is_rollup_sector,
        is_rollup_energy_source — exactly the INSERT column list."""
        r = _record()
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert len(row) == 10

    def test_basic_mapping(self):
        r = _record()
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row == (
            RUN_ID,
            CONTENT_HASH,
            "2026-07",
            "AK",
            "1",
            "NG",
            331.84,
            False,
            False,
            False,
        )

    def test_run_id_and_content_hash_pass_through(self):
        """Both IDs come from the manifest, not derived from the record."""
        r = _record()
        row = record_to_stage_row(r, "run-uuid-42", "sha256-abc")
        assert row[0] == "run-uuid-42"
        assert row[1] == "sha256-abc"

    def test_null_generation_preserved(self):
        """null ≠ zero: None stays None, not 0.0."""
        r = _record(generation=None)
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[6] is None

    def test_zero_generation_preserved(self):
        """0.0 is a measured zero — stays 0.0, not None."""
        r = _record(generation=0.0)
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[6] == 0.0
        assert row[6] is not None

    def test_negative_generation_preserved(self):
        """Negatives are valid (pumped storage/CHP net consumption)."""
        r = _record(generation=-722.57)
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[6] == -722.57

    # -- Geography classification ------------------------------------------------

    def test_rollup_geography_US(self):
        r = _record(location="US")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is True  # is_rollup_geography

    def test_rollup_geography_census_division(self):
        r = _record(location="NEW")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is True

    def test_pacific_api_spelling(self):
        for code in ("PAC", "PCC", "PCN"):
            r = _record(location=code)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[7] is True, f"Pacific code {code!r} should be rollup"

    def test_pacific_bulk_spelling(self):
        for code in ("PACC", "PACN"):
            r = _record(location=code)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[7] is True, f"Pacific code {code!r} should be rollup"

    def test_leaf_geography_state_code(self):
        for loc in ("AK", "TX", "WY"):
            r = _record(location=loc)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[7] is False, f"{loc!r} should be leaf"

    # -- Energy source classification ------------------------------------------

    def test_rollup_energy_source_ALL(self):
        r = _record(fueltypeid="ALL")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[9] is True  # is_rollup_energy_source

    def test_rollup_energy_source_AOR(self):
        r = _record(fueltypeid="AOR")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[9] is True

    def test_rollup_energy_source_verified_aggregates(self):
        """All KNOWN_ENERGY_SOURCE_AGGREGATES classify as rollup."""
        from energy_pipeline.records import KNOWN_ENERGY_SOURCE_AGGREGATES

        for code in KNOWN_ENERGY_SOURCE_AGGREGATES:
            r = _record(fueltypeid=code)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[9] is True, f"{code!r} should be rollup"

    def test_leaf_energy_source_NG(self):
        r = _record(fueltypeid="NG")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[9] is False

    def test_leaf_energy_source_all_base(self):
        """All BASE_ENERGY_SOURCES classify as leaf (not rollup)."""
        from energy_pipeline.records import BASE_ENERGY_SOURCES

        for code in BASE_ENERGY_SOURCES:
            r = _record(fueltypeid=code)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[9] is False, f"{code!r} should be leaf"

    # -- Sector classification --------------------------------------------------

    def test_sector_leaf_all_known(self):
        """Sectors 1-7 are leaf - is_rollup_sector always False."""
        for sid in ("1", "2", "3", "4", "5", "6", "7"):
            r = _record(sectorid=sid)
            row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
            assert row[8] is False, f"sector {sid!r} should be leaf"

    # -- Unknown codes ----------------------------------------------------------

    def test_unknown_geography_not_rollup(self):
        """Unknown geography code — neither leaf nor rollup. Flag is False."""
        r = _record(location="ZZ")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is False  # not rollup (unknown)

    def test_unknown_energy_source_not_rollup(self):
        r = _record(fueltypeid="ZZZ")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[9] is False  # not rollup (unknown)

    def test_unknown_sector_not_rollup(self):
        r = _record(sectorid="95")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[8] is False  # not rollup (unknown)

    def test_unknown_code_all_flags_false(self):
        """Unknown in all dimensions — all three rollup flags False (not True)."""
        r = _record(location="ZZ", sectorid="95", fueltypeid="ZZZ")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is False  # geography
        assert row[8] is False  # sector
        assert row[9] is False  # energy source

    def test_unknown_code_still_produces_row(self):
        """Unknown codes ARE staged — they are accepted at ingestion.
        Only the publish gate (Stage 3) blocks them from core.fact_generation."""
        r = _record(location="ZZ", fueltypeid="ZZZ", generation=None)
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        # Row exists, is_rollup flags are all False (unknown ≠ rollup ≠ leaf)
        assert row is not None
        assert row[6] is None  # null preserved

    # -- Combined flags ---------------------------------------------------------

    def test_all_three_rollup_flags(self):
        """US (geo rollup) + ALL (energy source rollup) → two flags True."""
        r = _record(location="US", fueltypeid="ALL")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is True  # geography rollup
        assert row[8] is False  # sector leaf
        assert row[9] is True  # energy source rollup

    def test_leaf_row_all_flags_false(self):
        """A fully leaf row: all three is_rollup flags are False."""
        r = _record(location="TX", sectorid="1", fueltypeid="NG")
        row = record_to_stage_row(r, RUN_ID, CONTENT_HASH)
        assert row[7] is False
        assert row[8] is False
        assert row[9] is False
