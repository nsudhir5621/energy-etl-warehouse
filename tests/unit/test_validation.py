"""Validation severity, and the Phase 0 gate on the pipeline's own batch.

The distinction under test: a finding that means *we cannot trust this data*
stops the run; a finding that means *the seed is out of date* is recorded and
does not. A pipeline that halts because EIA added a census division is a
pipeline that gets switched off.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from energy_pipeline.extract import FixtureSource, extract_fixture
from energy_pipeline.records import (
    classify_energy_source,
    classify_geography,
    classify_sector,
    dedupe,
    parse_record,
    parse_records,
)
from energy_pipeline.validation import Severity, aggregate_publish_gate, validate

WINDOW = ("2026-01", "2026-07")


def _validated(fixture):
    rows = FixtureSource(fixture).rows
    records, errors = parse_records(rows)
    result = dedupe(records)
    return validate(
        result.records,
        parse_errors=errors,
        dedupe_result=result,
        source_record_total=len(rows),
        window_start=WINDOW[0],
        window_end=WINDOW[1],
    )


def test_a_clean_batch_passes(edge_cases_fixture):
    report = _validated(edge_cases_fixture)
    assert report.ok is True
    assert report.hard_failures == []


def test_parse_errors_are_hard_failures():
    report = validate([], parse_errors=["row 0: generation 'oops' is neither null nor a number"])

    assert report.ok is False
    assert report.hard_failures[0].check == "row_contract"


def _row(**overrides):
    row = {
        "period": "2026-07",
        "location": "AK",
        "stateDescription": "Alaska",
        "sectorid": "1",
        "sectorDescription": "Electric Utility",
        "fueltypeid": "NG",
        "fuelTypeDescription": "Natural Gas",
        "generation": "1.0",
        "generation-units": "thousand megawatthours",
    }
    return {**row, **overrides}


@pytest.mark.parametrize(
    "generation",
    [
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
    ],
)
def test_nonfinite_generation_in_typed_record_is_a_hard_failure(generation):
    record = replace(parse_record(_row()), generation=generation)
    report = validate([record])

    assert report.ok is False
    assert report.hard_failures


@pytest.mark.parametrize("units", ["megawatthours", "", None])
def test_unsupported_units_in_typed_record_are_a_hard_failure(units):
    record = replace(parse_record(_row()), generation_units=units)
    report = validate([record])

    assert report.ok is False
    assert report.hard_failures


@pytest.mark.parametrize("generation", [None, 0.0, -1.25, 1.25])
def test_null_and_finite_generation_with_canonical_units_pass_validation(generation):
    record = replace(parse_record(_row()), generation=generation)
    report = validate([record])

    assert report.ok is True
    assert report.hard_failures == []
    assert record.generation_units == "thousand megawatthours"


def test_an_unrecognised_code_warns_but_does_not_stop_the_run():
    """EIA adding a census division is routine. Halting on it would be worse
    than continuing with a recorded finding — and dropping the row would lose
    real source data over a missing dimension member."""
    report = validate([parse_record(_row(location="XYZ"))])

    assert report.ok is True
    assert [f.check for f in report.findings if f.severity is Severity.WARN] == [
        "unclassified_geography"
    ]


def test_fabricated_geography_with_known_sector_and_fuel_is_not_leaf():
    record = parse_record(_row(location="ZZ", sectorid="1", fueltypeid="NG"))

    assert record.sector_class == "leaf"
    assert record.energy_source_class == "leaf"
    assert record.is_leaf is False
    assert record.is_unknown_code is True


def test_fabricated_geography_with_known_sector_and_fuel_warns():
    record = parse_record(_row(location="ZZ", sectorid="1", fueltypeid="NG"))
    report = validate([record])

    assert report.ok is True, "unknown geography is allowed at ingestion"
    assert [f.check for f in report.findings if f.severity is Severity.WARN] == [
        "unclassified_geography"
    ]
    assert report.unknown_codes == {"geography": {"ZZ"}}
    assert report.unknown_row_count == 1
    assert report.leaf_row_count == 0


def test_fabricated_geography_alone_blocks_aggregate_publication():
    record = parse_record(_row(location="ZZ", sectorid="1", fueltypeid="NG"))
    blocked = aggregate_publish_gate([record])

    assert len(blocked) == 1
    assert blocked[0].severity is Severity.HARD
    assert blocked[0].check == "unclassified_geography"
    assert "ZZ" in blocked[0].message


def test_an_unrecognised_energy_source_is_ingested_but_never_treated_as_leaf():
    """The silent-double-count failure mode. A new 3-character code we cannot
    classify must not default to base/leaf: if EIA ships a new aggregate and we
    treat it as a fuel, every national total silently doubles."""
    record = parse_record(_row(fueltypeid="XYZ"))

    assert record.energy_source_class == "unknown"
    assert record.is_rollup_energy_source is False
    assert record.is_leaf is False, "an unclassified code is not a leaf"
    assert record.is_rollup is False, "and it is not claimed as a rollup either"
    assert record.is_unknown_code is True

    report = validate([record])
    assert report.ok is True, "raw ingestion allows the row"
    assert report.unknown_row_count == 1
    assert report.leaf_row_count == 0, "and it is not counted as a leaf row"
    assert report.unknown_codes == {"energy_source": {"XYZ"}}


UNKNOWN_WITH_ROLLUP_CASES = [
    ("US", "95", "NG", ("rollup", "unknown", "leaf"), "sector", "95"),
    ("US", "1", "ZZZ", ("rollup", "leaf", "unknown"), "energy_source", "ZZZ"),
    ("AK", "95", "ALL", ("leaf", "unknown", "rollup"), "sector", "95"),
]


@pytest.mark.parametrize(
    "location, sectorid, fueltypeid, expected_classes, dimension, code",
    UNKNOWN_WITH_ROLLUP_CASES,
    ids=["US-95-NG", "US-1-ZZZ", "AK-95-ALL"],
)
def test_known_rollup_does_not_hide_unknown_code_warning(
    location, sectorid, fueltypeid, expected_classes, dimension, code
):
    record = parse_record(_row(location=location, sectorid=sectorid, fueltypeid=fueltypeid))
    classes = (record.geography_class, record.sector_class, record.energy_source_class)
    assert classes == expected_classes
    assert record.is_rollup is True
    report = validate([record])

    assert report.record_count == 1
    assert report.leaf_row_count == 0
    assert report.rollup_row_count == 1
    assert report.unknown_row_count == 1
    assert report.ok is True, "unknown codes remain allowed at ingestion"
    warnings = [f for f in report.findings if f.severity is Severity.WARN]
    assert [f.check for f in warnings] == [f"unclassified_{dimension}"]
    assert report.unknown_codes == {dimension: {code}}
    assert code in warnings[0].message


@pytest.mark.parametrize(
    "location, sectorid, fueltypeid, expected_classes, dimension, code",
    UNKNOWN_WITH_ROLLUP_CASES,
    ids=["US-95-NG", "US-1-ZZZ", "AK-95-ALL"],
)
def test_unknown_code_on_rollup_record_still_blocks_publication(
    location, sectorid, fueltypeid, expected_classes, dimension, code
):
    record = parse_record(_row(location=location, sectorid=sectorid, fueltypeid=fueltypeid))
    classes = (record.geography_class, record.sector_class, record.energy_source_class)
    assert classes == expected_classes
    blocked = aggregate_publish_gate([record])

    assert len(blocked) == 1
    assert blocked[0].severity is Severity.HARD
    assert blocked[0].check == f"unclassified_{dimension}"
    assert code in blocked[0].message


def test_known_rollup_without_unknown_codes_has_no_unknown_warning():
    record = parse_record(_row(location="US", sectorid="1", fueltypeid="NG"))
    assert (record.geography_class, record.sector_class, record.energy_source_class) == (
        "rollup",
        "leaf",
        "leaf",
    )
    assert record.is_rollup is True
    assert record.is_unknown_code is False
    report = validate([record])

    assert report.ok is True
    assert [f for f in report.findings if f.severity is Severity.WARN] == []
    assert report.unknown_codes == {}
    assert aggregate_publish_gate([record]) == []


def test_multiple_unknown_dimensions_on_rollup_count_as_one_unknown_row():
    record = parse_record(_row(location="US", sectorid="95", fueltypeid="ZZZ"))
    assert (record.geography_class, record.sector_class, record.energy_source_class) == (
        "rollup",
        "unknown",
        "unknown",
    )
    report = validate([record])

    assert report.ok is True
    assert report.record_count == 1
    assert report.leaf_row_count == 0
    assert report.rollup_row_count == 1
    assert report.unknown_row_count == 1
    assert report.unknown_codes == {"sector": {"95"}, "energy_source": {"ZZZ"}}
    assert [f.check for f in report.findings if f.severity is Severity.WARN] == [
        "unclassified_sector",
        "unclassified_energy_source",
    ]
    blocked = aggregate_publish_gate([record])
    assert [f.check for f in blocked] == ["unclassified_sector", "unclassified_energy_source"]
    assert all(f.severity is Severity.HARD for f in blocked)


def test_unknown_codes_are_visible_in_the_audit_record():
    report = validate([parse_record(_row(fueltypeid="ZZZ", sectorid="9", location="QQQ"))])
    payload = report.to_dict()

    assert payload["unknown_row_count"] == 1
    assert payload["unknown_codes"] == {
        "energy_source": ["ZZZ"],
        "geography": ["QQQ"],
        "sector": ["9"],
    }
    assert "unknown_code_rows_retained" in {f["check"] for f in payload["findings"]}


def test_an_unclassified_code_cannot_participate_in_an_aggregate():
    """Raw ingestion accepts it; publishing an aggregate must not."""
    rows = [
        parse_record(_row()),
        parse_record(_row(location="TX", fueltypeid="XYZ")),
    ]

    assert aggregate_publish_gate(rows) != [], "a known-code-only batch aggregates cleanly"
    assert aggregate_publish_gate(rows[:1]) == []

    blocked = aggregate_publish_gate(rows, context="national total")
    assert blocked[0].severity is Severity.HARD
    assert blocked[0].check == "unclassified_energy_source"
    assert "XYZ" in blocked[0].message
    assert "national total" in blocked[0].message


def test_every_dimension_classifies_into_exactly_one_of_three_states():
    """The contract itself, independent of any particular batch."""
    assert classify_energy_source("NG") == "leaf"
    assert classify_energy_source("ALL") == "rollup"
    assert classify_energy_source("AOR") == "rollup"
    assert classify_energy_source("WHAT") == "unknown"
    assert classify_geography("AK") == "leaf"
    assert classify_geography("US") == "rollup"
    assert classify_geography("PACC") == "rollup"
    assert classify_geography("ZZZZ") == "unknown"
    assert classify_sector("1") == "leaf"
    assert classify_sector("95") == "unknown"


def test_duplicate_keys_that_survived_deduplication_are_a_hard_failure(duplicates_fixture):
    rows = FixtureSource(duplicates_fixture).rows
    records, _ = parse_records(rows)
    report = validate(records)  # deliberately skipping dedupe, as if it had been forgotten

    assert report.ok is False
    assert any(f.check == "key_uniqueness" for f in report.hard_failures)


def test_a_truncated_extract_warns(monthly_fixture):
    result = extract_fixture(monthly_fixture)
    report = validate(result.records, source_record_total=10_000)

    assert report.ok is True, "a truncated read is a warning, not a corrupted batch"
    assert any(f.check == "truncated_extract" for f in report.findings)


# -- measured characteristics, carried as evidence ----------------------------


def test_nulls_negatives_and_rollups_are_counted_not_complained_about(edge_cases_fixture):
    report = _validated(edge_cases_fixture)

    assert report.null_generation_count == 2
    assert report.negative_generation_count == 2
    assert report.zero_generation_count == 1
    assert report.rollup_row_count == 6  # US, NEW, PCC, SUN + two AOR
    assert report.leaf_row_count == 5  # SUN moved from leaf to rollup (verified live)
    assert report.ok is True, "valid data must not fail validation"


def test_the_report_serialises_for_the_audit_layer(edge_cases_fixture):
    payload = _validated(edge_cases_fixture).to_dict()

    assert payload["ok"] is True
    assert payload["record_count"] == 11
    assert {f["severity"] for f in payload["findings"]} == {"info"}


@pytest.mark.parametrize(
    "fuel", ["ANT", "BIT", "RC", "SUB", "LIG", "SPV", "STH", "WNS", "WNT", "LFG", "MSB", "DPV"]
)
@pytest.mark.parametrize("generation", [None, "0", "-1.25", "2.5"])
def test_new_leaf_fuels_preserve_generation_and_publication_contract(fuel, generation):
    from energy_pipeline.load import record_to_stage_row

    record = parse_record(_row(fueltypeid=fuel, generation=generation))
    assert record.is_leaf
    assert validate([record]).ok
    assert aggregate_publish_gate([record]) == []
    staged = record_to_stage_row(record, "test-run", "source-hash")
    assert staged[5] == fuel
    assert staged[6] == (None if generation is None else float(generation))
    assert staged[7:] == (False, False, False)


@pytest.mark.parametrize("sector", ["90", "94", "96", "97", "98", "99"])
def test_sector_rollup_flags_survive_staging(sector):
    from energy_pipeline.load import record_to_stage_row

    record = parse_record(_row(sectorid=sector))
    assert aggregate_publish_gate([record]) == []
    assert not record.is_leaf
    assert record_to_stage_row(record, "test-run", "source-hash")[7:] == (False, True, False)


def test_new_fuel_components_do_not_double_count_biomass_parents():
    values = {
        "LFG": "5",
        "MSB": "2",
        "ORW": "3",
        "WWW": "4",
        "MLG": "7",
        "OBW": "5",
        "WAS": "10",
        "BIO": "14",
        "OB2": "3",
    }
    records = [parse_record(_row(fueltypeid=k, generation=v)) for k, v in values.items()]
    assert aggregate_publish_gate(records) == []
    assert sum(r.generation for r in records if r.is_leaf) == 14
    assert {r.fueltypeid for r in records if r.is_leaf} == {"LFG", "MSB", "ORW", "WWW"}


def test_new_classifications_keep_identical_retry_and_conflicting_duplicates_distinct():
    record = parse_record(_row(fueltypeid="SPV"))
    repeated = dedupe([record, record])
    assert repeated.records == [record]
    assert repeated.identical_duplicates == 1
    assert validate(repeated.records, dedupe_result=repeated).ok
    revised = replace(record, generation=2.0)
    conflicting = dedupe([record, revised])
    assert not validate(conflicting.records, dedupe_result=conflicting).ok


@pytest.mark.parametrize("field", ["location", "sectorid", "fueltypeid"])
def test_missing_identifier_remains_a_hard_parse_error(field):
    row = _row()
    del row[field]
    records, errors = parse_records([row])
    assert records == []
    assert not validate(records, parse_errors=errors).ok


@pytest.mark.parametrize("field", ["location", "sectorid", "fueltypeid"])
@pytest.mark.parametrize("bad", [None, "", "ZZZ"])
def test_invalid_identifier_never_passes_publication(field, bad):
    record = parse_record(_row(**{field: bad}))
    assert not record.is_leaf
    assert aggregate_publish_gate([record])


def test_residential_small_scale_record_is_valid_but_sector95_still_blocks_batch():
    residential = parse_record(_row(sectorid="8", fueltypeid="DPV"))
    unresolved = parse_record(_row(sectorid="95"))
    assert residential.is_leaf
    assert aggregate_publish_gate([residential]) == []
    findings = aggregate_publish_gate([residential, unresolved])
    assert len(findings) == 1 and "95" in findings[0].message
