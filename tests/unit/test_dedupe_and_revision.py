"""Proofs 6 and 7: duplicate detection before MERGE, and revision representation.

The contract these tests pin down: **at most one source row per MERGE key after
batch-level deduplication.** Identical duplicates collapse silently; conflicting
ones are reported and never resolved by guessing.
"""

from __future__ import annotations

import pytest

from energy_pipeline.extract import FixtureSource, extract_fixture
from energy_pipeline.records import dedupe, parse_record
from energy_pipeline.validation import Severity

# -- Proof 6: duplicates -------------------------------------------------------


def test_identical_duplicates_collapse(duplicates_fixture):
    """The fixture holds both kinds on purpose: 6 rows collapse, 2 conflict."""
    source = FixtureSource(duplicates_fixture)
    records = [parse_record(row) for row in source.rows]
    result = dedupe(records)

    assert len(source.rows) == 8
    assert len(result.records) == 3, "3 distinct keys must survive from 8 rows"
    assert result.identical_duplicates == 4
    assert result.conflicting_keys == [("2026-07", "CA", "1", "COL")]


def test_a_batch_of_only_identical_duplicates_reports_no_conflict(fixtures_dir):
    """Isolated from the conflict rows, identical duplicates are routine and must
    not be treated as loudly as a contradiction."""
    import json
    import tempfile
    from pathlib import Path

    rows = FixtureSource(fixtures_dir / "duplicates.json").rows[:6]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "clean_dupes.json"
        path.write_text(json.dumps({"rows": rows}), encoding="utf-8")
        result = dedupe([parse_record(row) for row in FixtureSource(path).rows])

    assert result.identical_duplicates == 4
    assert not result.has_conflicts


def test_collapsed_duplicates_keep_first_occurrence_order(duplicates_fixture):
    source = FixtureSource(duplicates_fixture)
    result = dedupe([parse_record(row) for row in source.rows])

    assert [r.key for r in result.records] == [
        ("2026-07", "AK", "1", "NG"),
        ("2026-07", "TX", "1", "NUC"),
        ("2026-07", "CA", "1", "COL"),
    ]


def test_conflicting_payloads_are_reported_not_resolved(duplicates_fixture):
    """Same key, different values: the source contradicted itself. There is no
    principled way to pick a winner, so the pipeline refuses."""
    source = FixtureSource(duplicates_fixture)
    result = dedupe([parse_record(row) for row in source.rows])

    assert result.has_conflicts
    assert result.conflicting_keys == [("2026-07", "CA", "1", "COL")]


def test_conflicting_duplicates_are_a_hard_validation_failure(duplicates_fixture):
    result = extract_fixture(duplicates_fixture, window=_window(duplicates_fixture))

    assert result.report.ok is False
    hard = [f for f in result.report.findings if f.severity is Severity.HARD]
    assert any(f.check == "key_conflict" for f in hard)


def test_merge_rows_are_unique_by_key(duplicates_fixture):
    """Whatever the input, the rows handed onward never contain a repeated key."""
    result = extract_fixture(duplicates_fixture, window=_window(duplicates_fixture))
    keys = [r.key for r in result.merge_rows]

    assert len(keys) == len(set(keys))


def test_rerunning_the_same_batch_produces_the_same_merge_rows(duplicates_fixture):
    """Idempotency at the batch level: a second extraction of unchanged content
    must not add a single row."""
    first = extract_fixture(duplicates_fixture, window=_window(duplicates_fixture))
    second = extract_fixture(duplicates_fixture, window=_window(duplicates_fixture))

    assert [r.key for r in first.merge_rows] == [r.key for r in second.merge_rows]
    assert first.batch.content_hash == second.batch.content_hash


# -- Proof 7: revisions --------------------------------------------------------


def test_a_revised_record_keeps_its_key_and_changes_its_value(fixtures_dir, monthly_fixture):
    v1 = extract_fixture(fixtures_dir / "revised_2026_07_v1.json")
    v2 = extract_fixture(fixtures_dir / "revised_2026_07_v2.json")

    before = {r.key: r.generation for r in v1.merge_rows}
    after = {r.key: r.generation for r in v2.merge_rows}

    assert set(before) == set(after), "a revision must not change the key"
    assert before[("2026-07", "AK", "1", "NG")] == pytest.approx(331.840757)
    assert after[("2026-07", "AK", "1", "NG")] == pytest.approx(339.2214)
    assert before[("2026-07", "AK", "1", "NG")] != after[("2026-07", "AK", "1", "NG")]


def test_a_revision_older_than_a_month_is_still_representable(fixtures_dir, monthly_fixture):
    """2026-06 is revised in v2. A `WHERE period > MAX(period)` strategy would
    never look at it. This is the case that proves MERGE-on-key is required."""
    v2 = extract_fixture(fixtures_dir / "revised_2026_07_v2.json")
    june = [r for r in v2.merge_rows if r.period == "2026-06"]

    assert len(june) == 1
    assert june[0].generation == pytest.approx(318.77)


def test_each_vintage_gets_a_distinct_run_id_and_content_hash(fixtures_dir):
    """Different content must be different lineage, or a revision would be filed
    under the run that first published the old value."""
    v1 = extract_fixture(fixtures_dir / "revised_2026_07_v1.json")
    v2 = extract_fixture(fixtures_dir / "revised_2026_07_v2.json")

    assert v1.batch.run_id != v2.batch.run_id
    assert v1.batch.content_hash != v2.batch.content_hash


def test_re_applying_a_revision_is_a_no_op_on_the_keys(fixtures_dir):
    """Applying v2 twice must converge, not drift. This is the batch-level half
    of idempotency; the warehouse half is the Phase 6 MERGE test."""
    v1 = extract_fixture(fixtures_dir / "revised_2026_07_v1.json")
    v2a = extract_fixture(fixtures_dir / "revised_2026_07_v2.json")
    v2b = extract_fixture(fixtures_dir / "revised_2026_07_v2.json")

    assert [r.generation for r in v2a.merge_rows] == [r.generation for r in v2b.merge_rows]
    assert {r.key: r.generation for r in v2a.merge_rows} == {
        r.key: r.generation for r in v2b.merge_rows
    }
    assert {r.key: r.generation for r in v1.merge_rows} != {
        r.key: r.generation for r in v2a.merge_rows
    }


def _window(fixture_path):
    from energy_pipeline.extract import PeriodWindow

    latest = FixtureSource(fixture_path).latest_period()
    return PeriodWindow(start=latest, end=latest)
