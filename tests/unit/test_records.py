"""Proofs 2, 3, 4, 5: parsing preserves what the source actually said.

Each of these exists because the opposite behaviour is a real and easy mistake
with this source. A test that merely round-trips a value would not catch it.
"""

from __future__ import annotations

import json

import pytest

from energy_pipeline.errors import ValidationError
from energy_pipeline.extract import FixtureSource
from energy_pipeline.records import (
    KNOWN_LEAF_GEOGRAPHIES,
    ROLLUP_GEOGRAPHIES,
    classify_geography,
    parse_record,
    parse_records,
)

# Independent contract: 50 states in state-name order, then DC and Puerto Rico.
# Do not derive this expected vocabulary from the implementation's leaf set.
EXPECTED_LEAF_GEOGRAPHIES = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "PR",
)


@pytest.fixture
def cases(edge_cases_fixture):
    """The edge-case fixture indexed by its ``_case`` label."""
    return {row["_case"]: row for row in FixtureSource(edge_cases_fixture).rows}


def _parse(cases, label):
    return parse_record(cases[label])


# -- Proof 2: numeric strings are converted ----------------------------------


def test_numeric_strings_are_converted_to_numbers(cases):
    """The API returns {"generation": "331.840757"} — a JSON *string*. Passing it
    through would load STRING into the warehouse and break every aggregate."""
    record = _parse(cases, "string-typed positive number")

    assert record.generation == pytest.approx(331.840757)
    assert isinstance(record.generation, float)
    assert not isinstance(record.generation, str)


def test_conversion_is_exact_not_rounded(cases):
    record = _parse(cases, "geography rollup: U.S. Total")
    assert record.generation == 412888.123456


# -- Proof 3: null stays null --------------------------------------------------


def test_null_generation_remains_null(cases):
    """null means *no data*. Zero means *measured zero*. Collapsing one into the
    other would fabricate roughly a quarter of the dataset, since 23.9% of
    observed cells are genuine zeros."""
    record = _parse(cases, "null means no data, not zero")

    assert record.generation is None
    assert record.generation != 0


def test_null_and_zero_are_distinguishable(cases):
    nulled = _parse(cases, "null means no data, not zero")
    zeroed = _parse(cases, "zero is a measured zero and must stay zero")

    assert nulled.generation is None
    assert zeroed.generation == 0
    assert nulled.generation != zeroed.generation


# -- Proof 4: negatives are preserved ------------------------------------------


def test_negative_generation_is_preserved(cases):
    """6.15% of monthly cells are negative: pumped storage and CHP plants consume
    more than they generate. A `generation >= 0` rule would fail on valid data."""
    record = _parse(cases, "negative net generation is valid (storage/CHP net consumption)")

    assert record.generation == -2335.0
    assert record.generation < 0


def test_negative_values_are_not_clamped_to_zero(edge_cases_fixture):
    values = [parse_record(row).generation for row in FixtureSource(edge_cases_fixture).rows]
    negatives = [v for v in values if v is not None and v < 0]
    assert len(negatives) == 2, "both negative rows must survive parsing"


# -- Proof 5: rollups are retained and labelled --------------------------------


def test_geography_rollups_are_retained_not_dropped(cases):
    """US, census divisions and the US total are legitimate rows an analyst may
    want. They are *labelled*, never removed — dropping them would make the
    reconciliation total against `US`/`ALL` impossible to even attempt."""
    labels = ["geography rollup: U.S. Total", "geography rollup: census division"]
    for label in labels:
        assert _parse(cases, label).is_rollup_geography is True


def test_energy_source_aggregates_are_retained_and_labelled(cases):
    record = _parse(cases, "energy source aggregate: all renewables")
    assert record.is_rollup_energy_source is True
    assert record.fueltypeid == "AOR"


def test_leaf_rows_are_not_flagged_as_rollups(cases):
    record = _parse(cases, "string-typed positive number")
    assert record.is_rollup is False
    assert record.is_rollup_geography is False
    assert record.is_rollup_sector is False
    assert record.is_rollup_energy_source is False


def test_rollup_rows_survive_parsing_in_full(edge_cases_fixture):
    rows = FixtureSource(edge_cases_fixture).rows
    records, errors = parse_records(rows)

    assert errors == []
    assert len(records) == len(rows), "no row may be dropped for being a rollup"
    assert sum(r.is_rollup for r in records) == 6, "US, NEW, PCC, SUN + two AOR rows"


@pytest.mark.parametrize(
    "location, expected",
    [("AK", "leaf"), ("DC", "leaf"), ("PR", "leaf"), ("US", "rollup"), ("ZZ", "unknown")],
)
def test_geography_classification_requires_recognized_location(location, expected):
    assert classify_geography(location) == expected


@pytest.mark.parametrize("location", EXPECTED_LEAF_GEOGRAPHIES)
def test_every_expected_geography_leaf_classifies_as_leaf(location):
    assert classify_geography(location) == "leaf"


def test_geography_leaf_vocabulary_matches_expected_codes_exactly():
    expected = frozenset(EXPECTED_LEAF_GEOGRAPHIES)
    assert len(EXPECTED_LEAF_GEOGRAPHIES) == len(expected) == 52
    assert expected == KNOWN_LEAF_GEOGRAPHIES


@pytest.mark.parametrize("location", sorted(ROLLUP_GEOGRAPHIES))
def test_every_existing_geography_rollup_classifies_as_rollup(location):
    assert classify_geography(location) == "rollup"


def test_geography_leaf_and_rollup_vocabularies_are_disjoint():
    assert KNOWN_LEAF_GEOGRAPHIES.isdisjoint(ROLLUP_GEOGRAPHIES)


@pytest.mark.parametrize("location", ["ZZ", "AA", "A1", "ak", " AK", "AK ", ""])
def test_unsupported_geography_strings_classify_as_unknown(location):
    assert classify_geography(location) == "unknown"


def test_both_pacific_spellings_are_classified_as_rollups(cases):
    """The bulk file spells the Pacific divisions PACC/PACN; the API facet spells
    them PCC/PCN/PAC. Both must classify, or the same region would be treated as
    leaf depending on which source it came from."""
    api_spelling = _parse(cases, "geography rollup: Pacific spelled as the API facet spells it")
    assert api_spelling.is_rollup_geography is True

    from energy_pipeline.records import ROLLUP_GEOGRAPHIES

    assert {"PACC", "PACN", "PCC", "PCN", "PAC"} <= ROLLUP_GEOGRAPHIES


# -- the key ------------------------------------------------------------------


def test_business_key_is_the_natural_four_column_composite(cases):
    record = _parse(cases, "string-typed positive number")
    assert record.key == ("2026-07", "AK", "1", "NG")


def test_sector_is_never_a_rollup(edge_cases_fixture):
    """EIA's 7 sectors are mutually exclusive and collectively exhaustive, with no
    rollup member. The flag exists as a uniform contract and a tripwire."""
    from energy_pipeline.records import KNOWN_SECTORS

    assert frozenset("1234567") == KNOWN_SECTORS
    for row in FixtureSource(edge_cases_fixture).rows:
        assert parse_record(row).is_rollup_sector is False


# -- contract violations -------------------------------------------------------


@pytest.mark.parametrize(
    "mutation, match",
    [
        ({"generation": "not-a-number"}, "neither null nor a number"),
        ({"period": "2026-13"}, "not a valid YYYY-MM month"),
        ({"period": "202607"}, "not a valid YYYY-MM month"),
        ({"sectorid": None}, "missing required field"),
    ],
)
def test_contract_violations_raise_rather_than_guess(mutation, match):
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
    row.update(mutation)
    if mutation.get("sectorid", "sentinel") is None:
        del row["sectorid"]

    with pytest.raises(ValidationError, match=match):
        parse_record(row)


@pytest.mark.parametrize(
    "generation",
    [
        pytest.param(float("nan"), id="numeric-nan"),
        pytest.param(float("inf"), id="numeric-positive-infinity"),
        pytest.param(float("-inf"), id="numeric-negative-infinity"),
        pytest.param("NaN", id="string-nan"),
        pytest.param("Infinity", id="string-positive-infinity"),
        pytest.param("-Infinity", id="string-negative-infinity"),
        pytest.param("1e309", id="overflow-to-infinity"),
    ],
)
def test_nonfinite_generation_is_rejected(cases, generation):
    row = {**cases["string-typed positive number"], "generation": generation}

    with pytest.raises(ValidationError):
        parse_record(row)


@pytest.mark.parametrize("units", ["megawatthours", "", None])
def test_unsupported_generation_units_are_rejected(cases, units):
    row = {**cases["string-typed positive number"], "generation-units": units}

    with pytest.raises(ValidationError):
        parse_record(row)


@pytest.mark.parametrize(
    "generation, expected",
    [
        (None, None),
        (0, 0.0),
        ("0", 0.0),
        (-1.25, -1.25),
        ("-1.25", -1.25),
        (1.25, 1.25),
        ("1.25", 1.25),
    ],
)
def test_null_and_finite_generation_preserve_canonical_units(cases, generation, expected):
    row = {**cases["string-typed positive number"], "generation": generation}
    record = parse_record(row)

    if expected is None:
        assert record.generation is None
    else:
        assert record.generation == expected
        assert record.generation is not None
    assert record.generation_units == "thousand megawatthours"


def test_one_bad_row_is_reported_not_silently_dropped():
    good = {
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
    records, errors = parse_records([good, {**good, "generation": "oops"}])

    assert len(records) == 1
    assert len(errors) == 1
    assert "row 1" in errors[0]


def test_fixture_files_are_valid_json_with_rows(fixtures_dir):
    for path in sorted(fixtures_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload, path.name
        # Most fixtures are row lists; the verbatim capture holds a raw envelope.
        assert "rows" in payload or "envelope" in payload, path.name
