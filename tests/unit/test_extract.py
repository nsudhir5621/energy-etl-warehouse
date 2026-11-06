"""End-to-end extraction, offline, plus a check against the real captured response.

The verbatim fixture matters: everything else is a reconstruction of the API's
shape, and a reconstruction can be wrong in exactly the way that lets a bug
through. That file was captured from a live response during Phase 0.
"""

from __future__ import annotations

import json

import pytest

from energy_pipeline.config import Settings
from energy_pipeline.extract import (
    ApiSource,
    FixtureSource,
    PeriodWindow,
    extract,
    extract_fixture,
)
from energy_pipeline.pagination import parse_envelope
from energy_pipeline.records import parse_record

EXTRACTED_AT = "2026-09-30T12:00:00Z"


# -- the real response ---------------------------------------------------------


def test_the_verbatim_live_response_parses_against_its_own_envelope(fixtures_dir):
    """Drives the real captured payload through the real parsing path."""
    captured = json.loads(
        (fixtures_dir / "verbatim_api_response.json").read_text(encoding="utf-8")
    )["envelope"]

    page = parse_envelope(captured, page_number=1, offset=0, requested_length=2)

    assert page.total == 4_771_692, "the live total is a fact about the source"
    assert len(page.rows) == 2
    assert any("incomplete return" in w for w in page.warnings)


def test_the_verbatim_live_response_yields_the_values_phase_0_reconciled(fixtures_dir):
    """Alaska 2026-07, sector 1, all fuels = 594,571.04 MWh. This exact value was
    reconciled against the EIA-923 bulk source file in Phase 0."""
    captured = _verbatim(fixtures_dir)

    records = [parse_record(row) for row in captured["response"]["data"]]

    all_fuels, renewables = records
    assert all_fuels.generation == pytest.approx(594.57104)
    assert all_fuels.generation * 1000 == pytest.approx(594_571.04, rel=1e-9)
    assert renewables.generation is None, "null is preserved exactly as the source sent it"
    assert all_fuels.is_rollup_energy_source is True
    assert renewables.is_rollup_energy_source is True


def test_the_live_source_reports_a_two_month_publication_lag(fixtures_dir):
    """A freshness check must be measured against the expected latest period, not
    against today's date. This pins the lag observed on 2026-09-30."""
    captured = _verbatim(fixtures_dir)

    assert captured["response"]["data"][0]["period"] == "2026-07"
    assert captured["response"]["frequency"] == "monthly"
    assert captured["response"]["dateFormat"] == "YYYY-MM"


def _verbatim(fixtures_dir):
    return json.loads((fixtures_dir / "verbatim_api_response.json").read_text(encoding="utf-8"))[
        "envelope"
    ]


# -- the offline path ----------------------------------------------------------


def test_extraction_runs_end_to_end_offline(monthly_fixture, settings):
    result = extract_fixture(
        monthly_fixture,
        settings,
        window=PeriodWindow("2026-01", "2026-08"),
        extracted_at=EXTRACTED_AT,
    )

    assert result.report.ok is True
    assert result.batch.record_count == 10
    assert len(result.merge_rows) == 10
    assert len(result.parse_errors) == 0
    assert (result.window.start, result.window.end) == ("2026-01", "2026-08")


def test_the_default_window_is_a_rolling_lookback_from_the_latest_period(monthly_fixture, settings):
    """No explicit window means: derive the range from what the source says is
    newest, never from today's date."""
    result = extract_fixture(monthly_fixture, settings, extracted_at=EXTRACTED_AT)

    assert result.window.end == "2026-08", "the source's latest period"
    assert result.window.start == "2024-09", "24 months back, inclusive of the end"


def test_the_summary_reports_the_full_extraction_accounting(monthly_fixture, settings):
    summary = extract_fixture(monthly_fixture, settings, extracted_at=EXTRACTED_AT).summary()

    assert summary["rows_extracted"] == 10
    assert summary["rows_parsed"] == 10
    assert summary["rows_after_deduplication"] == 10
    assert summary["identical_duplicates_collapsed"] == 0
    assert summary["conflicting_keys"] == 0
    assert summary["validation"]["ok"] is True


def test_pagination_covers_every_row_with_a_small_page_size(monthly_fixture, settings):
    """page_size=3 against 10 rows must page four times and lose nothing."""
    result = extract_fixture(monthly_fixture, settings, extracted_at=EXTRACTED_AT)

    assert len(result.batch.pages) == 4
    assert [p.offset for p in result.batch.pages] == [0, 3, 6, 9]
    assert len(result.merge_rows) == 10


def test_the_latest_period_comes_from_the_source_not_the_calendar(monthly_fixture, settings):
    source = FixtureSource(monthly_fixture)
    assert source.latest_period() == "2026-08"

    # Inferring it from today's date would be wrong by two months today, and would
    # silently change whenever EIA changes its publication lag.
    window = PeriodWindow.lookback(source.latest_period(), 2)
    assert (window.start, window.end) == ("2026-07", "2026-08")


def test_fixture_and_api_sources_share_one_interface(monthly_fixture, settings):
    """A fixture-only code path would let a bug live in the production path until
    the first live run — which is what a fixture is supposed to prevent."""
    source = FixtureSource(monthly_fixture)
    result = extract(
        source, settings, PeriodWindow("2026-01", "2026-08"), extracted_at=EXTRACTED_AT
    )

    assert result.report.ok is True
    assert len(result.merge_rows) == 10
    assert ApiSource is not FixtureSource


def test_api_source_builds_the_documented_query_parameters():
    """Guards the request shape against drift, without sending anything.

    The sort keys are the *indexed* form the live API requires: it
    rejects the flat "sort[column]" form with HTTP 400 ("Invalid sort
    index 'column'"), measured live on 2026-10-07. Fixtures never
    validate parameters, so this test is the only thing standing
    between the correct format and a silently broken live path."""
    sent: list[dict] = []

    class RecordingClient:
        def get(self, params):
            sent.append(params)
            return {"response": {"total": "0", "data": []}}

    source = ApiSource(
        RecordingClient(),  # type: ignore[arg-type]
        Settings(api_key="k", page_size=5000),
        PeriodWindow("2024-08", "2026-07"),
    )
    source.fetch_page(0, 5000)
    source.latest_period()

    assert sent[0] == {
        "frequency": "monthly",
        "data[0]": "generation",
        "start": "2024-08",
        "end": "2026-07",
        "offset": 0,
        "length": 5000,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    assert sent[1]["sort[0][column]"] == "period"
    assert sent[1]["sort[0][direction]"] == "desc"
    assert sent[1]["length"] == 1
    assert "api_key" not in sent[0], "the client injects the key, not the source"


@pytest.mark.parametrize("facets", [{"location": "TX", "sectorid": "1"}, {"fueltypeid": "NG"}])
def test_api_source_applies_independent_scope_to_pages_and_discovery(facets):
    from unittest.mock import Mock

    client = Mock()
    client.get.return_value = {"response": {"data": []}}
    original = dict(facets)
    source = ApiSource(
        client, Settings(api_key="offline"), PeriodWindow("2026-07", "2026-07"), facets=facets
    )
    facets.clear()
    source.fetch_page(0, 50)
    source.latest_period()
    for call in client.get.call_args_list:
        for key, value in original.items():
            assert call.args[0][f"facets[{key}][]"] == value
    assert client.get.call_args_list[0].args[0]["start"] == "2026-07"
    assert "start" not in client.get.call_args_list[1].args[0]


@pytest.mark.parametrize(
    "facets", [{"bad": "TX"}, {"location": ""}, {"location": " TX"}, {"sectorid": None}]
)
def test_api_source_rejects_ambiguous_scope_before_requests(facets):
    from unittest.mock import Mock

    client = Mock()
    with pytest.raises(ValueError):
        ApiSource(
            client, Settings(api_key="offline"), PeriodWindow("2026-07", "2026-07"), facets=facets
        )
    client.get.assert_not_called()


def test_explicit_extraction_page_budget_does_not_accept_partial_results(monthly_fixture):
    from energy_pipeline.errors import MalformedResponseError

    source = FixtureSource(monthly_fixture)
    with pytest.raises(MalformedResponseError, match="within 1 pages"):
        extract(
            source,
            Settings(api_key=None, page_size=3),
            PeriodWindow("2026-06", "2026-08"),
            max_pages=1,
        )
    assert source.requests == [(0, 3)]
