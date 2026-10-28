"""Proof 1: pagination combines pages correctly.

The API caps JSON responses at 5000 rows and signals that a response is
truncated with an ``incomplete return`` warning. That warning reflects the
5000-row-per-response cap, not the completeness of the result set: EIA may
attach it to the final page even when all remaining rows have been delivered.

Completeness is therefore verified by comparing the row count actually received
against the reported ``total``, not by the warning. The warning is captured
and carried per page for logging purposes but never treated as the authoritative
termination signal.
"""

from __future__ import annotations

import pytest

from energy_pipeline.errors import MalformedResponseError
from energy_pipeline.extract import FixtureSource
from energy_pipeline.pagination import iter_pages, parse_envelope


def test_pages_combine_into_every_row_exactly_once(monthly_fixture):
    source = FixtureSource(monthly_fixture)
    rows = [row for page in iter_pages(source.fetch_page, page_size=3) for row in page.rows]

    assert len(rows) == source.total == 10
    keys = [(r["period"], r["location"], r["sectorid"], r["fueltypeid"]) for r in rows]
    assert len(set(keys)) == len(keys), "pagination duplicated or dropped a logical record"


def test_offsets_advance_and_stop_when_exhausted(monthly_fixture):
    source = FixtureSource(monthly_fixture)
    pages = list(iter_pages(source.fetch_page, page_size=3))

    assert [p.offset for p in pages] == [0, 3, 6, 9]
    assert [len(p.rows) for p in pages] == [3, 3, 3, 1]
    assert source.requests == [(0, 3), (3, 3), (6, 3), (9, 3)]


def test_truncated_pages_carry_the_incomplete_return_warning(monthly_fixture):
    source = FixtureSource(monthly_fixture)
    pages = list(iter_pages(source.fetch_page, page_size=3))

    assert any("incomplete return" in w for w in pages[0].warnings)
    assert not any("incomplete return" in w for w in pages[-1].warnings)


def test_source_total_is_surfaced_on_every_page(monthly_fixture):
    source = FixtureSource(monthly_fixture)
    pages = list(iter_pages(source.fetch_page, page_size=3))
    assert {p.total for p in pages} == {10}


def test_a_source_that_never_signals_completion_fails_instead_of_looping():
    """A source that claims fewer rows than it just returned is inconsistent, and
    paging on from that would be guessing at a phantom result set."""

    def lying_source(offset: int, length: int) -> dict:
        return {
            "response": {
                "total": "0",  # contradicts the rows actually returned
                "data": [{"period": "2026-07"} for _ in range(length)],
            }
        }

    with pytest.raises(MalformedResponseError, match="total row"):
        list(iter_pages(lying_source, page_size=2, max_pages=25))


def test_a_missing_total_falls_back_to_the_short_page_signal():
    """Without this, a source that omits `total` would stop after page one of any
    multi-page extraction — silently loading a prefix and calling it success."""
    remaining = 5

    def source_without_total(offset: int, length: int) -> dict:
        nonlocal remaining
        rows = [{"period": "2026-07"} for _ in range(min(length, remaining))]
        remaining -= len(rows)
        return {"response": {"data": rows}}  # note: no "total" key at all

    pages = list(iter_pages(source_without_total, page_size=2, max_pages=10))

    assert [p.offset for p in pages] == [0, 2, 4]
    assert [len(p.rows) for p in pages] == [2, 2, 1], "a short final page ends it"
    assert pages[-1].total is None


@pytest.mark.parametrize(
    "payload, match",
    [
        ({}, "missing 'response'"),
        ({"response": {}}, "'response.data' is not a list"),
        ({"response": {"data": [], "total": "abc"}}, "'response.total' is not an integer"),
    ],
)
def test_broken_envelopes_are_rejected_with_a_specific_error(payload, match):
    with pytest.raises(MalformedResponseError, match=match):
        parse_envelope(payload, page_number=1, offset=0, requested_length=5000)


def test_empty_page_before_total_raises():
    """An empty page when the source says there are still rows is a
    contradiction: the data stopped arriving before total was reached."""

    def empty_source(offset: int, length: int) -> dict:
        return {
            "response": {
                "total": "5000",
                "data": [],
            }
        }

    with pytest.raises(MalformedResponseError, match="empty page"):
        list(iter_pages(empty_source, page_size=5000, max_pages=10))


@pytest.mark.parametrize(
    ("offset", "total"),
    [
        (0, 0),  # empty dataset
        (100, 100),  # all reported records consumed
    ],
)
def test_empty_page_at_offset_equal_to_total_is_accepted(offset: int, total: int):
    """An empty page where offset == total is a valid terminal response.

    Covers two scenarios:
    - total=0, offset=0: an empty dataset.
    - total=100, offset=100: all reported records consumed, next page is empty.

    The empty-page check must not raise when offset is not less than total.
    Tested at the ``is_last`` level since ``iter_pages`` cannot reach an empty
    page at ``offset == total > 0`` — a prior page delivering exactly ``total``
    rows would already satisfy ``is_last`` and stop iteration.
    """
    from energy_pipeline.pagination import Page

    page = Page(
        page_number=1,
        offset=offset,
        rows=[],
        total=total,
        requested_length=5000,
    )

    assert page.is_last is True


def test_empty_page_without_total_terminates_cleanly():
    """An empty page with no reported total is a valid end-of-stream signal."""

    def empty_source(offset: int, length: int) -> dict:
        return {"response": {"data": []}}  # no "total" key

    pages = list(iter_pages(empty_source, page_size=5000, max_pages=10))
    assert len(pages) == 1
    assert pages[0].total is None
    assert pages[0].rows == []


def test_valid_empty_dataset_with_zero_total():
    """A total of zero with an empty data list is a legitimate empty response."""

    def empty_source(offset: int, length: int) -> dict:
        return {"response": {"total": "0", "data": []}}

    pages = list(iter_pages(empty_source, page_size=5000, max_pages=10))
    assert len(pages) == 1
    assert pages[0].total == 0
    assert pages[0].rows == []


def test_final_page_with_incomplete_return_warning_is_accepted():
    """A final page (offset + rows >= total) carrying an 'incomplete return'
    warning is valid: EIA attaches the warning to the final page when the page
    size is smaller than the cap, not as an authoritative 'more follows' signal.
    The page is yielded and iteration stops once total is reached.
    """

    def source_with_warning_on_final_page(offset: int, length: int) -> dict:
        return {
            "response": {
                "total": "3",
                "data": [{"period": "2026-01"} for _ in range(3)],
            },
            "warnings": [
                {
                    "warning": "incomplete return",
                    "description": "truncated",
                }
            ],
        }

    pages = list(iter_pages(source_with_warning_on_final_page, page_size=3, max_pages=10))
    assert len(pages) == 1
    assert len(pages[0].rows) == 3
    assert any("incomplete return" in w for w in pages[0].warnings)


def test_incomplete_return_on_final_page_with_partial_last_page():
    """Four pages totaling 17,423 rows; the final (partial) page carries the
    'incomplete return' warning. This mirrors the live EIA API behaviour observed
    for 2026-07 and must complete successfully.

    The warning fires whenever the response has fewer rows than requested,
    including the final partial page — EIA does not suppress it once ``total``
    is reached.
    """
    total = 17423
    page_size = 5000

    def source(offset: int, length: int) -> dict:
        remaining = total - offset
        count = min(length, remaining)
        data = [{"period": "2026-07"} for _ in range(count)]
        payload: dict = {
            "response": {"total": str(total), "data": data},
        }
        # Warning fires when fewer rows returned than requested, including the
        # final page (2423 < 5000).
        if count < length:
            payload["warnings"] = [{"warning": "incomplete return", "description": "truncated"}]
        return payload

    pages = list(iter_pages(source, page_size=page_size, max_pages=10))
    assert len(pages) == 4
    assert [len(p.rows) for p in pages] == [5000, 5000, 5000, 2423]
    # The final (partial) page carries the warning
    assert any("incomplete return" in w for w in pages[-1].warnings)
    assert sum(len(p.rows) for p in pages) == total

    # Verify offsets advanced correctly
    assert [p.offset for p in pages] == [0, 5000, 10000, 15000]


def test_intermediate_incomplete_return_does_not_raise():
    """Intermediate pages carrying 'incomplete return' warnings are normal and
    must not interfere with pagination."""

    def two_page_source(offset: int, length: int) -> dict:
        if offset == 0:
            return {
                "response": {
                    "total": "4",
                    "data": [{"i": i} for i in range(2)],
                },
                "warnings": [{"warning": "incomplete return", "description": "truncated"}],
            }
        return {
            "response": {
                "total": "4",
                "data": [{"i": i} for i in range(2, 4)],
            },
        }  # final page — no warning

    pages = list(iter_pages(two_page_source, page_size=2, max_pages=10))
    assert len(pages) == 2
    assert any("incomplete return" in w for w in pages[0].warnings)
    assert not any("incomplete return" in w for w in pages[1].warnings)


def test_completeness_check_rejects_short_total():
    """If the source reports total=10 but the rows stop arriving early, iteration
    must fail rather than silently accepting a partial result. The empty-page
    guard catches the empty final page; the completeness check is a belt-and-
    suspenders safety net that catches any gap parse_envelope and is_last miss."""

    def short_source(offset: int, length: int) -> dict:
        if offset == 0:
            return {"response": {"total": "10", "data": [{"i": i} for i in range(5)]}}
        if offset == 5:
            # 3 rows — not enough to reach total (5+3=8 < 10), and 3 < 5 requested
            # so is_last would be True via the short-page signal... but total is
            # present, so is_last checks offset+rows >= total → 8 >= 10 → False.
            # Iteration continues.
            return {"response": {"total": "10", "data": [{"i": i} for i in range(5, 8)]}}
        # No more data — empty page with total=10 > offset=8 triggers the
        # empty-page guard.
        return {"response": {"total": "10", "data": []}}

    with pytest.raises(MalformedResponseError, match="empty page"):
        list(iter_pages(short_source, page_size=5, max_pages=10))


def test_changing_total_across_pages_is_rejected():
    """A source that reports different totals on successive pages is inconsistent
    and must fail — the caller cannot trust which total is correct.

    The first page reports total=10 with 5 rows (not last, 5 < 10), so iteration
    continues. The second page reports total=20, which conflicts → must raise."""

    def changing_source(offset: int, length: int) -> dict:
        if offset == 0:
            return {"response": {"total": "10", "data": [{"i": i} for i in range(5)]}}
        return {"response": {"total": "20", "data": [{"i": i} for i in range(5, 10)]}}

    with pytest.raises(MalformedResponseError, match="changed total"):
        list(iter_pages(changing_source, page_size=5, max_pages=10))
