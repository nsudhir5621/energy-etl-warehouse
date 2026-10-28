"""Pagination over the EIA API.

The API returns at most 5000 rows per page in JSON and signals that a response
is truncated with an ``incomplete return`` warning. That warning reflects the
5000-row per-response cap, not the completeness of the result set: EIA may
attach it to the final page even when all remaining rows have been delivered
(i.e. when ``offset + len(rows) >= total``). The warning is therefore captured
and carried per page, but it is never used as the sole termination signal —
completeness is verified by comparing the row count actually received against
the reported ``total``.

Pagination is driven by ``offset``/``length`` over a *bounded period window*,
never over the whole history. A window is the natural idempotent, restartable
unit: re-reading the same window produces the same batch shape, which is what
makes the pipeline safe to re-run.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from .errors import MalformedResponseError

log = logging.getLogger(__name__)

#: Signature of whatever produces a page. An API client and a fixture reader both
#: satisfy it, so pagination logic is identical for live and offline runs.
PageFetcher = Callable[[int, int], dict[str, Any]]

INCOMPLETE_RETURN_MARKER = "incomplete return"


@dataclass
class Page:
    """One page, with the parts of the envelope that are not row data."""

    page_number: int
    offset: int
    rows: list[dict[str, Any]]
    total: int | None
    requested_length: int
    warnings: list[str] = field(default_factory=list)

    @property
    def is_last(self) -> bool:
        """Whether iteration can stop after this page.

        ``total`` is the authoritative stop signal when the source supplies it.
        When it does not, a short page is the only signal available — and that
        distinction matters: treating a missing ``total`` as zero would silently
        stop after the first page of any multi-page extraction.

        An empty page is only terminal when there is no ``total`` indicating
        further rows; an empty page that should have data is a source error
        surfaced by the caller via ``parse_envelope``.
        """
        if not self.rows:
            # No total → end of stream (short-page signal was never available).
            # Total > offset → contradiction: stop signal says done, total says more.
            if self.total is not None and self.total > self.offset:
                raise MalformedResponseError(
                    f"page {self.page_number}: source reported {self.total} total row(s) "
                    f"but returned an empty page at offset {self.offset}"
                )
            return True
        if self.total is not None:
            return self.offset + len(self.rows) >= self.total
        return len(self.rows) < self.requested_length


def parse_envelope(
    payload: dict[str, Any], *, page_number: int, offset: int, requested_length: int
) -> Page:
    """Validate the API envelope and extract rows, total and warnings."""
    response = payload.get("response")
    if not isinstance(response, dict):
        raise MalformedResponseError(
            f"page {page_number}: missing 'response' object",
        )
    rows = response.get("data")
    if not isinstance(rows, list):
        raise MalformedResponseError(f"page {page_number}: 'response.data' is not a list")

    total_raw = response.get("total")
    try:
        total = int(total_raw) if total_raw is not None else None
    except (TypeError, ValueError) as exc:
        raise MalformedResponseError(
            f"page {page_number}: 'response.total' is not an integer: {total_raw!r}"
        ) from exc

    if total is not None and total < offset + len(rows):
        # The source has told us it holds fewer rows than it just returned. Either
        # the count or the data is wrong, and continuing would page a phantom.
        raise MalformedResponseError(
            f"page {page_number}: source reported {total} total row(s) but returned "
            f"{len(rows)} at offset {offset}"
        )

    warnings: list[str] = []
    for warning in payload.get("warnings", []) or []:
        if isinstance(warning, dict):
            # The machine-readable code and the human sentence are both kept. The
            # code ("incomplete return") is what callers match on, and matching
            # only on the prose would break the day EIA rewords the sentence.
            warnings.append(" ".join(str(v) for v in warning.values() if v))
        else:
            warnings.append(str(warning))

    return Page(
        page_number=page_number,
        offset=offset,
        rows=rows,
        total=total,
        requested_length=requested_length,
        warnings=warnings,
    )


def iter_pages(fetch: PageFetcher, *, page_size: int, max_pages: int = 10_000) -> Iterator[Page]:
    """Yield pages until the source is exhausted.

    ``max_pages`` is a runaway guard, not a business rule: it exists so a source
    that never signals completion fails loudly instead of looping forever.

    Completeness is verified after iteration ends: when the source reports a
    ``total``, the sum of rows across all yielded pages must equal it. A
    mismatch means the source returned fewer rows than it claimed — a silent
    truncation that cannot be detected from ``is_last`` alone.
    """
    offset = 0
    page_number = 0
    rows_received = 0
    reported_total: int | None = None
    while True:
        page_number += 1
        if page_number > max_pages:
            raise MalformedResponseError(
                f"pagination did not terminate within {max_pages} pages; refusing to continue"
            )
        page = parse_envelope(
            fetch(offset, page_size),
            page_number=page_number,
            offset=offset,
            requested_length=page_size,
        )
        # Track the reported total across pages; every page that includes one
        # must agree. A source that changes its mind mid-stream is not trustworthy.
        if page.total is not None:
            if reported_total is None:
                reported_total = page.total
            elif page.total != reported_total:
                raise MalformedResponseError(
                    f"page {page_number}: source changed total from "
                    f"{reported_total} to {page.total}"
                )
        incomplete = any(INCOMPLETE_RETURN_MARKER in w for w in page.warnings)
        if incomplete:
            log.info(
                "page %d carries 'incomplete return' warning (5000-row cap signal, "
                "not a completeness guarantee)",
                page_number,
            )
        yield page
        rows_received += len(page.rows)
        if page.is_last:
            break
        # A source that returns a full page but claims a smaller total would loop
        # forever; advancing by the row count we actually got is the only safe step.
        offset += len(page.rows)

    # Completeness check: when the source reports a total, the rows we collected
    # must account for every one of them. This is the authoritative completeness
    # gate — not the 'incomplete return' warning, which EIA attaches to the final
    # page even when all rows have been delivered.
    if reported_total is not None and rows_received != reported_total:
        raise MalformedResponseError(
            f"pagination complete but received {rows_received} rows "
            f"against total {reported_total}; refusing to accept a partial result"
        )
