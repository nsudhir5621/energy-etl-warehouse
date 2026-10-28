"""Extraction: window arithmetic, sources, and the extract-to-batch pipeline.

The same orchestration runs against the live API and against local fixtures. A
single code path is deliberate — a fixture-only path would let a bug live in the
production path until the first live run, which is exactly the bug a fixture is
supposed to prevent.

Nothing here writes anywhere. Raw persistence is Phase 2, and the batch carries
the objects and metadata it would write.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from .client import EiaClient
from .config import Settings
from .errors import MalformedResponseError
from .pagination import iter_pages
from .raw import RawBatch, build_batch
from .records import DedupeResult, GenerationRecord, dedupe, parse_records
from .validation import ValidationReport, validate

_PERIOD_RE = re.compile(r"^(\d{4})-(\d{2})$")
# Legacy/custom-source metadata is ambiguous; never upgrade it to live origin.
SOURCE_NAME = "eia_api_v2"
API_SOURCE_NAME = "eia_api_v2_live"
FIXTURE_SOURCE_NAME = "eia_api_v2_fixture"


# -- period arithmetic ---------------------------------------------------------


def parse_period(period: str) -> tuple[int, int]:
    match = _PERIOD_RE.match(period)
    if not match:
        raise ValueError(f"invalid period {period!r}, expected YYYY-MM")
    year, month = int(match.group(1)), int(match.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"invalid month in period {period!r}")
    return year, month


def shift_month(period: str, months: int) -> str:
    """Shift a YYYY-MM period by a whole number of months.

    Uses a total-month index so that stepping back from January crosses the year
    boundary correctly — the kind of off-by-one that silently truncates a
    lookback window by a month at every year end.
    """
    year, month = parse_period(period)
    index = year * 12 + (month - 1) + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


@dataclass(frozen=True, slots=True)
class PeriodWindow:
    start: str
    end: str

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"window start {self.start} is after end {self.end}")
        parse_period(self.start)
        parse_period(self.end)

    @classmethod
    def lookback(cls, latest: str, months: int) -> PeriodWindow:
        """The operational rolling window ending at the latest available period.

        ``months`` is the configurable revision lookback. The default is
        deliberately conservative and *not* a measured revision horizon — see
        docs/source-data.md. Explicit historical reconciliation is a different
        entry point with an operator-supplied range, not a wider default.
        """
        if months < 1:
            raise ValueError("lookback must be at least 1 month")
        return cls(start=shift_month(latest, -(months - 1)), end=latest)

    def as_params(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}


# -- sources -------------------------------------------------------------------


class ExtractionSource(Protocol):
    """What pagination and extraction need from a data source."""

    def fetch_page(self, offset: int, length: int) -> dict[str, Any]: ...

    def latest_period(self) -> str | None: ...


class ApiSource:
    """Reads a period window from the live EIA API."""

    def __init__(self, client: EiaClient, settings: Settings, window: PeriodWindow) -> None:
        self._client = client
        self._settings = settings
        self._window = window

    def _base_params(self) -> dict[str, Any]:
        return {
            "frequency": self._settings.frequency,
            "data[0]": self._settings.measure,
            **self._window.as_params(),
        }

    def fetch_page(self, offset: int, length: int) -> dict[str, Any]:
        return self._client.get(
            {
                **self._base_params(),
                "offset": offset,
                "length": length,
                # Indexed sort keys are what the API requires: it rejects
                # the flat "sort[column]" form with HTTP 400 ("Invalid
                # sort index 'column'"). The API also rejects a sort column
                # without its direction (HTTP 400, measured live
                # 2026-10-07: "No sort 'direction' specified for supplied
                # sort parameter '0'"). Both must be present.
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
            }
        )

    def latest_period(self) -> str | None:
        """Ask the source for its newest period, rather than assuming it.

        A pipeline that infers "now minus two months" from the calendar hardcodes
        a publication lag into every downstream decision, and silently misses
        every time EIA changes that lag.
        """
        payload = self._client.get(
            {
                "frequency": self._settings.frequency,
                "data[0]": self._settings.measure,
                "length": 1,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
            }
        )
        rows = payload.get("response", {}).get("data") or []
        return rows[0].get("period") if rows else None


class FixtureSource:
    """Serves a local fixture through the same interface as the API.

    Envelopes are synthesised to match the API exactly, including the
    ``incomplete return`` warning when a page is truncated, so the pagination
    and warning-handling code paths are genuinely exercised offline.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.rows: list[dict[str, Any]] = self._load(self.path)
        self.requests: list[tuple[int, int]] = []

    @staticmethod
    def _load(path: Path) -> list[dict[str, Any]]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload["rows"] if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise MalformedResponseError(f"fixture {path.name} does not contain a row list")
        return rows

    @property
    def total(self) -> int:
        return len(self.rows)

    def fetch_page(self, offset: int, length: int) -> dict[str, Any]:
        self.requests.append((offset, length))
        window = self.rows[offset : offset + length]
        payload: dict[str, Any] = {
            "response": {
                "total": str(self.total),
                "dateFormat": "YYYY-MM",
                "frequency": "monthly",
                "data": window,
            }
        }
        if offset + length < self.total:
            payload["warnings"] = [
                {
                    "warning": "incomplete return",
                    "description": (
                        "The API can only return 5000 rows in JSON format.  Please consider "
                        "constraining your request with facet, start, or end, or using offset "
                        "to paginate results."
                    ),
                }
            ]
        return payload

    def latest_period(self) -> str | None:
        periods = sorted({r["period"] for r in self.rows if "period" in r})
        return periods[-1] if periods else None


# -- extraction ----------------------------------------------------------------


@dataclass
class ExtractionResult:
    """Everything one extraction produced, ready for the next phase."""

    window: PeriodWindow
    batch: RawBatch
    records: list[GenerationRecord]
    dedupe_result: DedupeResult
    report: ValidationReport
    parse_errors: list[str] = field(default_factory=list)
    #: Rows that would be handed to MERGE: deduplicated, order-preserving.
    merge_rows: list[GenerationRecord] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "window": {"start": self.window.start, "end": self.window.end},
            "manifest": self.batch.manifest(),
            "rows_extracted": self.batch.record_count,
            "rows_parsed": len(self.records),
            "rows_after_deduplication": len(self.merge_rows),
            "identical_duplicates_collapsed": self.dedupe_result.identical_duplicates,
            "conflicting_keys": len(self.dedupe_result.conflicting_keys),
            "validation": self.report.to_dict(),
        }


def extract(
    source: ExtractionSource,
    settings: Settings,
    window: PeriodWindow,
    *,
    extracted_at: str | None = None,
) -> ExtractionResult:
    """Fetch a window, build the immutable batch, parse, deduplicate, validate.

    ``extracted_at`` is the local extraction start, injectable for offline tests.
    It is not an EIA revision timestamp; execution identity is a separate UUID4.
    Only recognized source objects receive explicit API/fixture origin labels.
    """
    if extracted_at is None:
        extracted_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    origin = SOURCE_NAME
    if isinstance(source, ApiSource):
        origin = API_SOURCE_NAME
    elif isinstance(source, FixtureSource):
        origin = FIXTURE_SOURCE_NAME

    pages: Sequence = list(iter_pages(source.fetch_page, page_size=settings.page_size))
    batch = build_batch(
        pages,
        source=origin,
        route=settings.route,
        frequency=settings.frequency,
        measure=settings.measure,
        window_start=window.start,
        window_end=window.end,
        extracted_at=extracted_at,
        prefix=settings.raw_prefix,
    )

    rows = [row for page in pages for row in page.rows]
    records, parse_errors = parse_records(rows)
    dedupe_result = dedupe(records)
    report = validate(
        dedupe_result.records,
        parse_errors=parse_errors,
        dedupe_result=dedupe_result,
        source_record_total=batch.source_record_total,
        window_start=window.start,
        window_end=window.end,
    )

    return ExtractionResult(
        window=window,
        batch=batch,
        records=records,
        dedupe_result=dedupe_result,
        report=report,
        parse_errors=parse_errors,
        merge_rows=dedupe_result.records,
    )


def extract_fixture(
    path: str | Path,
    settings: Settings | None = None,
    *,
    window: PeriodWindow | None = None,
    extracted_at: str = "2026-09-30T00:00:00Z",
) -> ExtractionResult:
    """Offline extraction, identical in every respect except where the rows came from."""
    source = FixtureSource(path)
    resolved = window or PeriodWindow.lookback(source.latest_period() or "1970-01", 24)
    return extract(source, settings or Settings(api_key=None), resolved, extracted_at=extracted_at)
