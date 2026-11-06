#!/usr/bin/env python3
"""Standalone smoke test for a single live EIA API extraction.

Verifies that the existing EiaClient + ApiSource + extraction pipeline can
retrieve, paginate, parse, and validate one complete historical month of real
data — without writing to PostgreSQL, the raw zone, or triggering dbt/Airflow.

Usage:
    python scripts/smoke_test_api.py --start 2026-07 --end 2026-07 \\
        --page-size 5000 --max-pages 10 --max-attempts 30

Uses EIA_API_KEY, then the protected ~/.config/energy-etl/eia.env fallback.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from typing import Any

import requests

from energy_pipeline.client import EiaClient
from energy_pipeline.config import MAX_PAGE_SIZE, Settings
from energy_pipeline.extract import ApiSource, PeriodWindow, extract
from energy_pipeline.validation import Severity

log = logging.getLogger("smoke_test")

#: Default max pages if not specified.
DEFAULT_MAX_PAGES = 100

#: Default max HTTP attempts (including any retries).
DEFAULT_MAX_ATTEMPTS = 30


class BudgetExhausted(RuntimeError):
    """Raised when the HTTP attempt budget would be exceeded."""


class AttemptBudget:
    """Transport wrapper that counts HTTP attempts against a budget."""

    def __init__(self, session: requests.Session, budget: int) -> None:
        self._session = session
        self._budget = budget
        self.attempts = 0

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        if self.attempts >= self._budget:
            raise BudgetExhausted(f"HTTP attempt budget exceeded ({self.attempts}/{self._budget})")
        self.attempts += 1
        return self._session.get(url, **kwargs)


def _redact(text: str) -> str:
    """Strip API key from any string that might be logged."""
    return re.sub(r"api_key=[^&\s]+", "api_key=REDACTED", text)


def _run_bounded_extraction(
    source: ApiSource,
    settings: Settings,
    window: PeriodWindow,
    max_pages: int,
) -> Any:
    """Run extraction with a bounded page count.

    Patches the module-level ``iter_pages`` reference that ``extract()`` uses so
    we can enforce ``max_pages`` without changing the production codepath.
    """
    import energy_pipeline.extract as extract_module

    original = extract_module.iter_pages

    def bounded(fetch: Any, *, page_size: int, **kwargs: Any) -> Any:
        return original(fetch, page_size=page_size, max_pages=max_pages)

    extract_module.iter_pages = bounded
    try:
        return extract(source, settings, window)
    finally:
        extract_module.iter_pages = original


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test one EIA API extraction without writing data."
    )
    parser.add_argument("--start", default="2026-07", help="Period start (YYYY-MM)")
    parser.add_argument("--end", default="2026-07", help="Period end (YYYY-MM)")
    parser.add_argument("--page-size", type=int, default=5000, help="Rows per page (max 5000)")
    parser.add_argument(
        "--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="Max pages before failing"
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help="Max HTTP attempts (including retries) before failing",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # --- 0. Validate CLI limits before any network activity ---
    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        log.error("--page-size must be between 1 and %d, got %d", MAX_PAGE_SIZE, args.page_size)
        return 2
    if args.max_pages < 1:
        log.error("--max-pages must be a positive integer, got %d", args.max_pages)
        return 2
    if args.max_attempts < 1:
        log.error("--max-attempts must be a positive integer, got %d", args.max_attempts)
        return 2

    # --- 1. Validate period range before any HTTP request ---
    try:
        window = PeriodWindow(start=args.start, end=args.end)
    except ValueError as exc:
        log.error("Invalid period range: %s", exc)
        return 2

    # --- 2. Build settings from env (reads EIA_API_KEY) ---
    env_settings = Settings.from_env(require_api_key=True)
    if not env_settings.api_key:
        log.error("EIA_API_KEY is missing from the environment and private credential file.")
        return 2

    settings = Settings(
        api_key=env_settings.api_key,
        base_url=env_settings.base_url,
        route=env_settings.route,
        frequency=env_settings.frequency,
        measure=env_settings.measure,
        page_size=args.page_size,
        timeout_seconds=env_settings.timeout_seconds,
        max_retries=0,
        backoff_base_seconds=0.0,
        backoff_max_seconds=0.0,
        max_retry_after_seconds=0.0,
        min_request_interval_seconds=0.0,
        lookback_months=env_settings.lookback_months,
        raw_root=env_settings.raw_root,
        raw_prefix=env_settings.raw_prefix,
    )

    # --- 3. Wire up attempt-budget transport ---
    session = requests.Session()
    budget = AttemptBudget(session=session, budget=args.max_attempts)
    client = EiaClient(settings, session=budget)
    source = ApiSource(client, settings, window)

    log.info(
        "Smoke test: period=%s-%s, page_size=%d, max_pages=%d, max_attempts=%d",
        args.start,
        args.end,
        args.page_size,
        args.max_pages,
        args.max_attempts,
    )

    # --- 4. Run extraction with bounded pagination ---
    try:
        result = _run_bounded_extraction(source, settings, window, args.max_pages)
    except BudgetExhausted:
        log.error("HTTP attempt budget exceeded before extraction completed.")
        return 3
    except Exception as exc:
        log.error("Extraction failed: %s", _redact(str(exc)))
        return 1

    # --- 5. Verify page count against max-pages ---
    page_count = len(result.batch.pages)
    if page_count > args.max_pages:
        log.error(
            "Dataset required %d pages but --max-pages is %d. "
            "Refusing to treat a truncated result as success.",
            page_count,
            args.max_pages,
        )
        return 4

    # --- 6. Summary ---
    batch = result.batch
    report = result.report

    log.info("=" * 60)
    log.info("Smoke test results")
    log.info("  Period:        %s to %s", window.start, window.end)
    log.info("  Pages:         %d", page_count)
    log.info("  HTTP attempts: %d", budget.attempts)
    log.info("  Source rows:   %d", batch.record_count)
    log.info("  Parsed:        %d", len(result.records))
    log.info("  After dedup:   %d", len(result.merge_rows))
    log.info("  Validation:    %s", "OK" if report.ok else "FAILED")
    log.info("  — Metrics —")
    log.info("  Record count:    %d", report.record_count)
    log.info("  Unique keys:     %d", report.unique_key_count)
    log.info("  Null generation: %d", report.null_generation_count)
    log.info("  Zero generation: %d", report.zero_generation_count)
    log.info("  Negative gen:    %d", report.negative_generation_count)
    log.info("  Rollup rows:     %d", report.rollup_row_count)
    log.info("  Leaf rows:       %d", report.leaf_row_count)
    log.info("  Unknown rows:    %d", report.unknown_row_count)
    if report.unknown_row_count:
        codes = ", ".join(
            f"{dim}={','.join(sorted(vals))}" for dim, vals in sorted(report.unknown_codes.items())
        )
        log.warning("  Unknown codes:   %s", codes)
    if not report.ok:
        for finding in report.hard_failures:
            log.error("  HARD: %s", finding.message)
    for finding in report.findings:
        if finding.severity is Severity.WARN:
            log.warning("  WARN: %s", finding.message)

    if not report.ok:
        log.error("Validation failed — smoke test unsuccessful.")
        return 5

    log.info("Smoke test PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
