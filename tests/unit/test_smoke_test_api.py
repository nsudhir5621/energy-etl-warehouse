"""Offline tests for the standalone EIA API smoke test script.

Tests verify CLI limit validation, bounded-extraction logic, page/attempt
budgets, rate-limit behaviour, exception handling, and redaction — all
without making live API requests.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from energy_pipeline.config import Settings
from energy_pipeline.errors import RateLimitError
from energy_pipeline.extract import PeriodWindow
from energy_pipeline.validation import Severity

# Make scripts/ importable
_project_root = str(Path(__file__).resolve().parents[2])
_scripts_dir = str(Path(_project_root) / "scripts")
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)


def _leaf_row(i: int) -> dict:
    """A fully-formed row that passes row_contract validation.

    fueltypeid 'NUC' is a verified leaf energy source, location 'TX' is a leaf
    state, sector '1' is known.
    """
    return {
        "period": "2026-07",
        "location": "TX",
        "stateDescription": "Texas",
        "sectorid": "1",
        "sectorDescription": "All Sectors",
        "fueltypeid": "NUC",
        "fuelTypeDescription": "Nuclear",
        "generation": "100.0",
        "generation-units": "thousand megawatthours",
    }


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: dict | None = None,
        headers: dict | None = None,
    ):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("Expecting value")
        return self._payload


class FakeSession:
    """Replays a scripted sequence and records every call."""

    def __init__(self, *responses: FakeResponse, exc: Exception | None = None):
        self._responses = list(responses)
        self._exc = exc
        self.calls: list[dict] = []

    def get(self, url: str, *, params: dict, timeout: float, headers: dict) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if self._exc is not None and len(self.calls) == 1:
            raise self._exc
        if self._responses:
            return self._responses.pop(0)
        return FakeResponse()


def _make_page(
    rows: list[dict], total: int, offset: int, page_size: int, warnings: list | None = None
) -> dict:
    """Build an EIA-style envelope for one page."""
    envelope: dict = {
        "response": {
            "total": str(total),
            "dateFormat": "YYYY-MM",
            "frequency": "monthly",
            "data": rows,
        }
    }
    if warnings:
        envelope["warnings"] = warnings
    if offset + page_size < total:
        envelope.setdefault("warnings", []).append(
            {"warning": "incomplete return", "description": "truncated"}
        )
    return envelope


def _make_client(session: FakeSession, *, max_retries: int = 0, **overrides) -> tuple:
    """Build an EiaClient with a fake session."""
    from energy_pipeline.client import EiaClient

    settings = Settings(
        api_key="test-key",
        max_retries=max_retries,
        backoff_base_seconds=0.0,
        backoff_max_seconds=0.0,
        min_request_interval_seconds=0.0,
        **overrides,
    )
    client = EiaClient(
        settings,
        session=session,
        sleep=lambda _: None,
        clock=lambda: 0.0,
        jitter=lambda: 0.0,
    )
    return client, settings


def _load_smoke_module():
    """Import the smoke test module fresh."""
    import smoke_test_api as mod

    importlib.reload(mod)
    return mod


class TestCliLimitValidation:
    """CLI limits are validated before any network activity."""

    def _run_main_with_argv(self, *argv: str) -> int:
        """Run main() with given argv and a mocked API key. Returns exit code."""
        mod = _load_smoke_module()
        env = {"EIA_API_KEY": "test-key"}

        with (
            patch.dict("os.environ", env, clear=False),
            patch("sys.argv", ["smoke_test_api.py", *argv]),
        ):
            return mod.main()

    def test_page_size_above_max_rejected(self):
        """page_size > 5000 is rejected with exit code 2."""
        assert (
            self._run_main_with_argv(
                "--page-size", "5001", "--start", "2026-07", "--end", "2026-07"
            )
            == 2
        )

    def test_page_size_zero_rejected(self):
        """page_size = 0 is rejected with exit code 2."""
        assert (
            self._run_main_with_argv("--page-size", "0", "--start", "2026-07", "--end", "2026-07")
            == 2
        )

    def test_page_size_negative_rejected(self):
        """Negative page_size is rejected with exit code 2."""
        assert (
            self._run_main_with_argv("--page-size", "-1", "--start", "2026-07", "--end", "2026-07")
            == 2
        )

    def test_max_pages_zero_rejected(self):
        """max_pages = 0 is rejected with exit code 2."""
        assert (
            self._run_main_with_argv("--max-pages", "0", "--start", "2026-07", "--end", "2026-07")
            == 2
        )

    def test_max_attempts_zero_rejected(self):
        """max_attempts = 0 is rejected with exit code 2."""
        assert (
            self._run_main_with_argv(
                "--max-attempts", "0", "--start", "2026-07", "--end", "2026-07"
            )
            == 2
        )

    def test_max_attempts_negative_rejected(self):
        """Negative max_attempts is rejected with exit code 2."""
        assert (
            self._run_main_with_argv(
                "--max-attempts", "-1", "--start", "2026-07", "--end", "2026-07"
            )
            == 2
        )

    def test_valid_limits_pass_through_to_period_validation(self):
        """Valid limits pass stage 0; invalid period then fails at stage 1 (still exit 2)."""
        assert (
            self._run_main_with_argv(
                "--page-size",
                "100",
                "--max-pages",
                "5",
                "--max-attempts",
                "10",
                "--start",
                "2026-13",
                "--end",
                "2026-07",
            )
            == 2
        )


class TestBoundedExtraction:
    def test_successful_one_page_extraction(self):
        """A single-page response extracts and validates cleanly."""
        rows = [_leaf_row(i) for i in range(3)]
        envelope = _make_page(rows, total=3, offset=0, page_size=5000)

        session = FakeSession(FakeResponse(200, payload=envelope))
        client, settings = _make_client(session, page_size=3)
        window = PeriodWindow(start="2026-07", end="2026-07")
        source = _build_source(client, settings, window)
        mod = _load_smoke_module()

        result = mod._run_bounded_extraction(source, settings, window, max_pages=100)

        assert result.batch.record_count == 3
        assert result.report.ok
        assert len(session.calls) == 1

    def test_successful_multi_page_extraction(self):
        """Two pages are fetched and combined correctly."""
        rows1 = [_leaf_row(i) for i in range(2)]
        rows2 = [_leaf_row(i) for i in range(2, 4)]

        envelope1 = _make_page(rows1, total=4, offset=0, page_size=2)
        envelope2 = _make_page(rows2, total=4, offset=2, page_size=2)

        session = FakeSession(
            FakeResponse(200, payload=envelope1),
            FakeResponse(200, payload=envelope2),
        )
        client, settings = _make_client(session, page_size=2)
        window = PeriodWindow(start="2026-07", end="2026-07")
        source = _build_source(client, settings, window)
        mod = _load_smoke_module()

        result = mod._run_bounded_extraction(source, settings, window, max_pages=100)

        assert result.batch.record_count == 4
        assert len(session.calls) == 2
        assert result.report.ok

    def test_max_pages_limit_fails_without_partial_result(self):
        """If the dataset needs more pages than allowed, extraction fails."""
        # 3 pages of 5 rows each; total claims 20 so a 4th page is required.
        session = FakeSession(
            FakeResponse(
                200,
                payload=_make_page(
                    [_leaf_row(i) for i in range(5)], total=20, offset=0, page_size=5
                ),
            ),
            FakeResponse(
                200,
                payload=_make_page(
                    [_leaf_row(i) for i in range(5, 10)], total=20, offset=5, page_size=5
                ),
            ),
            FakeResponse(
                200,
                payload=_make_page(
                    [_leaf_row(i) for i in range(10, 15)], total=20, offset=10, page_size=5
                ),
            ),
        )
        client, settings = _make_client(session, page_size=5)
        window = PeriodWindow(start="2026-07", end="2026-07")
        source = _build_source(client, settings, window)
        mod = _load_smoke_module()

        with pytest.raises(Exception, match="did not terminate"):
            mod._run_bounded_extraction(source, settings, window, max_pages=3)

    def test_rate_limit_with_zero_retries_fails_promptly(self):
        """A 429 with max_retries=0 raises RateLimitError without sleeping."""
        session = FakeSession(FakeResponse(429, headers={"Retry-After": "5793"}, payload=None))
        client, settings = _make_client(session, max_retries=0)
        window = PeriodWindow(start="2026-07", end="2026-07")
        source = _build_source(client, settings, window)
        mod = _load_smoke_module()

        with pytest.raises(RateLimitError):
            mod._run_bounded_extraction(source, settings, window, max_pages=100)

        # Only 1 HTTP attempt — no retries
        assert len(session.calls) == 1

    def test_invalid_periods_fail_before_http(self):
        """Invalid period strings are rejected before any HTTP request."""
        session = FakeSession(FakeResponse(200, payload={"response": {"total": "0", "data": []}}))
        _client, _settings = _make_client(session)

        # Invalid month (08 < 13 lexicographically, so the comparison passes first)
        with pytest.raises(ValueError, match="invalid month"):
            PeriodWindow(start="2026-08", end="2026-13")

        # Reversed range (both periods valid, but start after end)
        with pytest.raises(ValueError, match="after"):
            PeriodWindow(start="2026-08", end="2026-07")

        assert len(session.calls) == 0

    def test_api_key_not_in_logs_or_output(self, caplog):
        """The API key must never appear in log output."""
        rows = [_leaf_row(0)]
        envelope = _make_page(rows, total=1, offset=0, page_size=5000)

        session = FakeSession(FakeResponse(200, payload=envelope))
        client, settings = _make_client(session, page_size=1)
        window = PeriodWindow(start="2026-07", end="2026-07")
        source = _build_source(client, settings, window)
        mod = _load_smoke_module()

        with caplog.at_level("DEBUG"):
            mod._run_bounded_extraction(source, settings, window, max_pages=100)

        assert "test-key" not in caplog.text


class TestAttemptBudget:
    """Tests for the AttemptBudget transport wrapper."""

    def test_budget_allows_up_to_limit(self):
        """Exactly N calls succeed when budget=N; the (N+1)th raises."""
        mod = _load_smoke_module()
        session = FakeSession()
        budget = mod.AttemptBudget(session=session, budget=2)

        budget.get("http://example.com", params={}, timeout=30, headers={})
        budget.get("http://example.com", params={}, timeout=30, headers={})
        assert budget.attempts == 2

        with pytest.raises(mod.BudgetExhausted, match="budget exceeded"):
            budget.get("http://example.com", params={}, timeout=30, headers={})

    def test_budget_zero_rejects_immediately(self):
        """A budget of 0 means no HTTP requests are forwarded."""
        mod = _load_smoke_module()
        session = FakeSession()
        budget = mod.AttemptBudget(session=session, budget=0)

        with pytest.raises(mod.BudgetExhausted, match="budget exceeded"):
            budget.get("http://example.com", params={}, timeout=30, headers={})

        assert budget.attempts == 0
        assert len(session.calls) == 0  # nothing forwarded


class TestExceptionHandling:
    """Exception handling in main() produces controlled exits."""

    def _setup_main_mocks(self, mod):
        """Patch env and argv for main(), patch out network-dependent parts."""
        return [
            patch.dict("os.environ", {"EIA_API_KEY": "test-key"}, clear=False),
            patch("sys.argv", ["smoke_test_api.py", "--start", "2026-07", "--end", "2026-07"]),
        ]

    def test_budget_exhausted_returns_exit_code_3(self):
        """BudgetExhausted during extraction returns exit code 3."""
        mod = _load_smoke_module()

        patches = self._setup_main_mocks(mod)
        for p in patches:
            p.start()
        try:
            with patch.object(
                mod, "_run_bounded_extraction", side_effect=mod.BudgetExhausted("budget exceeded")
            ):
                rc = mod.main()
            assert rc == 3
        finally:
            for p in patches:
                p.stop()

    def test_unexpected_exception_returns_exit_code_1(self):
        """Unexpected runtime exceptions produce exit code 1, not a traceback."""
        mod = _load_smoke_module()

        patches = self._setup_main_mocks(mod)
        for p in patches:
            p.start()
        try:
            with patch.object(
                mod, "_run_bounded_extraction", side_effect=RuntimeError("unexpected boom")
            ):
                rc = mod.main()
            assert rc == 1
        finally:
            for p in patches:
                p.stop()

    def test_api_key_redacted_in_unexpected_failure(self, caplog):
        """API key is redacted even in unexpected exception messages."""
        mod = _load_smoke_module()

        patches = self._setup_main_mocks(mod)
        for p in patches:
            p.start()
        try:
            with (
                patch.object(
                    mod,
                    "_run_bounded_extraction",
                    side_effect=RuntimeError("failed with api_key=secret123 in URL"),
                ),
                caplog.at_level("DEBUG"),
            ):
                rc = mod.main()
            assert rc == 1
            assert "secret123" not in caplog.text
            assert "REDACTED" in caplog.text
        finally:
            for p in patches:
                p.stop()


class TestValidationReporting:
    """Tests for the validation summary in main()'s reporting section."""

    def _run_main_with_result(self, mod, mock_result, env=None):
        """Patch _run_bounded_extraction to return mock_result, run main()."""
        env = env or {"EIA_API_KEY": "test-key"}
        patches = [
            patch.dict("os.environ", env, clear=False),
            patch(
                "sys.argv",
                ["smoke_test_api.py", "--start", "2026-07", "--end", "2026-07"],
            ),
            patch.object(mod, "_run_bounded_extraction", return_value=mock_result),
        ]
        for p in patches:
            p.start()
        try:
            return mod.main()
        finally:
            for p in patches:
                p.stop()

    def _make_mock_result(self, ok, hard_findings=None, warn_findings=None):
        """Build a mock ExtractionResult with a report and batch."""
        report = MagicMock()
        report.ok = ok
        report.hard_failures = hard_findings or []
        report.record_count = 17423
        report.unique_key_count = 17423
        report.null_generation_count = 100
        report.zero_generation_count = 500
        report.negative_generation_count = 50
        report.rollup_row_count = 2000
        report.leaf_row_count = 15000
        report.unknown_row_count = 0
        report.unknown_codes = {}
        # findings = hard + warn, as the real ValidationReport does
        findings = list(report.hard_failures)
        if warn_findings:
            findings.extend(warn_findings)
        report.findings = findings
        batch = MagicMock()
        batch.pages = []
        batch.record_count = 17423
        result = MagicMock()
        result.batch = batch
        result.report = report
        result.records = list(range(17423))
        result.merge_rows = list(range(17423))
        return result

    def test_successful_report_without_warnings(self, caplog):
        """A clean report (ok=True, no warnings) returns exit code 0."""
        mod = _load_smoke_module()
        mock_result = self._make_mock_result(ok=True)
        with caplog.at_level("DEBUG"):
            rc = self._run_main_with_result(mod, mock_result)
        assert rc == 0
        assert "PASSED" in caplog.text
        assert "Validation:    OK" in caplog.text

    def test_successful_report_with_warnings(self, caplog):
        """A report with warnings but no hard failures returns exit code 0."""
        mod = _load_smoke_module()
        warn_finding = MagicMock()
        warn_finding.severity = Severity.WARN
        warn_finding.message = "unclassified energy_source code(s)"
        mock_result = self._make_mock_result(ok=True, warn_findings=[warn_finding])
        with caplog.at_level("DEBUG"):
            rc = self._run_main_with_result(mod, mock_result)
        assert rc == 0
        assert "PASSED" in caplog.text
        assert "WARN:" in caplog.text
        assert "Validation:    OK" in caplog.text

    def test_hard_failure_returns_exit_code_5(self, caplog):
        """A report with hard findings returns exit code 5."""
        mod = _load_smoke_module()
        hard_finding = MagicMock()
        hard_finding.severity = Severity.HARD
        hard_finding.message = "row_contract: row missing required field(s)"
        mock_result = self._make_mock_result(ok=False, hard_findings=[hard_finding])
        mock_result.report.findings = [hard_finding]
        with caplog.at_level("DEBUG"):
            rc = self._run_main_with_result(mod, mock_result)
        assert rc == 5
        assert "FAILED" in caplog.text
        assert "HARD:" in caplog.text
        assert "unsuccessful" in caplog.text

    def test_validation_metrics_logged(self, caplog):
        """Compact validation metrics are logged on successful extraction."""
        mod = _load_smoke_module()
        mock_result = self._make_mock_result(ok=True)
        with caplog.at_level("INFO"):
            rc = self._run_main_with_result(mod, mock_result)
        assert rc == 0
        assert "Record count:" in caplog.text
        assert "Unique keys:" in caplog.text
        assert "Null generation:" in caplog.text
        assert "Zero generation:" in caplog.text
        assert "Negative gen:" in caplog.text
        assert "Rollup rows:" in caplog.text
        assert "Leaf rows:" in caplog.text
        assert "Unknown rows:" in caplog.text


def _build_source(client, settings, window):
    from energy_pipeline.extract import ApiSource

    return ApiSource(client, settings, window)
