"""HTTP client for the EIA API v2.

Two behaviours here are load-bearing, and both come from measurements taken in
Phase 0 rather than from convention:

1. **Retry policy is chosen by failure class**, not by status-code convenience.
   See :mod:`energy_pipeline.errors`.
2. **The rate limit is read from the response headers** (``X-Ratelimit-Limit``,
   ``X-Ratelimit-Remaining``, ``Retry-After``) rather than hardcoded. The API
   publishes its budget; guessing a constant would be wrong for every key but one.
   A 429 measured at ``Retry-After: 5793`` is a ~96 minute lockout — a naive
   1s/2s/4s backoff would hammer the endpoint for two hours and never succeed.

Every dependency that would make this untestable offline (sleep, clock, jitter,
HTTP session) is injected, so the retry behaviour is asserted deterministically
rather than waited on.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .config import Settings
from .errors import (
    ClientSourceError,
    ExtractionError,
    MalformedResponseError,
    RateLimitError,
    TransientSourceError,
    error_for_status,
)

log = logging.getLogger(__name__)

#: Advertise a real client. EIA rejects some clients outright.
USER_AGENT = "energy-data-elt/0.1 (+https://api.eia.gov/signup/)"

RATE_LIMIT_HEADER = "X-Ratelimit-Limit"
RATE_REMAINING_HEADER = "X-Ratelimit-Remaining"
RETRY_AFTER_HEADER = "Retry-After"


def _parse_retry_after(headers: Mapping[str, str]) -> float | None:
    """Retry-After is seconds in the EIA's implementation, but the HTTP spec also
    allows an HTTP-date. Accept both; ignore anything unparseable."""
    raw = headers.get(RETRY_AFTER_HEADER)
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        target = parsedate_to_datetime(raw)
        return max(0.0, target.timestamp() - time.time())
    except (TypeError, ValueError):
        return None


class EiaClient:
    """Minimal, retry-aware client for one EIA API route."""

    def __init__(
        self,
        settings: Settings,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = lambda: random.uniform(0.0, 0.25),
    ) -> None:
        self._settings = settings
        self._session = session or requests.Session()
        self._sleep = sleep
        self._clock = clock
        self._jitter = jitter
        self._last_request_at: float | None = None
        #: Observed budget, learned from the first response that carries headers.
        self.rate_limit: int | None = None
        self.rate_remaining: int | None = None

    # -- public ---------------------------------------------------------------

    @property
    def endpoint(self) -> str:
        return f"{self._settings.base_url}/{self._settings.route}/data/"

    def get(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """Fetch one page. Retries transient failures according to their class."""
        if not self._settings.api_key:
            # Failing here is deliberate. An unauthenticated request is a wasted
            # round trip that also lands in the rate limiter's error path.
            raise ClientSourceError(
                "EIA_API_KEY is not set; refusing to make an unauthenticated request"
            )

        request_params = {k: v for k, v in params.items() if v is not None}
        request_params["api_key"] = self._settings.api_key

        attempt = 0
        while True:
            self._respect_minimum_interval()
            try:
                return self._attempt(request_params)
            except ExtractionError as exc:
                if not exc.retryable or attempt >= self._settings.max_retries:
                    raise
                delay = self._delay_for(exc, attempt)
                attempt += 1
                log.warning(
                    "eia request failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt,
                    self._settings.max_retries,
                    delay,
                    exc,
                )
                self._sleep_for(delay)

    # -- internals ------------------------------------------------------------

    def _respect_minimum_interval(self) -> None:
        """Spread requests out. Serial, and paced off a configurable floor."""
        if self._last_request_at is None:
            return
        floor = self._settings.min_request_interval_seconds
        elapsed = self._clock() - self._last_request_at
        if elapsed < floor:
            self._sleep_for(floor - elapsed)

    def _attempt(self, params: Mapping[str, Any]) -> dict[str, Any]:
        try:
            response = self._session.get(
                self.endpoint,
                params=params,
                timeout=self._settings.timeout_seconds,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
        except requests.Timeout as exc:
            raise TransientSourceError(f"request timed out: {exc}") from exc
        except requests.RequestException as exc:
            raise TransientSourceError(f"transport error: {exc}") from exc
        finally:
            self._last_request_at = self._clock()

        self._absorb_rate_limit_headers(response.headers)

        if response.status_code >= 400:
            raise error_for_status(
                response.status_code, retry_after=_parse_retry_after(response.headers)
            )

        try:
            payload = response.json()
        except ValueError as exc:
            # A malformed body is treated as a hard failure. Retrying usually
            # reproduces it, and guessing at the "intended" content would be
            # fabrication dressed up as resilience.
            raise MalformedResponseError(f"source returned unparseable JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise MalformedResponseError(
                f"expected a JSON object at the top level, got {type(payload).__name__}"
            )
        return payload

    def _absorb_rate_limit_headers(self, headers: Mapping[str, str]) -> None:
        limit = headers.get(RATE_LIMIT_HEADER)
        remaining = headers.get(RATE_REMAINING_HEADER)
        if limit is not None:
            with suppress(ValueError):
                self.rate_limit = int(limit)
        if remaining is not None:
            with suppress(ValueError):
                self.rate_remaining = int(remaining)

    def _delay_for(self, exc: ExtractionError, attempt: int) -> float:
        """A 429 is a budget signal: wait exactly as long as the server said.
        Everything else uses capped exponential backoff with jitter."""
        if isinstance(exc, RateLimitError) and exc.retry_after is not None:
            return min(exc.retry_after, self._settings.max_retry_after_seconds)
        backoff = self._settings.backoff_base_seconds * (2**attempt)
        return min(backoff, self._settings.backoff_max_seconds) + self._jitter()

    def _sleep_for(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._sleep(seconds)
