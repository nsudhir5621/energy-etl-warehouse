"""Retry behaviour, chosen by failure class rather than by status-code convenience.

Phase 0 measured a 429 with ``Retry-After: 5793`` — a ~96 minute lockout. A
generic 1s/2s/4s backoff would hammer the endpoint for two hours and never
succeed, so the class of failure has to decide the policy.
"""

from __future__ import annotations

import pytest
import requests

from energy_pipeline.client import EiaClient
from energy_pipeline.config import Settings
from energy_pipeline.errors import (
    ClientSourceError,
    MalformedResponseError,
    RateLimitError,
    TransientSourceError,
)

BODY = {"response": {"total": "0", "data": []}}


class FakeResponse:
    def __init__(self, status_code=200, payload=BODY, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return self._payload


class FakeSession:
    """Replays a scripted sequence and records every call."""

    def __init__(self, *responses, exc=None):
        self._responses = list(responses)
        self._exc = exc
        self.calls: list[dict] = []

    def get(self, url, *, params, timeout, headers):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if self._exc is not None and len(self.calls) == 1:
            raise self._exc
        if self._responses:
            return self._responses.pop(0)
        return FakeResponse()


def make_client(session, *, max_retries=2, **overrides):
    slept: list[float] = []
    settings = Settings(
        **{
            "api_key": "test-key",
            "max_retries": max_retries,
            "backoff_base_seconds": 1.0,
            "backoff_max_seconds": 60.0,
            "min_request_interval_seconds": 0.0,
            **overrides,
        }
    )
    client = EiaClient(
        settings,
        session=session,
        sleep=slept.append,
        clock=lambda: 0.0,
        jitter=lambda: 0.0,
    )
    return client, slept


# -- transient failures retry --------------------------------------------------


def test_a_500_is_retried_and_then_succeeds():
    session = FakeSession(FakeResponse(500), FakeResponse(200))
    client, slept = make_client(session)

    assert client.get({"frequency": "monthly"}) == BODY
    assert len(session.calls) == 2
    assert slept == [1.0], "exponential backoff starts at backoff_base"


def test_backoff_grows_exponentially_and_is_capped():
    session = FakeSession(*[FakeResponse(503) for _ in range(4)], FakeResponse(200))
    client, slept = make_client(session, max_retries=4, backoff_max_seconds=5.0)

    client.get({"frequency": "monthly"})
    assert slept == [1.0, 2.0, 4.0, 5.0], "doubling, then capped"


def test_a_timeout_is_retried():
    session = FakeSession(FakeResponse(200), exc=requests.Timeout("timed out"))
    client, _ = make_client(session)

    assert client.get({"frequency": "monthly"}) == BODY
    assert len(session.calls) == 2


def test_retries_are_bounded_and_the_last_error_propagates():
    session = FakeSession(*[FakeResponse(500) for _ in range(5)])
    client, _ = make_client(session, max_retries=2)

    with pytest.raises(TransientSourceError):
        client.get({"frequency": "monthly"})
    assert len(session.calls) == 3, "one initial attempt plus two retries"


# -- a 429 is a budget signal, not a server signal -----------------------------


def test_a_429_sleeps_exactly_retry_after():
    """The single most important behaviour in this module. Retrying sooner burns
    the whole lockout window."""
    session = FakeSession(
        FakeResponse(429, headers={"Retry-After": "5793"}),
        FakeResponse(200),
    )
    client, slept = make_client(session)

    assert client.get({"frequency": "monthly"}) == BODY
    assert slept == [5793.0]


def test_a_429_without_retry_after_falls_back_to_exponential_backoff():
    session = FakeSession(FakeResponse(429), FakeResponse(200))
    client, slept = make_client(session)

    client.get({"frequency": "monthly"})
    assert slept == [1.0]


def test_retry_after_is_clamped_so_a_pathological_value_cannot_hang_the_run():
    session = FakeSession(
        FakeResponse(429, headers={"Retry-After": "999999"}),
        FakeResponse(200),
    )
    client, slept = make_client(session, max_retry_after_seconds=3600.0)

    client.get({"frequency": "monthly"})
    assert slept == [3600.0]


def test_the_default_cap_does_not_truncate_a_real_lockout():
    """A cap set below the observed ~96 minute lockout would spend a wasted
    request and then re-enter the lockout, so the default must clear it."""
    settings = Settings(api_key="k")
    assert settings.max_retry_after_seconds > 5793


# -- failures that must NOT be retried ----------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_errors_are_not_retried(status):
    """A bad key or a malformed filter fails identically forever. Retrying it is
    just a slower way to fail."""
    session = FakeSession(FakeResponse(status), FakeResponse(200))
    client, slept = make_client(session)

    with pytest.raises(ClientSourceError):
        client.get({"frequency": "monthly"})
    assert len(session.calls) == 1
    assert slept == []


def test_malformed_json_is_not_retried():
    """A body we cannot parse usually reproduces. Guessing at the intended
    content would be fabrication dressed up as resilience."""
    session = FakeSession(FakeResponse(200, payload=None), FakeResponse(200))
    client, _ = make_client(session)

    with pytest.raises(MalformedResponseError):
        client.get({"frequency": "monthly"})
    assert len(session.calls) == 1


def test_a_non_object_payload_is_rejected():
    session = FakeSession(FakeResponse(200, payload=[1, 2, 3]))
    client, _ = make_client(session)

    with pytest.raises(MalformedResponseError, match="JSON object at the top level"):
        client.get({"frequency": "monthly"})


# -- the budget is read from headers, not hardcoded -----------------------------


def test_rate_limit_headers_are_learned_from_the_response():
    session = FakeSession(
        FakeResponse(200, headers={"X-Ratelimit-Limit": "10", "X-Ratelimit-Remaining": "9"})
    )
    client, _ = make_client(session)

    client.get({"frequency": "monthly"})
    assert client.rate_limit == 10
    assert client.rate_remaining == 9


def test_a_missing_api_key_fails_before_any_request_is_sent():
    """An unauthenticated request is a wasted round trip that also lands in the
    rate limiter's error path."""
    session = FakeSession(FakeResponse(200))
    client = EiaClient(
        Settings(api_key=None), session=session, sleep=lambda _: None, clock=lambda: 0.0
    )

    with pytest.raises(ClientSourceError, match="EIA_API_KEY is not set"):
        client.get({"frequency": "monthly"})
    assert session.calls == []


def test_the_api_key_is_sent_but_never_logged_in_the_url(caplog):
    session = FakeSession(FakeResponse(200))
    client, _ = make_client(session)

    with caplog.at_level("DEBUG"):
        client.get({"frequency": "monthly", "length": 5})

    assert session.calls[0]["params"]["api_key"] == "test-key"
    assert session.calls[0]["params"]["length"] == 5
    assert "test-key" not in caplog.text


def test_the_endpoint_targets_the_configured_route():
    session = FakeSession(FakeResponse(200))
    client, _ = make_client(session)

    client.get({"frequency": "monthly"})
    assert session.calls[0]["url"] == (
        "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
    )


def test_the_configured_timeout_is_applied():
    session = FakeSession(FakeResponse(200))
    client, _ = make_client(session, timeout_seconds=12.5)

    client.get({"frequency": "monthly"})
    assert session.calls[0]["timeout"] == 12.5


def test_rate_limit_error_carries_the_retry_after_for_observability():
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "120"}))
    client, _ = make_client(session, max_retries=0)

    with pytest.raises(RateLimitError) as excinfo:
        client.get({"frequency": "monthly"})
    assert excinfo.value.retry_after == 120.0
    assert excinfo.value.retryable is True
