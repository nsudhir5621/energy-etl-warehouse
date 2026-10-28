"""Shared test fixtures.

The one rule for this suite: nothing may reach the network. Every test either
drives a local fixture file or an injected fake transport, so results are
deterministic and the suite runs identically offline and in CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "data" / "fixtures" / "eia"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def monthly_fixture(fixtures_dir: Path) -> Path:
    return fixtures_dir / "generation_monthly.json"


@pytest.fixture
def edge_cases_fixture(fixtures_dir: Path) -> Path:
    return fixtures_dir / "edge_cases.json"


@pytest.fixture
def duplicates_fixture(fixtures_dir: Path) -> Path:
    return fixtures_dir / "duplicates.json"


@pytest.fixture
def settings():
    """A Settings with no API key, and instant retries for offline tests."""
    from energy_pipeline.config import Settings

    return Settings(
        api_key=None,
        page_size=3,
        max_retries=2,
        min_request_interval_seconds=0.0,
    )
