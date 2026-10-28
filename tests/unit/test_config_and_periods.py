"""Configuration and period arithmetic.

Period arithmetic is where a lookback window quietly loses a month: stepping back
from January has to cross the year boundary, and an off-by-one there truncates
the window at every year end without ever raising.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from energy_pipeline.config import MAX_PAGE_SIZE, ConfigError, Settings
from energy_pipeline.extract import PeriodWindow, parse_period, shift_month

# -- configuration -------------------------------------------------------------


def test_the_api_key_is_read_from_the_environment_and_is_never_defaulted():
    assert Settings.from_env({}).api_key is None
    assert Settings.from_env({"EIA_API_KEY": "secret"}).api_key == "secret"


def test_an_empty_api_key_variable_is_treated_as_unset():
    assert Settings.from_env({"EIA_API_KEY": ""}).api_key is None


def test_defaults_match_the_measured_source_behaviour():
    settings = Settings.from_env({})

    assert settings.frequency == "monthly"
    assert settings.measure == "generation"
    assert settings.route == "electricity/electric-power-operational-data"
    assert settings.page_size == MAX_PAGE_SIZE == 5000
    assert settings.lookback_months == 24


def test_values_are_overridable_from_the_environment():
    settings = Settings.from_env(
        {
            "EIA_PAGE_SIZE": "1000",
            "EIA_LOOKBACK_MONTHS": "6",
            "EIA_TIMEOUT_SECONDS": "5.5",
            "EIA_BASE_URL": "https://example.test/v2/",
        }
    )

    assert settings.page_size == 1000
    assert settings.lookback_months == 6
    assert settings.timeout_seconds == 5.5
    assert settings.base_url == "https://example.test/v2", "trailing slash normalised"


@pytest.mark.parametrize(
    "env, match",
    [
        ({"EIA_PAGE_SIZE": "5001"}, "must be between 1 and 5000"),
        ({"EIA_PAGE_SIZE": "0"}, "must be between 1 and 5000"),
        ({"EIA_PAGE_SIZE": "many"}, "EIA_PAGE_SIZE is not a valid int"),
        ({"EIA_LOOKBACK_MONTHS": "0"}, "must be >= 1"),
        ({"EIA_TIMEOUT_SECONDS": "soon"}, "EIA_TIMEOUT_SECONDS is not a valid float"),
        ({"EIA_MAX_RETRIES": "-1"}, "must be >= 0"),
    ],
)
def test_nonsensical_configuration_fails_at_the_boundary(env, match):
    """A bad value must fail immediately with a clear message, not surface later
    as a confusing request failure."""
    with pytest.raises(ConfigError, match=match):
        Settings.from_env(env)


def test_settings_are_frozen_so_a_run_cannot_mutate_its_own_configuration():
    settings = Settings.from_env({})
    with pytest.raises(FrozenInstanceError):
        settings.lookback_months = 99  # type: ignore[misc]


# -- period arithmetic ---------------------------------------------------------


@pytest.mark.parametrize(
    "period, months, expected",
    [
        ("2026-07", 0, "2026-07"),
        ("2026-07", 1, "2026-08"),
        ("2026-07", 6, "2027-01"),
        ("2026-07", -1, "2026-06"),
        ("2026-07", -7, "2025-12"),
        ("2026-01", -1, "2025-12"),
        ("2026-01", -13, "2024-12"),
        ("2026-12", 1, "2027-01"),
        ("2026-07", -279, "2003-04"),
    ],
)
def test_month_arithmetic_crosses_year_boundaries(period, months, expected):
    assert shift_month(period, months) == expected


@pytest.mark.parametrize("period", ["2026-13", "2026-00", "202607", "2026-7", "", "n/a"])
def test_invalid_periods_are_rejected(period):
    with pytest.raises(ValueError):
        parse_period(period)


def test_lookback_window_is_inclusive_of_the_latest_period():
    window = PeriodWindow.lookback("2026-07", 24)
    assert window.end == "2026-07"
    assert window.start == "2024-08"
    assert window.start < window.end


def test_a_one_month_lookback_is_just_the_latest_period():
    assert PeriodWindow.lookback("2026-07", 1) == PeriodWindow("2026-07", "2026-07")


def test_lookback_crossing_a_year_boundary():
    window = PeriodWindow.lookback("2026-01", 3)
    assert (window.start, window.end) == ("2025-11", "2026-01")


def test_a_backwards_window_is_rejected():
    with pytest.raises(ValueError, match="is after end"):
        PeriodWindow("2026-07", "2026-01")


def test_a_zero_month_lookback_is_rejected():
    with pytest.raises(ValueError, match="at least 1 month"):
        PeriodWindow.lookback("2026-07", 0)


def test_window_serialises_to_api_parameters():
    assert PeriodWindow("2026-01", "2026-07").as_params() == {
        "start": "2026-01",
        "end": "2026-07",
    }
