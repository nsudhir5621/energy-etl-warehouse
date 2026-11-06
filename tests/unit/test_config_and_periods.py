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


@pytest.fixture
def private_api_key_file(tmp_path, monkeypatch):
    from pathlib import Path

    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    directory = tmp_path / ".config" / "energy-etl"
    directory.mkdir(parents=True, mode=0o700)
    path = directory / "eia.env"
    path.write_text("EIA_API_KEY=file-test-key\n")
    path.chmod(0o600)
    return path


def test_private_key_is_loaded_only_for_live_configuration(private_api_key_file):
    assert Settings.from_env({}).api_key is None
    assert Settings.from_env({}, require_api_key=True).api_key == "file-test-key"


def test_environment_key_takes_precedence_without_opening_private_file(
    private_api_key_file, monkeypatch
):
    from pathlib import Path

    private_api_key_file.chmod(0o666)

    def forbidden_home(cls):
        pytest.fail("Environment credentials must bypass private-file inspection")

    monkeypatch.setattr(Path, "home", classmethod(forbidden_home))
    env = {"EIA_API_KEY": "environment-test-key"}
    assert Settings.from_env(env, require_api_key=True).api_key == "environment-test-key"
    assert env == {"EIA_API_KEY": "environment-test-key"}


def test_file_loading_does_not_change_environment(private_api_key_file, monkeypatch):
    import os

    monkeypatch.setenv("EIA_API_KEY", "")
    assert Settings.from_env(require_api_key=True).api_key == "file-test-key"
    assert os.environ["EIA_API_KEY"] == ""


def test_missing_private_file_does_not_invent_credentials(private_api_key_file):
    private_api_key_file.unlink()
    assert Settings.from_env({}, require_api_key=True).api_key is None


@pytest.mark.parametrize("contents", ["", "# no credential\n", "EIA_API_KEY=\n"])
def test_empty_private_configuration_has_no_credential(private_api_key_file, contents):
    private_api_key_file.write_text(contents)
    assert Settings.from_env({}, require_api_key=True).api_key is None


@pytest.mark.parametrize("quote", ["", "'", '"'])
def test_private_key_accepts_one_literal_assignment(private_api_key_file, quote):
    private_api_key_file.write_text(
        f"# private EIA credential\nEIA_API_KEY={quote}file-test-key{quote}\n"
    )
    assert Settings.from_env({}, require_api_key=True).api_key == "file-test-key"


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o660, 0o700])
def test_private_key_rejects_unsafe_permissions(private_api_key_file, mode):
    private_api_key_file.chmod(mode)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_read_only_owner_private_key_is_supported(private_api_key_file):
    private_api_key_file.chmod(0o400)
    assert Settings.from_env({}, require_api_key=True).api_key == "file-test-key"


def test_private_key_rejects_nonprivate_directory(private_api_key_file):
    private_api_key_file.parent.chmod(0o755)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rejects_symlink_file(private_api_key_file):
    target = private_api_key_file.with_suffix(".original")
    private_api_key_file.rename(target)
    private_api_key_file.symlink_to(target)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rejects_symlink_directory(private_api_key_file):
    directory = private_api_key_file.parent
    target = directory.with_name("original")
    directory.rename(target)
    directory.symlink_to(target, target_is_directory=True)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rejects_nonregular_file(private_api_key_file):
    import os

    private_api_key_file.unlink()
    os.mkfifo(private_api_key_file, 0o600)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rejects_wrong_owner(private_api_key_file, monkeypatch):
    import energy_pipeline.config as config

    actual_uid = config.os.getuid()
    monkeypatch.setattr(config.os, "getuid", lambda: actual_uid + 1)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


@pytest.mark.parametrize(
    "contents",
    [
        "OTHER_KEY=sensitive-test-value\n",
        "EIA_API_KEY=sensitive-test-value\nEIA_API_KEY=another-key\n",
        "EIA_API_KEY='sensitive-test-value\n",
        "EIA_API_KEY=sensitive test value\n",
    ],
)
def test_malformed_private_credentials_fail_without_disclosing_contents(
    private_api_key_file, contents
):
    private_api_key_file.write_text(contents)
    with pytest.raises(ConfigError, match="credential") as exc:
        Settings.from_env({}, require_api_key=True)
    assert "sensitive" not in str(exc.value)


def test_private_key_rejects_invalid_encoding_without_disclosing_contents(private_api_key_file):
    private_api_key_file.write_bytes(b"EIA_API_KEY=sensitive-test-value\xff")
    with pytest.raises(ConfigError, match="credential") as exc:
        Settings.from_env({}, require_api_key=True)
    assert "sensitive" not in str(exc.value)


def test_private_key_rejects_oversized_file(private_api_key_file):
    private_api_key_file.write_text("EIA_API_KEY=" + "x" * 4096)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rejects_hard_links(private_api_key_file):
    import os

    os.link(private_api_key_file, private_api_key_file.with_suffix(".linked"))
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_private_key_rechecks_open_descriptor_permissions(private_api_key_file, monkeypatch):
    import energy_pipeline.config as config

    original_open = config.os.open

    def change_permissions(path, *args, **kwargs):
        if path == private_api_key_file:
            private_api_key_file.chmod(0o644)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(config.os, "open", change_permissions)
    with pytest.raises(ConfigError, match="credential"):
        Settings.from_env({}, require_api_key=True)


def test_settings_repr_does_not_expose_api_key(private_api_key_file):
    settings = Settings.from_env({"EIA_API_KEY": "sensitive-test-value"})
    assert "sensitive-test-value" not in repr(settings)


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
