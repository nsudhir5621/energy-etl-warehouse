"""Regression tests for explicit WSL fixture-loader configuration.

Imports must preserve caller settings. Missing/blank connection settings must
fail before psycopg.connect, and existing schema/business-key checks remain.
"""

from __future__ import annotations

import importlib.util
import inspect
import os
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest

_MODULE_NAME = "_run_in_wsl_test_module"


@pytest.fixture(autouse=True)
def _isolate_script_side_effects(monkeypatch: pytest.MonkeyPatch):
    """Restore environment and path mutations made directly by the imported script."""
    monkeypatch.setattr(sys, "path", sys.path.copy())
    with patch.dict(os.environ):
        yield


def _load_run_in_wsl() -> object:
    """Import scripts/run_in_wsl.py as a module with a controlled environment.

    The script is not part of the energy_pipeline package, so we load it by path.
    We stub out psycopg and project imports so the module can be imported without
    a running PostgreSQL instance.
    """
    script_path = Path(__file__).resolve().parent.parent.parent / "scripts" / "run_in_wsl.py"
    assert script_path.exists(), f"Script not found at {script_path}"

    # Restore both entries exactly, including when module execution raises.
    with pytest.MonkeyPatch.context() as import_patch:
        if "psycopg" not in sys.modules:
            import_patch.setitem(sys.modules, "psycopg", types.ModuleType("psycopg"))

        # Remove any cached copy so we get a fresh import with our env values.
        import_patch.delitem(sys.modules, _MODULE_NAME, raising=False)

        spec = importlib.util.spec_from_file_location(_MODULE_NAME, script_path)
        mod = importlib.util.module_from_spec(spec)
        import_patch.setitem(sys.modules, _MODULE_NAME, mod)
        spec.loader.exec_module(mod)
        return mod


class TestRunInWslEnv:
    """Verify the script honors POSTGRES_DSN from the environment."""

    def test_does_not_clobber_existing_postgres_dsn(self, monkeypatch: pytest.MonkeyPatch):
        """If POSTGRES_DSN is already set in the environment, the script must
        preserve that value (the bug was os.environ['POSTGRES_DSN'] = ...
        unconditionally overwriting it)."""
        custom_dsn = "postgresql://example.invalid/warehouse_unit_test"
        monkeypatch.setenv("POSTGRES_DSN", custom_dsn)

        _load_run_in_wsl()

        # After module import, POSTGRES_DSN should still be our custom value,
        # No fallback connection is supplied by the imported script.
        assert os.environ["POSTGRES_DSN"] == custom_dsn

    @pytest.mark.parametrize("dsn", [None, "", "   "])
    def test_missing_dsn_fails_before_connect(self, monkeypatch: pytest.MonkeyPatch, dsn):
        if dsn is None:
            monkeypatch.delenv("POSTGRES_DSN", raising=False)
        else:
            monkeypatch.setenv("POSTGRES_DSN", dsn)
        mod = _load_run_in_wsl()
        assert os.environ.get("POSTGRES_DSN") == dsn

        def unexpected_connection(*args, **kwargs):
            pytest.fail("Missing connection configuration must not attempt a database connection")

        monkeypatch.setattr(mod.psycopg, "connect", unexpected_connection, raising=False)
        with pytest.raises(ValueError, match="POSTGRES_DSN"):
            mod.main()

    def test_dsn_assignment_uses_env_get(self, monkeypatch: pytest.MonkeyPatch):
        """The dsn assignment in main() reads from os.environ.get, not a
        hardcoded literal."""
        monkeypatch.setenv("POSTGRES_DSN", "postgresql://example.invalid/warehouse_unit_test")

        mod = _load_run_in_wsl()

        source = inspect.getsource(mod.main)
        assert "os.environ.get" in source

    def test_verification_joins_fact_to_staging(self, monkeypatch: pytest.MonkeyPatch):
        """Verification must check that fact_generation has rows matching the
        current run's business keys via a JOIN, not by WHERE run_id alone.

        Equal-time identical observations may retain existing provenance,
        so counting only by run_id can omit successfully matched facts."""
        monkeypatch.setenv("POSTGRES_DSN", "postgresql:///warehouse_unit_test")

        mod = _load_run_in_wsl()
        source = inspect.getsource(mod.main)

        # stg_generation count must be filtered by run_id
        assert "FROM raw.stg_generation WHERE run_id" in source
        # fact_generation must be verified by JOIN to staging business keys
        assert "JOIN raw.stg_generation AS s" in source
        assert "ON f.period = s.period" in source
        # Must NOT count fact_generation by run_id alone
        assert "FROM core.fact_generation WHERE run_id" not in source

    def test_raw_root_defaulted_when_unset(self, monkeypatch: pytest.MonkeyPatch):
        """RAW_ROOT should be defaulted if not provided (existing behavior)."""
        monkeypatch.delenv("RAW_ROOT", raising=False)

        _load_run_in_wsl()

        assert "RAW_ROOT" in os.environ
        assert os.environ["RAW_ROOT"].endswith(str(Path("data") / "raw"))

    def test_schema_skip_logic_in_source(self, monkeypatch: pytest.MonkeyPatch):
        """The schema-loading section must check for existing schemas before
        executing DDL, so non-admin roles don't need CREATE on every schema."""
        monkeypatch.setenv("POSTGRES_DSN", "postgresql:///warehouse_unit_test")

        mod = _load_run_in_wsl()

        source = inspect.getsource(mod.main)
        # Must check if schema already exists before CREATE SCHEMA
        assert "information_schema.schemata" in source
        # Must conditionally skip DDL
        assert "skipping DDL" in source or "skip" in source.lower()
