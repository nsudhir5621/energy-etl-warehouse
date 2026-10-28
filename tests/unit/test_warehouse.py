"""Unit tests for the Phase 3 Warehouse connection seam.

Tests the pure helper functions (``next_period``, ``partition_name``) and the
Warehouse class's explicit configuration behavior. Real loader SQL is covered
by the separately guarded PostgreSQL integration harness; this unit suite does
not connect to a database.
"""

from __future__ import annotations

import pytest

from energy_pipeline.warehouse import Warehouse, next_period, partition_name


class TestNextPeriod:
    def test_january_to_february(self):
        assert next_period("2026-01") == "2026-02"

    def test_july_to_august(self):
        assert next_period("2026-07") == "2026-08"

    def test_december_wraps_to_january(self):
        assert next_period("2026-12") == "2027-01"

    def test_year_boundary_in_january(self):
        assert next_period("2001-01") == "2001-02"

    def test_far_future(self):
        assert next_period("2099-12") == "2100-01"


class TestPartitionName:
    def test_basic_name(self):
        assert partition_name("2026-07") == "fact_generation_2026_07"

    def test_january(self):
        assert partition_name("2001-01") == "fact_generation_2001_01"

    def test_pacific_timezone_period(self):
        assert partition_name("2026-12") == "fact_generation_2026_12"


class TestWarehouseConfig:
    def test_from_env_with_dsn(self):
        w = Warehouse.from_env({"POSTGRES_DSN": "postgresql:///warehouse_unit_test"})
        assert w._conninfo == "postgresql:///warehouse_unit_test"

    def test_from_env_missing_dsn_raises(self):
        env: dict[str, str] = {}
        with pytest.raises(ValueError, match="POSTGRES_DSN"):
            Warehouse.from_env(env)

    def test_from_env_empty_dsn_raises(self):
        with pytest.raises(ValueError, match="POSTGRES_DSN"):
            Warehouse.from_env({"POSTGRES_DSN": ""})

    def test_init_with_explicit_dsn(self):
        w = Warehouse("postgresql://example.invalid/warehouse_unit_test")
        assert w._conninfo == "postgresql://example.invalid/warehouse_unit_test"

    def test_init_missing_dsn_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        with pytest.raises(ValueError, match="POSTGRES_DSN"):
            Warehouse()

    def test_init_uses_explicit_environment(self, monkeypatch: pytest.MonkeyPatch):
        dsn = "postgresql:///warehouse_unit_test"
        monkeypatch.setenv("POSTGRES_DSN", dsn)
        assert Warehouse()._conninfo == dsn

    @pytest.mark.parametrize("dsn", ["", " ", "\t"])
    def test_init_empty_dsn_raises(self, dsn):
        with pytest.raises(ValueError, match="connection string"):
            Warehouse(dsn)

    def test_from_env_uses_system_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POSTGRES_DSN", "postgresql:///warehouse_unit_test")
        w = Warehouse.from_env()
        assert w._conninfo == "postgresql:///warehouse_unit_test"
