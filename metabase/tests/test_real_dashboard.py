"""The separate real-data dashboard must not reuse fixture SQL or identifiers."""

import json
from pathlib import Path


def test_real_dashboard_scope_and_queries_are_explicit():
    root = Path(__file__).parents[2]
    dashboard = json.loads((root / "metabase/dashboards/texas_real_eia_dashboard.json").read_text())
    assert dashboard["name"] == "Texas Electricity Generation — July 2026 (Real EIA Data)"
    assert dashboard["data_source_kind"] == "manual_reporting_snapshot"
    assert len(dashboard["cards"]) == 5
    assert [c["display"] for c in dashboard["cards"]].count("table") == 3
    assert all(c["display"] not in ("pie", "line") for c in dashboard["cards"])
    for card in dashboard["cards"]:
        path = root / card["sql"]
        assert path.is_file() and path.is_relative_to(root / "metabase/real_data/cards")
        sql = path.read_text().lower()
        assert "marts." in sql and "{{" not in sql
        assert not any(word in sql.split() for word in ("insert", "update", "delete", "truncate"))
    scope = (root / "metabase/real_data/cards/05_scope.sql").read_text()
    assert "Manual copy" in scope and "National coverage" in scope


def test_reporting_schema_retains_signed_values_and_separates_rollups():
    root = Path(__file__).parents[2]
    sql = (root / "metabase/real_data/reporting.sql").read_text()
    assert "energy_real_reporting%" in sql
    assert "WHERE is_leaf" in sql and "fueltypeid <> 'DPV'" in sql
    assert "PRIMARY KEY (period, location, sectorid, fueltypeid)" in sql
    assert "COALESCE" not in sql.upper()
