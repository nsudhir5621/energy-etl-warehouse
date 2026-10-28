"""Tests for the Metabase dashboard definition and card queries.

These tests verify the dashboard structure (cards, layout, parameters) and
SQL query correctness (syntax, required columns, filter semantics) without
needing a running Metabase instance or PostgreSQL connection.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

DASHBOARD_DIR = Path(__file__).resolve().parent.parent
CARD_DIR = DASHBOARD_DIR / "cards"
DASHBOARD_FILE = DASHBOARD_DIR / "dashboards" / "energy_generation_dashboard.json"


@pytest.fixture(scope="module")
def dashboard():
    with open(DASHBOARD_FILE, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def cards():
    result = {}
    for path in sorted(CARD_DIR.glob("*.sql")):
        result[path.name] = path.read_text(encoding="utf-8")
    return result


class TestDashboardStructure:
    def test_dashboard_has_name(self, dashboard):
        assert dashboard["name"]
        assert len(dashboard["name"]) > 0

    def test_dashboard_has_description(self, dashboard):
        assert dashboard["description"]
        assert "rollup" in dashboard["description"].lower()

    def test_has_six_cards(self, dashboard):
        assert len(dashboard["ordered_cards"]) == 6

    def test_all_cards_have_required_fields(self, dashboard):
        required = {"card_name", "card_file", "display", "row", "col", "size_x", "size_y"}
        for card in dashboard["ordered_cards"]:
            missing = required - set(card.keys())
            assert not missing, f"Card missing fields: {missing}"

    def test_all_card_files_exist(self, dashboard):
        for card in dashboard["ordered_cards"]:
            path = CARD_DIR / card["card_file"]
            assert path.exists(), f"Card file not found: {card['card_file']}"

    def test_has_parameters(self, dashboard):
        assert len(dashboard["parameters"]) >= 3
        names = {p["name"] for p in dashboard["parameters"]}
        assert {"Period", "State / Geography", "Sector", "Energy Source"}.issubset(names)

    def test_display_types(self, dashboard):
        displays = {c["card_name"]: c["display"] for c in dashboard["ordered_cards"]}
        assert displays["Total Net Generation by Month"] == "line"
        assert displays["Top States by Generation (Latest 12 Months)"] == "bar"
        assert displays["Generation Mix by Fuel (Latest Month)"] == "pie"
        assert displays["Reconciliation: Leaf vs US+ALL Total"] == "table"


class TestCardLayout:
    def test_cards_have_unique_names(self, dashboard):
        names = [c["card_name"] for c in dashboard["ordered_cards"]]
        assert len(names) == len(set(names))

    def test_cards_have_unique_files(self, dashboard):
        files = [c["card_file"] for c in dashboard["ordered_cards"]]
        assert len(files) == len(set(files))

    def test_card_positions_within_grid(self, dashboard):
        for card in dashboard["ordered_cards"]:
            assert card["row"] >= 0
            assert card["col"] >= 0
            assert card["size_x"] > 0
            assert card["size_y"] > 0

    def test_no_overlapping_positions(self, dashboard):
        occupied = set()
        for card in dashboard["ordered_cards"]:
            for r in range(card["row"], card["row"] + card["size_y"]):
                for c in range(card["col"], card["col"] + card["size_x"]):
                    assert (r, c) not in occupied, f"Overlap at ({r}, {c})"
                    occupied.add((r, c))


class TestCardSQL:
    def test_all_cards_reference_marts(self, cards):
        for name, sql in cards.items():
            assert "mart_monthly_generation" in sql or "mart_reconciliation_total" in sql, (
                f"{name} does not reference a dbt mart"
            )

    def test_all_cards_use_schema_template(self, cards):
        for name, sql in cards.items():
            assert "{{ schema }}" in sql, f"{name} does not use schema template"

    def test_no_coalesce_violates_null_zero(self, cards):
        for name, sql in cards.items():
            sql_lower = sql.lower()
            assert "coalesce" not in sql_lower, (
                f"{name} uses COALESCE - violates null!=zero invariant"
            )

    def test_cards_filter_null_generation(self, cards):
        for name, sql in cards.items():
            if "sum(generation_mwh)" in sql.lower():
                assert "generation_mwh is not null" in sql.lower(), (
                    f"{name} sums generation without filtering NULLs"
                )

    def test_reconciliation_card_joins_both_marts(self, cards):
        sql = cards["05_reconciliation_check.sql"]
        assert "mart_monthly_generation" in sql
        assert "mart_reconciliation_total" in sql

    def test_reconciliation_card_has_tolerance(self, cards):
        sql = cards["05_reconciliation_check.sql"]
        assert "0.01" in sql

    def test_fuel_mix_card_has_pct_share(self, cards):
        sql = cards["03_generation_by_fuel.sql"]
        assert "pct_share" in sql
        assert "100." in sql

    def test_fuel_mix_card_handles_zero_total(self, cards):
        """Null-safe denominator: division by zero → NULL pct_share, not error."""
        sql = cards["03_generation_by_fuel.sql"]
        assert "NULLIF(" in sql, (
            "03_generation_by_fuel.sql must guard zero-total denominator with NULLIF"
        )

    def test_trend_card_groups_by_period(self, cards):
        sql = cards["06_generation_trend.sql"]
        assert "GROUP BY period" in sql

    def test_no_hardcoded_marts_schema(self, cards):
        for name, sql in cards.items():
            assert "marts." not in sql, f"{name} hardcodes schema - use {{ schema }} instead"

    def test_cards_have_comments(self, cards):
        for name, sql in cards.items():
            assert sql.strip().startswith("--"), f"{name} missing header comment"


class TestAnalyticalCoverage:
    def test_dashboard_answers_key_questions(self, dashboard):
        card_names = [c["card_name"] for c in dashboard["ordered_cards"]]
        assert any("Total" in n for n in card_names)
        assert any("State" in n for n in card_names)
        assert any("Fuel" in n for n in card_names)
        assert any("Sector" in n for n in card_names)
        assert any("Reconciliation" in n for n in card_names)
        assert any("Trend" in n for n in card_names)

    def test_dashboard_has_visualization_diversity(self, dashboard):
        displays = {c["display"] for c in dashboard["ordered_cards"]}
        assert len(displays) >= 3, f"Only {len(displays)} display types"

    def test_reconciliation_card_present_and_table(self, dashboard):
        recon = [c for c in dashboard["ordered_cards"] if "Reconciliation" in c["card_name"]]
        assert len(recon) == 1
        assert recon[0]["display"] == "table"
