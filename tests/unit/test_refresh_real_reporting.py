"""Fail-closed snapshot contracts; no database, Docker or credentials needed."""

import importlib.util
from copy import deepcopy
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def reporting():
    path = Path(__file__).parents[2] / "scripts/refresh_real_reporting.py"
    spec = importlib.util.spec_from_file_location("real_reporting", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def observations(reporting):
    def row(code, value, leaf):
        result = {k: None for k in reporting.COLUMNS}
        result.update(
            period="2026-07",
            location="TX",
            sectorid="1",
            fueltypeid=code,
            generation_thousand_mwh=value,
            generation_mwh=None if value is None else value * 1000,
            is_rollup_geography=False,
            is_rollup_sector=False,
            is_rollup_energy_source=not leaf,
            is_leaf=leaf,
            run_id="00000000-0000-4000-8000-000000000001",
            content_hash="a" * 64,
            year=2026,
            month=7,
            date_month="2026-07-01",
        )
        return result

    stage = [
        row("ALL", 1.9, False),
        row("NG", 2.0, True),
        row("OTH", -0.1, True),
        row("RFO", 0.0, True),
    ]
    monthly = [{k: r[k] for k in reporting.MONTHLY_COLUMNS} for r in stage[1:]]
    return stage, monthly, {"period": "2026-07", "location": "TX", "sectorid": "1"}


def test_positive_zero_negative_and_rollup_are_not_double_counted(reporting, observations):
    assert reporting.validate_snapshot(*observations) == 1900


@pytest.mark.parametrize(
    "change",
    [
        {"fueltypeid": "ZZZ"},
        {"sectorid": "95"},
        {"location": "US"},
        {"generation_mwh": float("nan")},
        {"generation_mwh": float("inf")},
        {"generation_mwh": 2001},
        {"is_leaf": False},
        {"is_rollup_energy_source": True},
    ],
)
def test_invalid_snapshot_fails_before_target_write(reporting, observations, change):
    stage, monthly, scope = deepcopy(observations)
    stage[1].update(change)
    with pytest.raises(ValueError):
        reporting.validate_snapshot(stage, monthly, scope)


def test_null_is_never_zero_imputed(reporting, observations):
    stage, monthly, scope = deepcopy(observations)
    stage[1]["generation_thousand_mwh"] = stage[1]["generation_mwh"] = None
    monthly[0]["generation_thousand_mwh"] = monthly[0]["generation_mwh"] = None
    with pytest.raises(ValueError, match="NULL leaf"):
        reporting.validate_snapshot(stage, monthly, scope)


def test_stale_dbt_provenance_is_rejected(reporting, observations):
    stage, monthly, scope = deepcopy(observations)
    monthly[0]["run_id"] = "old-run"
    with pytest.raises(ValueError, match="stale"):
        reporting.validate_snapshot(stage, monthly, scope)


def test_duplicate_source_key_is_rejected(reporting, observations):
    stage, monthly, scope = observations
    with pytest.raises(ValueError, match="Duplicate"):
        reporting.validate_snapshot([*stage, stage[1]], monthly, scope)


def test_refresh_is_atomic_identity_guarded_and_data_not_sql_interpolated(reporting):
    target = {
        "database": "energy_real_reporting_test",
        "oid": 42,
        "owner": "energy_real_owner_test",
        "system_identifier": "7704822437372517837",
    }
    payload = {
        "source_records": [{"name": "'); DROP DATABASE energy; --"}],
        "snapshot_metadata": [],
        "expected_monthly": [],
    }
    statement = reporting.refresh_sql(target, payload)
    assert statement.startswith("BEGIN;") and statement.rstrip().endswith("COMMIT;")
    assert "pg_control_system()" in statement and "<>42" in statement
    assert "DROP DATABASE energy" not in statement
    assert "TRUNCATE" not in statement
    assert "DELETE FROM marts.source_records" in statement


@pytest.mark.parametrize("database", ["energy", "energy_reporting", "energy_b42_pilot", "postgres"])
def test_protected_database_cannot_be_a_refresh_target(reporting, database):
    target = {
        "database": database,
        "owner": "energy_real_owner_test",
        "oid": 42,
        "system_identifier": "7704822437372517837",
    }
    with pytest.raises(ValueError, match="dedicated real-reporting"):
        reporting.refresh_sql(target, {"source_records": [], "expected_monthly": []})


@pytest.mark.parametrize(
    "mutation",
    [{"oid": 0}, {"oid": True}, {"owner": "postgres"}, {"system_identifier": "unverified"}],
)
def test_target_identity_is_required(reporting, mutation):
    target = {
        "database": "energy_real_reporting_test",
        "owner": "energy_real_owner_test",
        "oid": 42,
        "system_identifier": "7704822437372517837",
    } | mutation
    with pytest.raises(ValueError):
        reporting.validate_target(target)


@pytest.fixture
def raw_example(tmp_path):
    import hashlib
    import json

    run = "00000000-0000-4000-8000-000000000001"
    key = f"raw/eia/electric_power_operational_data/ingestion_date=2026-11-06/run_id={run}"
    directory = tmp_path / key
    directory.mkdir(parents=True)
    rows = [
        {
            "period": "2026-07",
            "location": "TX",
            "stateDescription": "Texas",
            "sectorid": "1",
            "sectorDescription": "Electric Utility",
            "fueltypeid": "NG",
            "fuelTypeDescription": "Natural Gas",
            "generation": "2.0",
            "generation-units": "thousand megawatthours",
        }
    ]
    page = json.dumps(rows).encode()
    sha = hashlib.sha256(page).hexdigest()
    content = hashlib.sha256(sha.encode()).hexdigest()
    (directory / "page.json").write_bytes(page)
    manifest = {
        "run_id": run,
        "content_hash": content,
        "source": "eia_api_v2_live",
        "frequency": "monthly",
        "measure": "generation",
        "record_count": 1,
        "route": "electricity/electric-power-operational-data",
        "objects": [{"path": key + "/page.json", "sha256": sha}],
    }
    (directory / "manifest.json").write_text(json.dumps(manifest))
    (directory / "source_scope.json").write_text(
        json.dumps({"run_id": run, "content_hash": content})
    )
    return (
        {"raw_roots": [str(tmp_path)]},
        {"run_id": run, "content_hash": content, "extracted_at": "2026-11-06T00:00:00+00:00"},
        directory,
    )


def test_immutable_raw_provenance_is_read_without_modification(reporting, raw_example):
    source, audit, directory = raw_example
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    records = reporting.raw_records(source, audit)
    assert records[("2026-07", "TX", "1", "NG")].generation == 2
    assert before == {p.name: p.read_bytes() for p in directory.iterdir()}


@pytest.mark.parametrize("problem", ["page", "manifest", "scope"])
def test_missing_or_corrupt_raw_prevents_refresh(reporting, raw_example, problem):
    source, audit, directory = raw_example
    if problem == "page":
        (directory / "page.json").write_text("[]")
    elif problem == "manifest":
        (directory / "manifest.json").unlink()
    else:
        (directory / "source_scope.json").write_text('{"run_id":"wrong","content_hash":"wrong"}')
    with pytest.raises(ValueError):
        reporting.raw_records(source, audit)
