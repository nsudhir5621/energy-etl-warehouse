"""Airflow DAG for the Energy ETL Warehouse pipeline (Phase 5).

Orchestrates: Extract -> Raw storage -> Load staging -> Merge to core -> Run dbt.

Reuses all implemented pipeline modules — nothing is duplicated in the DAG.
Configuration is externalized via environment variables and Airflow connections.

The pipeline can run in two modes:
  - **Live**: uses EIA_API_KEY + ApiSource to fetch from the EIA API.
  - **Fixture**: uses a local fixture file path (FIXTURE_PATH env var) instead,
    which is how the unit test suite exercises extraction offline.

An unchanged retry with matching immutable run metadata is idempotent. Core
publication uses PostgreSQL INSERT ... ON CONFLICT ... DO UPDATE: newer identical
observations advance provenance; equal-time identical observations retain the
existing provenance. Stale or conflicting observations are rejected atomically.
Observation time is local extracted_at, not an authoritative EIA revision time.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "energy-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="energy_etl_pipeline",
    description=(
        "EIA electricity generation pipeline: extract -> raw zone -> "
        "PostgreSQL staging -> core MERGE -> dbt transformations"
    ),
    default_args=DEFAULT_ARGS,
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["energy", "elt", "dbt"],
) as dag:
    # ------------------------------------------------------------------ #
    # Task 1: Extract (API or fixture) -> immutable raw zone              #
    # ------------------------------------------------------------------ #
    def extract_task(**context) -> str:
        """Extract EIA data and write to the immutable raw zone.

        Pushes run_id, manifest_path and content_hash to XCom for downstream task.
        """
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import PeriodWindow, extract, extract_fixture
        from energy_pipeline.storage import LocalObjectStore, RawSink

        settings = Settings.from_env()
        fixture_path = os.environ.get("FIXTURE_PATH")

        if fixture_path:
            # Offline mode — use the fixture instead of the live API.
            result = extract_fixture(fixture_path, settings)
        else:
            if not settings.api_key:
                raise ValueError(
                    "EIA_API_KEY is not set and FIXTURE_PATH is not set. Cannot run extraction."
                )
            from energy_pipeline.client import EiaClient
            from energy_pipeline.extract import ApiSource

            client = EiaClient(settings)
            # Discovery sends no window bounds; the placeholder only satisfies
            # ApiSource's constructor until the real lookback is known.
            discovery_source = ApiSource(client, settings, PeriodWindow("1970-01", "1970-01"))
            # latest_period() tells us the newest available period; we
            # back-fill 24 months (settings.lookback_months) by default.
            period_window = PeriodWindow.lookback(
                discovery_source.latest_period() or "1970-01",
                settings.lookback_months,
            )
            source = ApiSource(client, settings, period_window)
            result = extract(source, settings, period_window)

        store = LocalObjectStore(root=settings.raw_root)
        upload = RawSink(store).write(result.batch)

        context["ti"].xcom_push(key="run_id", value=upload.run_id)
        context["ti"].xcom_push(key="manifest_path", value=upload.manifest_path)
        context["ti"].xcom_push(key="content_hash", value=upload.content_hash)

        return upload.run_id

    # ------------------------------------------------------------------ #
    # Task 2: Load raw zone -> PostgreSQL staging (raw.stg_generation)    #
    # ------------------------------------------------------------------ #
    def load_staging_task(**context) -> int:
        """Stage raw zone data into PostgreSQL stg_generation.

        Returns the number of staged rows.
        """
        from energy_pipeline.config import Settings
        from energy_pipeline.load import STATUS_STAGED, load_raw_batch
        from energy_pipeline.storage import LocalObjectStore
        from energy_pipeline.warehouse import Warehouse

        settings = Settings.from_env()
        store = LocalObjectStore(root=settings.raw_root)
        warehouse = Warehouse.from_env()

        manifest_path = context["ti"].xcom_pull(key="manifest_path", task_ids="extract")

        with warehouse.connect() as conn:
            result = load_raw_batch(store, conn, manifest_path)

        assert result.status == STATUS_STAGED, (
            f"Expected staging status 'staged', got '{result.status}'"
        )
        return result.staged_rows

    # ------------------------------------------------------------------ #
    # Task 3: Merge staged -> core.fact_generation                        #
    # ------------------------------------------------------------------ #
    def merge_to_core_task(**context) -> str:
        """Merge staged rows into core.fact_generation.

        Returns the audit status (succeeded / empty). Raises for any other
        status (failed / staged), which triggers Airflow's retry + alerting.
        """
        from energy_pipeline.load import (
            STATUS_EMPTY,
            STATUS_SUCCEEDED,
            merge_to_core,
        )
        from energy_pipeline.warehouse import Warehouse

        warehouse = Warehouse.from_env()
        run_id = context["ti"].xcom_pull(key="run_id", task_ids="extract")

        with warehouse.connect() as conn:
            result = merge_to_core(conn, run_id)

        if result.status not in (STATUS_SUCCEEDED, STATUS_EMPTY):
            raise RuntimeError(
                f"merge_to_core returned unexpected status '{result.status}' "
                f"(expected '{STATUS_SUCCEEDED}' or '{STATUS_EMPTY}')"
            )

        return result.status

    # ------------------------------------------------------------------ #
    # Task 4: Run dbt transformations                                     #
    # ------------------------------------------------------------------ #
    def run_dbt_task(**context) -> None:
        """Run dbt to build mart models on top of the merged fact data.

        Uses the dbt project in the dbt/ directory. Requires dbt-core and
        dbt-postgres installed, and DBT_PROFILES_DIR pointing to dbt/profiles.yml.
        """
        from subprocess import run

        dbt_project_dir = os.environ.get("DBT_PROJECT_DIR", "dbt")
        dbt_profiles_dir = os.environ.get("DBT_PROFILES_DIR", dbt_project_dir)

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = dbt_profiles_dir

        proc = run(
            ["dbt", "run", "--project-dir", dbt_project_dir],
            env=env,
            capture_output=True,
            text=True,
        )
        print(proc.stdout)
        if proc.returncode != 0:
            print(proc.stderr)
            raise RuntimeError(f"dbt run failed with code {proc.returncode}")

    extract_op = PythonOperator(task_id="extract", python_callable=extract_task)
    load_staging_op = PythonOperator(task_id="load_staging", python_callable=load_staging_task)
    merge_core_op = PythonOperator(task_id="merge_to_core", python_callable=merge_to_core_task)
    run_dbt_op = PythonOperator(task_id="run_dbt", python_callable=run_dbt_task)

    # Dependency chain: each task waits for the previous.
    extract_op >> load_staging_op >> merge_core_op >> run_dbt_op
