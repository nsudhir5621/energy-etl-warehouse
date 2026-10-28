#!/usr/bin/env python3
"""Optional fixture-to-warehouse helper for an already provisioned environment.

This stateful helper performs the following steps:

    1. Extracts from the offline fixture (no EIA_API_KEY needed).
    2. Stages raw objects into the immutable local raw zone.
    3. Loads raw zone -> PostgreSQL staging (raw.stg_generation).
    4. Merges staging -> core.fact_generation (with publish gate).
    5. Runs dbt (run + test) to populate the marts layer.

Prerequisites:
    - An approved disposable PostgreSQL database with sql/001_schema.sql applied.
    - dbt installed (pip install -e ".[dev]").
    - Explicit POSTGRES_DSN and all DBT_PG_* profile settings, supplied securely.
    - A fresh RAW_ROOT namespace; run from the repository root.

Usage:
    python scripts/init_pipeline.py

This is not a fresh-machine installer. It runs unfiltered dbt tests, including
national reconciliation that the incomplete fixture cannot pass. See docs/demo.md
for the controlled fixture procedure. The repository Compose example starts only
standalone Metabase; it does not provision PostgreSQL, Airflow or the private
reporting deployment, and does not represent the historical Metabase application.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure src/ is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from energy_pipeline.config import Settings
from energy_pipeline.extract import extract_fixture
from energy_pipeline.load import load_raw_batch, merge_to_core
from energy_pipeline.storage import LocalObjectStore, RawSink
from energy_pipeline.warehouse import Warehouse

FIXTURE = "data/fixtures/eia/generation_monthly.json"
RAW_ROOT = os.environ.get("RAW_ROOT", "data/raw")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "dbt")


def main() -> None:
    print("=" * 60)
    print("Energy ETL Pipeline - Local Init")
    print("=" * 60)

    # 1. Settings + extraction (offline fixture, no API key)
    settings = Settings.from_env({"RAW_ROOT": RAW_ROOT, "EIA_API_KEY": ""})
    print("\n[1/4] Extracting from fixture (offline)...")
    result = extract_fixture(FIXTURE, settings=settings)
    print(f"  rows extracted: {len(result.merge_rows)}")
    print(f"  content_hash:   {result.batch.content_hash[:16]}...")

    # 2. Write to immutable local raw zone
    print("\n[2/4] Writing to local raw zone...")
    store = LocalObjectStore(settings.raw_root)
    written = RawSink(store).write(result.batch)
    print(f"  objects written: {len(written.object_paths) + 1}")
    print(f"  run_id:          {result.batch.run_id}")

    # 3. Load raw zone -> PostgreSQL staging + core MERGE
    print("\n[3/4] Loading raw zone -> PostgreSQL...")
    manifest_path = written.manifest_path
    wh = Warehouse.from_env()
    with wh.connect() as conn:
        # Stage (raw zone -> raw.stg_generation)
        staged = load_raw_batch(
            store=store,
            manifest_path=manifest_path,
            conn=conn,
        )
        print(f"  staged rows:    {staged.staged_rows}")

        # Merge (raw.stg_generation -> core.fact_generation)
        merge_to_core(conn, run_id=result.batch.run_id)
        print("  core MERGE:     OK")

    # 4. Run dbt (models + tests)
    print("\n[4/4] Running dbt...")
    import subprocess

    dbt_env = os.environ.copy()

    # dbt run
    run_result = subprocess.run(
        ["dbt", "run", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR],
        env=dbt_env,
        capture_output=False,
    )
    if run_result.returncode != 0:
        print("  dbt run: FAILED", file=sys.stderr)
        sys.exit(1)
    print("  dbt run:        OK")

    # dbt test
    test_result = subprocess.run(
        ["dbt", "test", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR],
        env=dbt_env,
        capture_output=False,
    )
    if test_result.returncode != 0:
        print("  dbt test:       FAILED", file=sys.stderr)
        sys.exit(1)
    print("  dbt test:       OK")

    print("\n" + "=" * 60)
    print("Pipeline init complete.")
    print(f"  Raw zone:  {settings.raw_root}")
    print("  Database:  see POSTGRES_DSN env var")
    print("  Dashboard: http://localhost:3000 (Metabase)")
    print("=" * 60)


if __name__ == "__main__":
    main()
