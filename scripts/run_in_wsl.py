#!/usr/bin/env python3
"""Run the fixture loader inside Linux/WSL with explicit connection settings.

This helper can apply schema DDL and load data. Use only an approved disposable
warehouse, with POSTGRES_DSN supplied securely by the caller. From the project
root, invoke ``python scripts/run_in_wsl.py``; it never selects a database for you.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project src to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

# Set raw storage for the pipeline; the caller supplies POSTGRES_DSN explicitly.
os.environ["RAW_ROOT"] = str(project_root / "data" / "raw")

# Keep imports after path/environment setup for direct script execution.
import psycopg  # noqa: E402

from energy_pipeline.config import Settings  # noqa: E402
from energy_pipeline.extract import extract_fixture  # noqa: E402
from energy_pipeline.load import load_raw_batch, merge_to_core  # noqa: E402
from energy_pipeline.storage import LocalObjectStore, RawSink  # noqa: E402
from energy_pipeline.warehouse import Warehouse  # noqa: E402

FIXTURE = str(project_root / "data" / "fixtures" / "eia" / "generation_monthly.json")


def main() -> int:
    print("=" * 60)
    print("Energy ETL Pipeline — End-to-End (WSL2 PostgreSQL)")
    print("=" * 60)

    # Connect to PostgreSQL
    print("\n[1/6] Connecting to PostgreSQL...")
    dsn = os.environ.get("POSTGRES_DSN", "")
    if not dsn.strip():
        raise ValueError("Set POSTGRES_DSN explicitly before running the fixture loader")
    conn = psycopg.connect(dsn)
    print(f"  ✅ Connected as: {conn.execute('SELECT current_user').fetchone()[0]}")
    print(f"  Database: {conn.execute('SELECT current_database()').fetchone()[0]}")

    # Load schema (skip if already initialized — avoids requiring CREATE
    # privilege on schemas owned by the administrator role).
    print("\n[2/6] Loading schema...")
    schema_sql = (project_root / "sql" / "001_schema.sql").read_text()

    existing = conn.execute(
        "SELECT string_agg(schema_name, ', ') FILTER (WHERE schema_name IS NOT NULL) "
        "FROM (VALUES ('raw'), ('core'), ('audit'), ('marts')) AS s(name) "
        "LEFT JOIN information_schema.schemata ON schemata.schema_name = s.name"
    ).fetchone()

    if existing[0] and "," in existing[0]:
        print(f"  ✅ Already initialized (skipping DDL): {existing[0]}")
    else:
        conn.execute(schema_sql)
        conn.commit()
        schemas = conn.execute(
            "SELECT string_agg(schema_name, ', ') "
            "FROM information_schema.schemata "
            "WHERE schema_name IN ('raw', 'core', 'audit', 'marts')"
        ).fetchone()
        print(f"  Schemas: {schemas[0]}")

    # 1. Extract from fixture
    print("\n[3/6] Extracting from fixture (offline)...")
    settings = Settings.from_env(
        {"RAW_ROOT": str(project_root / "data" / "raw"), "EIA_API_KEY": ""}
    )
    result = extract_fixture(FIXTURE, settings=settings)
    print(f"  rows extracted: {len(result.merge_rows)}")
    print(f"  content_hash:   {result.batch.content_hash[:16]}...")

    # 2. Write to immutable local raw zone
    print("\n[4/6] Writing to local raw zone...")
    store = LocalObjectStore(settings.raw_root)
    written = RawSink(store).write(result.batch)
    print(f"  objects written: {len(written.object_paths) + 1}")
    print(f"  run_id:          {result.batch.run_id}")
    print(f"  manifest_path:   {written.manifest_path}")

    # 3. Load raw zone → PostgreSQL staging + core MERGE
    print("\n[5/6] Loading raw zone → PostgreSQL...")
    wh = Warehouse(dsn)
    with wh.connect() as pg_conn:
        staged = load_raw_batch(
            store=store,
            conn=pg_conn,
            manifest_path=written.manifest_path,
        )
        print(f"  staged rows:    {staged.staged_rows}")
        print(f"  status:         {staged.status}")

        merge_result = merge_to_core(pg_conn, run_id=result.batch.run_id)
        print(f"  merged rows:    {merge_result.merged_rows}")
        print(f"  partitions:     {merge_result.partitions_created}")
        print(f"  merge status:   {merge_result.status}")

    # 4. Verify results
    print("\n[6/6] Verifying database contents...")
    conn = psycopg.connect(dsn)

    stg_count = conn.execute(
        "SELECT COUNT(*) FROM raw.stg_generation WHERE run_id = %s",
        (result.batch.run_id,),
    ).fetchone()[0]
    print(f"  raw.stg_generation rows (this run): {stg_count}")
    assert stg_count == staged.staged_rows

    # fact_generation is a cumulative upsert by business key. Verify the
    # current run's 10 business keys are present (deduplicated to 10 rows
    # across all runs). Equal-time identical observations may retain existing
    # provenance, so verify business keys rather than counting facts by run_id.
    fact_count = conn.execute(
        "SELECT COUNT(*) FROM core.fact_generation AS f "
        "JOIN raw.stg_generation AS s "
        "  ON f.period = s.period "
        "  AND f.location = s.location "
        "  AND f.sectorid = s.sectorid "
        "  AND f.fueltypeid = s.fueltypeid "
        "WHERE s.run_id = %s",
        (result.batch.run_id,),
    ).fetchone()[0]
    print(f"  core.fact_generation rows (matching this run's keys): {fact_count}")
    assert fact_count == staged.staged_rows

    audit = conn.execute(
        "SELECT status, row_count FROM audit.ingestion_run WHERE run_id = %s",
        (result.batch.run_id,),
    ).fetchone()
    print(f"  audit: status={audit[0]}, row_count={audit[1]}")

    null_count = conn.execute(
        "SELECT COUNT(*) FROM core.fact_generation WHERE generation_thousand_mwh IS NULL"
    ).fetchone()[0]
    print(f"  NULL generation rows: {null_count}")

    neg_count = conn.execute(
        "SELECT COUNT(*) FROM core.fact_generation WHERE generation_thousand_mwh < 0"
    ).fetchone()[0]
    print(f"  negative generation rows: {neg_count}")

    dims = conn.execute(
        "SELECT (SELECT COUNT(*) FROM core.dim_date) AS d, "
        "(SELECT COUNT(*) FROM core.dim_geography) AS g, "
        "(SELECT COUNT(*) FROM core.dim_sector) AS s, "
        "(SELECT COUNT(*) FROM core.dim_energy_source) AS f"
    ).fetchone()
    print(f"  dimensions: date={dims[0]}, geo={dims[1]}, sector={dims[2]}, fuel={dims[3]}")

    conn.close()
    print("\n✅ Pipeline executed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
