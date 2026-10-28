# Demo and verification guide

The verified end-to-end demonstration uses **10 synthetic fixture records across
June–August 2026**. It does not publish a complete month of live EIA data.

The commands below distinguish a runnable offline walkthrough from stateful
steps requiring a separately prepared disposable environment. The public-snapshot
safety changes are checked offline; no live pipeline or database test is rerun
without separately approved provisioning.

## Offline fixture walkthrough

From the repository root, create the Python environment and install the project
as shown in the [README](../README.md#start-offline). Then:

```bash
.venv/bin/python - <<'PY'
from energy_pipeline.extract import PeriodWindow, extract_fixture
from energy_pipeline.storage import InMemoryObjectStore, RawSink
from energy_pipeline.validation import aggregate_publish_gate

result = extract_fixture(
    "data/fixtures/eia/generation_monthly.json",
    window=PeriodWindow("2026-06", "2026-08"),
)
assert result.report.ok
assert not aggregate_publish_gate(result.merge_rows)

store = InMemoryObjectStore()
upload = RawSink(store).write(result.batch)
assert store.read(upload.manifest_path)

print({
    "fixture_rows": upload.record_count,
    "validation_ok": result.report.ok,
    "unknown_rows": result.report.unknown_row_count,
    "manifest_present": True,
})
PY
```

Expected fixture row count: **10**. This exercises fixture extraction through
the shared pagination/parsing path, validation, the publication gate check and
the raw-sink interface. The store is in memory: this example makes no API calls,
opens no database and writes no raw files. It does **not** demonstrate staging,
core publication or filesystem immutability by itself; the filesystem tests
exercise create-only writes.

For the full offline gate:

```bash
ENERGY_TEST_PG=0 .venv/bin/python -m pytest tests/ dags/tests/ metabase/tests/ -ra
```

Keep `tests/integration/test_local_raw_storage.py` in that collection. Tests use
temporary directories and fake transports/connections. The recorded result was
549 passed and 41 deliberately skipped PostgreSQL cases after the public-snapshot
safety changes; future counts can change.

## Isolated warehouse demonstration

A reproducible full deployment still needs portable provisioning work. Do not
interpret the following checklist as an automatic installer.

1. Provision a new disposable PostgreSQL database with its own restricted owner
   and private transport. Verify actual database, server and role identity before
   DDL or writes. Apply the unchanged `sql/001_schema.sql` only there.
2. Configure an isolated Airflow metadata environment and a fresh raw root.
   Set `FIXTURE_PATH` to the tracked monthly fixture before any DAG execution,
   so extraction cannot fall back to the live API.
3. Set explicit warehouse and dbt connection configuration. Confirm both point
   to the same disposable warehouse. Use the supported environment-loading
   interfaces, not local password-bearing setup utilities.
4. Execute the DAG locally through its `dag.test()` interface after Airflow
   initialization. The four tasks are `extract`, `load_staging`,
   `merge_to_core` and `run_dbt`. Check each task and final run state.
5. Independently verify ten published fixture facts, business-key uniqueness,
   provenance, source-unit conversion, null/zero/negative preservation and audit
   status. The resulting monthly/reconciliation mart row counts are six and one.
6. Execute the applicable dbt tests separately; the DAG runs only `dbt run`.
7. Copy only the two materialized marts into a separate reporting snapshot,
   verifying schema, values and fingerprints. Grant the reporting account
   SELECT-only access.
8. Use a clean Metabase application with no production connections. Keep it and
   reporting PostgreSQL backend-only; publish a fixed-upstream HTTP reverse
   proxy exclusively on host loopback.
9. Create six native SQL questions from `metabase/cards/`, replacing only the
   trusted `{{ schema }}` identifier with `marts`. Execute each question and
   compare its results to independent SQL before assembling the dashboard.
10. Verify actual browser charts/tables and preserve sanitized screenshots.

The package reads process environment variables; `Settings.from_env()` does not
automatically read a dotenv file. The tracked `.env.example` is a reference
template, not permission to reuse local credentials.

| Configuration | Required choice for a new demonstration |
|---|---|
| `FIXTURE_PATH` | `data/fixtures/eia/generation_monthly.json` |
| `RAW_ROOT` | A fresh, private raw directory retained as immutable evidence |
| `POSTGRES_DSN` | Explicit disposable warehouse connection, supplied securely |
| `DBT_PG_HOST` / `DBT_PG_PORT` | The disposable server's approved transport |
| `DBT_PG_DBNAME` / `DBT_PG_USER` | Explicit disposable database and restricted role |
| `DBT_PG_PASSWORD` | Explicit authentication value supplied securely; empty only when the configured authentication permits it |
| `DBT_PROJECT_DIR` / `DBT_PROFILES_DIR` | The repository's `dbt` directory |
| `AIRFLOW_HOME` and Airflow connection/configuration | A separate demo metadata environment |

Do not paste passwords into shell arguments, URLs, screenshots or documentation.
`Warehouse()` and the WSL helper now refuse missing or blank connection settings.
The dbt profile requires every connection environment variable; it supplies no
warehouse, user or password defaults. An explicitly empty dbt password is valid
only for a deliberately configured password-free authentication mechanism.

After the isolated warehouse is populated and its identity verified, the dbt
entry points are:

```bash
.venv/bin/dbt debug --profiles-dir dbt --project-dir dbt
.venv/bin/dbt run --profiles-dir dbt --project-dir dbt
.venv/bin/dbt test --profiles-dir dbt --project-dir dbt --exclude test_reconciliation
```

The last selector excludes exactly the national reconciliation test,
`test.energy_dbt.test_reconciliation`. Twelve applicable tests passed in the
recorded fixture demonstration. On a complete, classified dataset, run the full
test suite including reconciliation; fixture exclusion is not a production pass.

### What is not packaged yet

- An isolated PostgreSQL provisioning procedure reusable on a fresh machine.
- The Airflow metadata initialization and local-run environment used for verification.
- The reporting snapshot transfer and loopback proxy configuration.
- A direct Metabase import: the tracked dashboard JSON is a blueprint, not an
  API export, and its filter declarations are not wired to the card queries.
- An all-green fixture path in `scripts/init_pipeline.py`, which invokes
  unfiltered `dbt test`.
- A locked dependency set reproducing the tested tool versions.

These are concrete setup gaps, not evidence that live publication or scheduler
execution passed. The current tracked Compose file provides only Metabase.

## Real PostgreSQL integration tests

The opt-in `tests/integration/test_postgres_loader.py` harness uses real
transactions and independent observer connections. Its 41 recorded passing cases
cover inserts, retries, revisions, replay rejection, immutable identity,
mixed-batch rollback, partition routing and bounded concurrent publication.

The harness requires a newly provisioned private PostgreSQL 18 cluster. Its
configuration is explicit and validated without connecting. It fixes the disposable
database name to `energy_pg_test` and restricted role to `energy_pg_runner`; it
never selects a production connection. Provisioning is still separate. Never relax
the identity guards simply to connect.

Required controls:

| Variable | Meaning |
|---|---|
| `ENERGY_TEST_PG=1` | Explicitly enables database integration |
| `ENERGY_TEST_PG_CLUSTER_ROOT` | Absolute normalized private directory, without whitespace or symlink/writable ancestry |
| `ENERGY_TEST_PG_PORT` | Explicit unprivileged port from 1024–65535, excluding the default PostgreSQL port |
| `ENERGY_TEST_PG_CLUSTER_NAME` | Explicit test-specific name matching `energy_pg_test_[a-z0-9_]{1,40}` |
| `ENERGY_TEST_PG_DSN` | Exact canonical Unix-socket connection shown below; no password or production fallback |
| `ENERGY_TEST_PG_DATABASE_OID` | Independently verified database identity |

Canonical test DSN (substitute the explicitly provisioned root and port):

```text
host=<cluster-root>/socket port=<private-port> dbname=energy_pg_test user=energy_pg_runner connect_timeout=5 passfile=/dev/null
```

Run as a non-root user. The cluster root and its `socket` child must have mode
`0700`, belong to that user, and have trusted, non-writable, non-symlink ancestry.
The live identity guard also verifies PostgreSQL 18, database ownership, restricted
role, exact OID/cluster/socket/port, TCP disabled, and no database named `energy`.

Provisioning is separate: the database must be empty, owned by the restricted
test role, have the tracked schema, and have no fact partitions. Remove inherited
production PostgreSQL settings. Once every guard is satisfied, run sequentially:

```bash
.venv/bin/python -m pytest tests/integration/test_postgres_loader.py -x -ra
```

This is a state-mutating test suite. Preserve its results for review; a populated
database is not a safe starting point for another whole-suite run. Disabled or
skipped tests provide no PostgreSQL verification.
