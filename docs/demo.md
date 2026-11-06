# Demo and verification guide

Two demonstrations are verified: **10 synthetic fixture records across
June–August 2026**, and a bounded real EIA pilot for **Texas, July 2026,
Electric Utility sector 1** with 31 facts and 13 additive analytical rows.
Neither demonstrates complete nationwide publication.

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
temporary directories and fake transports/connections. The final audit on 7 November 2026 passed
779 tests with 41 deliberately skipped PostgreSQL cases. See
[verification.md](verification.md) for the separate database gate; future counts
can change.

## Isolated warehouse demonstration

A reproducible full deployment still needs portable provisioning work. Do not
interpret the following checklist as an automatic installer.

For a separate static reporting demonstration, see the
[canonical reporting setup](reporting.md). Its new-volume sample initializes
only analytical tables; it does not execute or verify this ETL walkthrough.

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

The package reads process environment variables. Live EIA entry points first use
a nonempty `EIA_API_KEY`, then lazily read `~/.config/energy-etl/eia.env`. They never
change the process environment. Fixture extraction, staging and ordinary
`Settings.from_env()` do not inspect that file. Custom live scripts can use
`Settings.from_env(require_api_key=True)`; `EiaClient` also resolves a missing key
on its first request.

The optional private file must belong to the executing user, have mode `0600`
(or read-only `0400`), and contain one literal `EIA_API_KEY=...` assignment,
optionally quoted. Blank lines and whole-line comments are supported; shell
expansion is never performed. The `~/.config/energy-etl` directory must be owned
by that user with mode `0700`; home/config directories must not be symlinks or
writable by other users. Symlink, hard-linked, nonregular, oversized and malformed
credential files are rejected without displaying their contents.

For one-time setup, create the private directory and enter the registered key
using a local editor or password-manager workflow. Never paste the key into chat,
shell arguments or the repository. Missing/blank credentials cause live commands
to stop before HTTP requests; fixture verification needs no key. Airflow resolves
the file under its executing user's home, not the developer's home.

Other dotenv files are not loaded automatically. The tracked `.env.example` is a
reference template, not permission to reuse local credentials.

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
- Automatic warehouse-to-reporting scheduling. The real pilot now has an
  explicit, tested [manual refresh](reporting.md#real-eia-reporting-snapshot).
  Initial reporting-database provisioning and Metabase setup remain operator steps.
  The canonical Compose sample does not execute ETL/dbt/Airflow.
- A direct Metabase import: the tracked dashboard JSON is a blueprint, not an
  API export, and its filter declarations are not wired to the card queries.
- An all-green fixture path in `scripts/init_pipeline.py`, which invokes
  unfiltered `dbt test`.
- A locked dependency set reproducing the tested tool versions.

These are concrete setup gaps, not evidence that live publication or scheduler
execution passed. The tracked Compose file defines a separate reporting project;
it does not provision the source warehouse or Airflow.

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


## Bounded real EIA execution

Provision your own disposable PostgreSQL 18 warehouse and restricted role; verify
its identity before applying the tracked schema. Use a new raw namespace and
isolated Airflow metadata directory. Never target an existing database merely
because its name resembles an example. The reporting Compose database is separate
from this source warehouse.

Supply your own registered EIA key through the protected loader described above.
Explicitly configure `POSTGRES_DSN`, every `DBT_PG_*` field, `RAW_ROOT`,
`DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `AIRFLOW_HOME` and the isolated Airflow
database/logging configuration. Ensure the virtual environment's `bin` directory
is on PATH so the DAG can invoke dbt. Do not copy private verification scripts,
administrator credentials or historical configuration from another machine.

For the bounded live branch, unset `FIXTURE_PATH` and configure:

```bash
unset FIXTURE_PATH
export EIA_START_PERIOD=2026-07 EIA_END_PERIOD=2026-07
export EIA_LOCATION=TX EIA_SECTOR_ID=1
export EIA_PAGE_SIZE=100 EIA_MAX_PAGES=1
export EIA_TIMEOUT_SECONDS=30 EIA_MAX_RETRIES=0
```

`EIA_FUELTYPE_ID` is optional; leave it unset for all returned fuels.
Both period bounds are required together. The page budget bounds successful
data pages; retries are separate requests, so the example disables them.
EIA may revise this historical month; future values or row counts must be
investigated rather than coerced to the recorded pilot.

After initializing a separate Airflow 3 metadata environment and configuring
its local DAG folder/bundle, the supported local execution interface is:

```python
from dags.energy_pipeline_dag import dag

result = dag.test(use_executor=False)
assert str(result.state) == "success"
```

This is stateful: it makes API requests, writes immutable raw objects, publishes
the explicitly selected warehouse and runs four dbt models. It is not an offline
test, an automatic installer or scheduler startup. Do not execute it until every
connection and storage boundary is independently verified.

The recorded pilot produced 31 core facts, 13 utility-scale analytical rows,
11,036,928.08 MWh and a 0.00 MWh scoped difference. Verify raw hashes, unique
business keys, accepted audit provenance, dimension joins and classifications,
unit conversion, and source ALL versus the additive sum independently.
Current facts can advance to a newer observation's provenance while measures and
business keys stay unchanged; that is intentional idempotency behavior.

Run the twelve applicable dbt tests separately with the exact national test
excluded. Then follow the [real reporting guide](reporting.md#real-eia-reporting-snapshot)
to provision separate SELECT-only reporting storage, invoke the public refresh
script, and create the five questions using your own Metabase administrator.
The dashboard blueprints do not automatically create application objects.

No private local helper is a prerequisite. A fresh-machine full-stack deployment
has not been verified; the manual provisioning and Airflow setup remain explicit
reproduction work. The retained pilot and the complete 17,423-row diagnostic have
different scopes. Sector 95 and national reconciliation remain unresolved.
