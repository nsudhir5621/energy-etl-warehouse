# Verification record and release boundaries

Verification date: **7 November 2026**. This is an auditable record of a bounded
portfolio demonstration, not a production certification.

The offline, fresh isolated PostgreSQL, read-only dbt, reporting and actual
browser gates passed. The retained pilot and the three existing canonical
reporting containers were started after guarded identity/storage checks; no
containers, volumes or source databases were recreated or reset.

## What was demonstrated

| Dataset | Scope and outcome |
|---|---|
| Synthetic fixture | Ten input facts across June-August 2026; six monthly reporting rows and one incomplete reconciliation row |
| Real EIA warehouse | Texas, July 2026, Electric Utility sector 1; all 31 returned fuel observations retained in core |
| Real analytical mart | Thirteen additive utility-scale rows; 11,036,928.08 MWh |
| Scoped reconciliation | Raw ALL = warehouse ALL = additive sum; difference 0.00 MWh |
| Complete July diagnostic | 17,423 extracted records; not published nationally |

The real pilot has eight measured zero core observations, four measured zero
analytical observations and one negative (`OTH = -86 MWh`). It has no nulls.
Null preservation is established separately by fixture/unit/PostgreSQL tests;
nulls are never replaced with zero to obtain closure. Non-additive representations
remain in raw/core and are excluded from additive sums. Residential sector 8 and
small-scale DPV are valid source components outside the utility-scale mart.

Raw page and combined SHA-256 hashes, source origin, run identity, period and
scope were checked. Current facts retain the accepted latest run/hash provenance.
The warehouse has 31 current facts, 93 staging rows across three successful runs,
one monthly partition and no duplicate business keys. All four dbt relations exist:
`stg_generation` (31), `int_generation_leaf` (13), `mart_monthly_generation` (13)
and `mart_reconciliation_total` (0). The last relation targets national totals;
zero rows there are expected for this pilot, not a national reconciliation pass.

## Final checks

| Check | Result and evidence boundary |
|---|---|
| Offline pytest | 779 passed; 41 deliberately skipped PostgreSQL cases |
| Real PostgreSQL suite | 41 passed, 0 failed/skipped, in a new isolated PostgreSQL 18.6 target; retained pilot/evidence databases not reset |
| dbt tests | 12 passed, 0 failed/warnings; exactly the national reconciliation node excluded |
| Warehouse read-only verification | 31/13 rows, 0.00 MWh gap, zero duplicate keys, zero logical orphans and zero dimension-flag mismatches |
| Airflow structural/unit checks | Included in the offline suite |
| Airflow actual execution | Two recorded real-data `dag.test(use_executor=False)` runs, all four tasks successful; independent commit visibility verified |
| Airflow negative control | Deliberate extraction failure; all downstream tasks upstream_failed; accepted warehouse and raw history unchanged |
| Metabase | All eleven API and browser queries matched verified SQL after restart; both dashboards/collections preserved |
| Browser | Both dashboards visibly rendered in Chrome during the final audit; zero unexpected query/network/JavaScript errors |
| Manual reporting refresh | Two successful copies of the same accepted source; values/provenance stable and snapshot freshness advanced |
| Ruff | Lint and formatting passed for all 43 public Python files |
| Shell / Compose | Bash syntax and three-service Compose validation passed; no services created by validation |

The final audit started only the identity-verified existing services for read-only
checks. It did not run upstream extraction, rebuild source marts or trigger
another Airflow ingestion. It rechecked retained execution records, warehouse/raw
integrity and current read-only SQL/dbt/Metabase results. The same-run retry
preserves facts; a newer identical source observation intentionally advances
provenance without changing the business key or measure.

## Commands and setup boundary

The public offline gates are:

```bash
ENERGY_TEST_PG=0 .venv/bin/python -m pytest tests/ dags/tests/ metabase/tests/ -ra
git ls-files -z -- '*.py' | xargs -0 .venv/bin/ruff check --no-cache
git ls-files -z -- '*.py' | xargs -0 .venv/bin/ruff format --check --no-cache
bash -n docker/reporting/entrypoint.sh docker/reporting/initdb/10-reader.sh
docker compose --env-file .env.reporting config --quiet
```

For an uncommitted working tree, also include new public Python files in Ruff.
No credentials or local values are supplied by this document.

The state-mutating PostgreSQL suite requires an approved, newly provisioned
private PostgreSQL 18 target with empty application tables and no partitions.
Follow the exact [connection guards](demo.md#real-postgresql-integration-tests);
never rerun it against a populated evidence database or source warehouse:

```bash
.venv/bin/python -m pytest tests/integration/test_postgres_loader.py -x -ra
```

dbt checks use explicitly supplied disposable connection fields:

```bash
.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --exclude test_reconciliation
```

The exclusion is exactly `test.energy_dbt.test_reconciliation`. On a complete,
classified national dataset the full reconciliation test remains required.
The final pilot audit used PostgreSQL read-only sessions and separate local
dbt artifacts; it did not modify the retained marts.

See [demo.md](demo.md#bounded-real-eia-execution) for bounded live configuration
and [reporting.md](reporting.md#real-eia-reporting-snapshot) for the public refresh
script, private configuration template and explicit Metabase setup. New users
supply their own EIA key and administrator credentials. Private verification
helpers, machine paths, application volumes and historical receipts are not
installation dependencies or publication inputs.

## Critical release checklist

- [x] Offline tests and static checks pass.
- [x] Final isolated PostgreSQL rerun passes before publication.
- [x] Bounded raw/core/mart values, classification and provenance reconcile.
- [x] dbt tests pass with the national exclusion stated explicitly.
- [x] Local Airflow success, negative control and scheduling boundaries are distinguished.
- [x] Fixture and real reporting remain separate; all eleven queries match verified results.
- [x] Reader cannot INSERT, UPDATE, DELETE, TRUNCATE, CREATE or use temporary tables.
- [x] Public documentation states the demonstrated scope and known limits.
- [x] Current post-restart source/reporting runtime checks complete after guarded existing-service startup.
- [x] Final public inventory, links, staged privacy scan and Git checks pass before commit.

## Explicit non-blocking limitations

- Sector 95 remains UNKNOWN; 122 July observations block full-batch publication.
  National reconciliation and incomplete source cells remain unresolved.
- Airflow is configured monthly, but scheduler-driven production operation and
  operational retry execution have not been demonstrated.
- Reporting refresh is manual and must follow successful warehouse ingestion
  and dbt. Browser reload, query cache and application startup do not copy data.
  Refresh operators must run serially; monotonic ordering across concurrent
  reporting refreshes is not implemented.
- The schema has no physical foreign keys. Application publication is
  transactional; independent logical checks support the demonstrated path.
  Arbitrary direct SQL writers can bypass those safeguards.
- Dependency ranges are not locked, CI is not supplied, and a fresh-machine
  full-stack installation has not been verified. Initial PostgreSQL/Airflow
  provisioning and Metabase object creation remain explicit operator steps.
- The unused standalone `Warehouse.ensure_partition()` helper is outside the
  verified loader path. The active `load._ensure_partitions()` path is covered
  by the PostgreSQL tests; no standalone-helper database compatibility claim is made.
- The fixture comparison is incomplete and its latest-month fuel share is NULL.
  Dashboard filters are not implemented. Original interface captures remain private;
  the README includes a reviewed, cropped real-dashboard image with the local run
  identifier omitted and data values unchanged.

These limitations do not turn a correctly scoped pilot into a national or
production deployment. They must remain visible in any portfolio description.
