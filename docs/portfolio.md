# Portfolio evidence and interview summary

## Project description

Energy ETL Warehouse processes monthly U.S. Energy Information Administration
electricity-generation data using Python, immutable raw storage, PostgreSQL,
dbt, Apache Airflow and Metabase. It demonstrates both a synthetic fixture
pipeline and a reconciled, bounded real-data pilot. It does not claim national
publication or scheduled production operation.

## Bounded real-data demonstration

The real pilot covers **Texas, July 2026, Electric Utility sector 1**, across all
returned fuel categories. It is not total Texas generation.

| Measure | Verified result |
|---|---:|
| Source records and core facts | 31 |
| Additive utility-scale analytical rows | 13 |
| Source ALL, warehouse ALL and analytical sum | 11,036,928.08 MWh |
| Scoped reconciliation gap | 0.00 MWh |
| Measured zero core / analytical observations | 8 / 4 |
| Negative observation | OTH: -86 MWh |
| Duplicate business keys / logical dimension or audit orphans | 0 / 0 |

Raw/core retain non-additive source representations. The analytical mart excludes
those representations, Residential sector 8 and small-scale DPV explicitly.
It never sums parent totals together with their components.

A manual load and two local Airflow DAG executions published the same scoped
observations under separate immutable run identities. Each DAG completed
`extract -> load_staging -> merge_to_core -> run_dbt` with `use_executor=False`.
Business keys and measures stayed stable while newer identical observations
advanced effective provenance. An unchanged same-run retry was a no-op.
An intentional extraction failure prevented downstream tasks from running and
left the accepted warehouse unchanged. Runtime retries were disabled for those
bounded local checks; the repository retry configuration was not changed.

Four dbt models built successfully. Twelve applicable tests passed; the national
`test.energy_dbt.test_reconciliation` node was excluded because this single-state,
single-sector dataset cannot establish national closure. The scoped 0.00 MWh
check is separate and is not a national test pass.

## Reporting demonstrations

The existing fixture dashboard contains six SQL questions over a static synthetic
six-row monthly / one-row reconciliation sample. Its incomplete comparison and
undefined latest-month fuel share remain disclosed.

A separate real-data collection contains five questions:

- Exact scoped generation total.
- Additive generation by fuel code.
- Source-to-warehouse-to-dbt reconciliation and row counts.
- Measured zeros and negative generation.
- Scope, accepted provenance, source observation time and snapshot freshness.

Both dashboards rendered in an actual Chrome browser, and their query results
matched independent PostgreSQL SQL. The final audit rechecked all eleven saved
queries. A [cropped real-dashboard screenshot](assets/texas-july-2026-eia-dashboard.png)
is included publicly, alongside SQL definitions and descriptive dashboard blueprints.
The crop omits navigation, margins and the local ingestion-run identifier; data
values and charts are unchanged. Original captures and operational receipts remain private.

Metabase reads separate reporting storage through a SELECT-only login. The real
warehouse retains Unix-socket-only access. An explicit, tested manual refresh
copies verified dbt outputs into a dedicated reporting database atomically.
Two refreshes of the same warehouse data preserved values/provenance and advanced
the snapshot timestamp. No new upstream ingestion was performed to test a changed
data value during that refresh check. Browser reload, application startup, query
caching and source ingestion are distinct operations; no automatic copy schedule
or working dashboard filter mappings are claimed.

## Verification and engineering boundaries

The final offline run on **7 November 2026** passed **779 tests**, with 41
PostgreSQL cases deliberately skipped. The final fresh, isolated PostgreSQL 18.6
rerun passed all 41 cases covering SQL, commit visibility, rollback, immutable identity,
revision precedence, stale replay, mixed-batch atomicity and advisory-lock
concurrency. See [verification.md](verification.md) for the final isolated rerun
status, commands and evidence boundaries.

Ruff lint/format, shell syntax and Compose configuration checks passed.
The tested tools include Python 3.11.8, PostgreSQL 18.6, dbt Core 1.12.6,
Airflow 3.3.2 and Metabase v0.50.27. Dependencies are declared, not locked.

The warehouse has primary/business-key constraints but no physical foreign keys.
Loader transactions and logical orphan/provenance checks support the demonstrated
path; they do not prevent arbitrary direct SQL corruption.

The complete July extraction contained 17,423 records. Captured source evidence
supports the curated additive basis, but 122 sector-95 records remain UNKNOWN
and national reconciliation is unresolved. The whole national batch was not
published. Nulls are not imputed to make reconciliation pass.

The reporting Compose installation and its stop/start persistence were verified
separately. This does not establish a fresh-machine installation of the full
ETL/dbt/Airflow stack. PostgreSQL provisioning, Airflow initialization,
Metabase object setup, dependency locking and CI remain explicit setup work.

## Historical fixture milestones

The 28 October 2026 public-snapshot gate recorded 549 offline passes and 41
intentionally skipped database cases; 41 real PostgreSQL cases passed separately.
The earlier fixture demonstration published ten facts across June-August 2026,
built four dbt models, passed twelve selected tests and rendered six questions.
Its recorded local Airflow run took 56.6 seconds. These are historical results,
not the current suite count or real-pilot duration.

A fresh reporting-only Compose installation was verified on 5 November 2026
with PostgreSQL 18.6, Metabase v0.50.27 and nginx 1.30.6. Existing local images were
used. The private application state, reporting sample and local persistence
checks are separate from fresh full-project deployment.

## Resume-ready achievements

- Built a real EIA electricity-generation pipeline with Python, PostgreSQL,
  dbt and Airflow; reconciled a Texas monthly sector pilot containing 31 facts
  and 13 analytical rows to 11,036,928.08 MWh with zero difference.
- Implemented immutable raw provenance, UNKNOWN-code publication gates,
  transactional UPSERTs and stale-replay protection; verified 779 offline tests
  and 41 separately executed PostgreSQL integration cases.
- Demonstrated two four-task local Airflow runs and a five-question real-data
  Metabase dashboard, using SELECT-only reporting access and an explicit manual
  snapshot refresh while preserving a separate fixture dashboard.

## GitHub description

Monthly EIA electricity-generation warehouse with Python, PostgreSQL, dbt,
Airflow and Metabase; reconciled real-data pilot, immutable provenance and
revision-safe loading.

## About 60 seconds in an interview

I built an electricity-generation warehouse around the EIA API. The correctness
challenge is that the source mixes detailed and overlapping categories, preserves
nulls and negatives, and revises earlier observations.

Python captures immutable raw pages and validates records. PostgreSQL publishes
by monthly business key with immutable run identity, transaction-scoped locking
and replay protection. dbt builds scoped analytical marts, and Airflow coordinates
four tasks.

I demonstrated Texas, July 2026, Electric Utility sector 1: 31 real facts and
13 additive rows reconcile to 11,036,928.08 MWh. Two local Airflow runs succeeded,
and five real-data Metabase questions matched SQL in the browser. Reporting is
a manually refreshed read-only copy, not a continuous warehouse connection.

The full national batch remains blocked by sector 95 and unresolved reconciliation.
That boundary is deliberate: I preserve unsupported observations and report the
gap instead of relabeling or dropping data to obtain a pass.
