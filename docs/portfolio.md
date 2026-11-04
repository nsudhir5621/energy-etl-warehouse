# Portfolio evidence and interview summary

## Evidence scope

The original verification through **28 October 2026** demonstrates a synthetic-fixture
pipeline and a separate live-extraction diagnostic. Results were reviewed for
documentation. The public-snapshot connection-safety changes were then checked
offline: 549 passed and 41 PostgreSQL cases were deliberately skipped. The live
pipeline and real PostgreSQL suite were not rerun.

| Verification | Recorded result | What it establishes |
|---|---|---|
| Offline Python tests | 549 passed; 41 database cases skipped | Unit, filesystem raw-storage, mocked DAG and static reporting coverage |
| Isolated PostgreSQL integration | 41 passed | Real SQL, commit visibility, rollback, revision precedence and concurrent publication |
| Live EIA extraction | 17,423 July 2026 records retrieved | API extraction works; UNKNOWN codes still block full-month publication |
| Airflow local execution | Four tasks succeeded in 56.6 seconds | Local `dag.test()` operation, not scheduler execution |
| Fixture warehouse | Ten facts; no duplicate keys or provenance mismatches | Synthetic data publication and audit consistency |
| dbt models | Four models succeeded | `stg_generation`, `int_generation_leaf`, `mart_monthly_generation`, `mart_reconciliation_total` |
| dbt tests | Twelve applicable tests passed | Fixture checks with `test_reconciliation` excluded |
| Reporting snapshot | Six monthly rows and one reconciliation row | Snapshot matched source marts and retained its fingerprints |
| Metabase | Six queries completed and all six cards rendered | Browser query values matched independent SQL |

The 549-case offline result is the public-snapshot safety gate; five additional
cases cover explicit/missing/blank connection configuration. The separate
PostgreSQL and live-extraction counts are retained from their approved stages. Airflow, warehouse, dbt and dashboard results were also
cross-checked against their saved result records. No private receipts, database
identifiers, connection strings, credentials or machine-specific paths are
included here.

## Fresh canonical reporting installation

On **5 November 2026**, a separate `energy_reporting` Compose project initialized
fresh PostgreSQL and Metabase volumes with PostgreSQL 18.6, Metabase v0.50.27 and
nginx 1.30.6. The repository's static sample loaded six monthly rows and one
reconciliation row. Private tmpfs secret delivery worked with PostgreSQL running
as its non-root OS user; the reader could SELECT while seven write/CREATE/TEMP
probes were denied with SQLSTATE `42501`.

Administrator, connection, six questions and dashboard were explicitly provisioned
through Metabase APIs. Chrome displayed four charts and two tables; all six
responses matched independent SQL, with no query, network or JavaScript errors.
The [public reporting guide](reporting.md) documents the configuration and manual
UI setup; no automatic dashboard installer is claimed.

This is a fresh **reporting-only** installation using a static synthetic dataset,
not a rerun of extraction, warehouse loading, dbt or Airflow. Images were already
available locally. Full-project fresh-install reproducibility, fresh-machine
image pulls and a stop/start lifecycle cycle remain separate verification work.

## Verified dashboard results

All six questions executed and rendered in the verified browser demonstration.
The genuine Metabase interface screenshot is retained with private verification
evidence and omitted from this public snapshot. The verified dashboard displayed:

- Monthly and state generation queries using the synthetic fixture.
- A zero-valued latest-month fuel row with an undefined share, displayed as a table.
- A negative sector observation preserved in the charts.
- An incomplete July reconciliation comparison.
- Code labels where dimension display names are absent.

Chart/table formatting may round numbers; captured browser response values were
compared with independent SQL at result precision.

The execution results below are **text summaries of recorded verification**.
This public snapshot contains no interface screenshots.

## Airflow and dbt execution summaries

| Airflow task | Recorded state |
|---|---|
| `extract` | Success |
| `load_staging` | Success |
| `merge_to_core` | Success |
| `run_dbt` | Success |

The local run took 56.6 seconds. The DAG is configured monthly, but no running
scheduler, recurring trigger or production deployment was verified.

Both dbt staging/intermediate views and both mart tables built successfully.
All twelve selected tests passed. The excluded node was
`test.energy_dbt.test_reconciliation`; it remains required for a complete dataset.

The dashboard uses a static SELECT-only reporting snapshot. Its reconciliation
query's INNER JOIN omits June/August missing source totals; the July mismatch is
not a national reconciliation pass. No interactive filters or continuous refresh
are claimed. Metabase's bundled local sample database is separate from the
project's sole external reporting connection.

## Resume bullets

- Built and demonstrated a fixture-based electricity-generation ELT pipeline
  using Python, PostgreSQL, dbt, Apache Airflow and Metabase.
- Implemented immutable raw storage, UNKNOWN-code publication gates and
  transactional UPSERTs with immutable provenance and stale-replay protection.
- Verified 549 offline tests and 41 real PostgreSQL cases; exercised four local
  Airflow tasks, four dbt models, twelve applicable dbt tests and six dashboard queries.

## GitHub description

Monthly electricity-generation ELT warehouse with Python, PostgreSQL, dbt,
Airflow and Metabase; verified fixture demo, immutable raw storage and
revision-safe publication.

## About 60 seconds in an interview

I built a monthly electricity-generation warehouse around EIA data. The main
challenge is correctness: the source mixes detailed and aggregate categories,
allows nulls and negative values, and can revise earlier observations.

Python retrieves and validates the data, preserves immutable raw pages, and loads
PostgreSQL staging. Core publication uses the monthly business key, transactional
UPSERTs and provenance checks to reject stale or ambiguous replacements.
Unclassified codes block publication instead of silently entering totals.

Airflow coordinates four tasks, dbt builds analytical marts, and Metabase reads
a separate reporting snapshot. I verified 549 offline tests, 41 real PostgreSQL
integration cases, and a local fixture demonstration through all four Airflow
tasks to six rendered dashboard questions.

I also retrieved a live month of EIA data, but did not publish it because
classification remains incomplete. The demonstration is synthetic; national
reconciliation and scheduler-driven operation are still unverified.
