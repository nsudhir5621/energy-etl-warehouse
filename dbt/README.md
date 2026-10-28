# dbt — Energy ETL Warehouse

## Connection and invocation

Run the commands below from the repository root, with dbt installed in the active
Python environment. Supply all five profile variables securely: `DBT_PG_HOST`,
`DBT_PG_PORT`, `DBT_PG_DBNAME`, `DBT_PG_USER` and `DBT_PG_PASSWORD`. Explicitly select
an approved disposable warehouse and verify its identity before running models.
The profile supplies no connection defaults; use an explicitly empty password
only when the configured authentication permits it. Do not put secrets in shell
arguments or documentation.

```bash
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
```

The repository Compose example starts standalone Metabase only. It is separate
from the verified private reporting PostgreSQL/Metabase/proxy deployment and the
original historical Metabase application. None provisions this dbt warehouse.
See [the demo guide](../docs/demo.md) for prerequisites and recorded scope.

## Fixture vs. production test runs

The fixture in `data/fixtures/eia/` is a deliberately incomplete subset of the
EIA-923 / API data: 10 rows across three periods (June, July, August 2026).
It exercises the full transformation pipeline but is **not** a complete national
dataset.

### Reconciliation test

`test_reconciliation.sql` enforces the production invariant (data-model.md §4,
rule 4): the sum of all leaf-side generation must reproduce the US + ALL + all-
sectors reconciliation total within 0.01 MWh.

The fixture cannot satisfy this invariant by design — it contains only a
handful of leaf rows while the reconciliation total reflects the full national
value. This is **expected** and is the test working correctly.

### Running tests against the fixture

Exclude the reconciliation test when running against incomplete fixture data:

```bash
dbt test --project-dir dbt --profiles-dir dbt --exclude test_reconciliation
```

### Full-dataset validation

When running against a complete production dataset (or when the fixture is
superseded by full EIA data), run the **complete** test suite:

```bash
dbt test --project-dir dbt --profiles-dir dbt   # includes test_reconciliation
```

Excluding `test_reconciliation` for fixture-based development is not evidence
that production reconciliation passes. The test must be included whenever a
complete dataset is present.