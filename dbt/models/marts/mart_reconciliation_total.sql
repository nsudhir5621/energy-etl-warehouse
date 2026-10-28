-- Mart: reconciliation total for the rollup contract.
--
-- "US + all sectors + ALL" is the designated reconciliation total
-- (data-model.md section 4, rule 4). Every period, summing the leaf-side
-- (mart_monthly_generation) must reproduce this total within a tolerance.
--
-- This table stores the source-side total so analysts can compare:
--   SELECT sum(generation_mwh) FROM mart_monthly_generation GROUP BY period
--   vs
--   SELECT generation_mwh  FROM mart_reconciliation_total   GROUP BY period
--
-- The reconciliation itself is enforced by the data test
-- tests/test_reconciliation.sql (run `dbt test --select test_reconciliation`).

{{ config(materialized='table', tags=['reconciliation']) }}

select
    period,
    location,
    sectorid,
    fueltypeid,

    generation_thousand_mwh,
    generation_mwh,

    is_rollup_geography,
    is_rollup_sector,
    is_rollup_energy_source

from {{ source('core', 'fact_generation') }}
where location = 'US'
  and fueltypeid = 'ALL'
  and sectorid in ('1', '2', '3', '4', '5', '6', '7')
