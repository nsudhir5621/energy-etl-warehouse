-- Data test: assert mart_monthly_generation contains only leaf rows.
--
-- Per the rollup contract (data-model.md section 4, rule 3), every aggregate
-- model must filter to leaf members. This test returns 0 rows (passing) when
-- no rollup code contaminates the mart.

select *
from {{ ref('mart_monthly_generation') }}
where is_rollup_geography
   or is_rollup_sector
   or is_rollup_energy_source
