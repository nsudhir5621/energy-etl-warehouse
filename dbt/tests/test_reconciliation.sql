-- Data test: leaf-side sum must reproduce the US + ALL + all-sectors
-- reconciliation total, within tolerance (data-model.md section 4, rule 4).
--
-- Returns 0 rows (passing) when the sums match within 0.01 MWh for every
-- period present on either side. Fails when:
--   - A period exists in the leaf mart but is absent from the reconciliation
--     mart (r.recon_sum is NULL).
--   - A period exists in the reconciliation mart but is absent from the leaf
--     mart (l.leaf_sum is NULL).
--   - Both periods exist but their sums differ by more than 0.01 MWh.
--
-- A FULL OUTER JOIN is used so that periods present on only one side are
-- surfaced rather than silently dropped by an inner join. NULL values from
-- the missing side cannot be compared with arithmetic, so they are detected
-- explicitly.

with leaf_total as (
    select
        period,
        sum(generation_mwh) as leaf_sum
    from {{ ref('mart_monthly_generation') }}
    group by period
),

recon_total as (
    select
        period,
        sum(generation_mwh) as recon_sum
    from {{ ref('mart_reconciliation_total') }}
    group by period
)

select
    coalesce(l.period, r.period) as period,
    l.leaf_sum,
    r.recon_sum,
    case
        when l.leaf_sum is null then 'missing in leaf mart'
        when r.recon_sum is null then 'missing in reconciliation mart'
        else abs(l.leaf_sum - r.recon_sum)::text
    end as diff
from leaf_total as l
full outer join recon_total as r
  on l.period = r.period
where l.leaf_sum is null
   or r.recon_sum is null
   or abs(l.leaf_sum - r.recon_sum) > 0.01
