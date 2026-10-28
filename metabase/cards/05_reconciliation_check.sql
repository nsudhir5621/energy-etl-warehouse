-- Q5. Reconciliation: leaf sum vs US+ALL total (data-model.md section 4, rule 4)
-- Answers: Are the leaf-side totals consistent with the reconciliation total?
-- Source: mart_monthly_generation + mart_reconciliation_total

WITH leaf_total AS (
    SELECT
        period,
        SUM(generation_mwh) as leaf_sum_mwh
    FROM {{ schema }}.mart_monthly_generation
    WHERE generation_mwh IS NOT NULL
    GROUP BY period
),

recon_total AS (
    SELECT
        period,
        SUM(generation_mwh) as recon_sum_mwh
    FROM {{ schema }}.mart_reconciliation_total
    WHERE generation_mwh IS NOT NULL
    GROUP BY period
)

SELECT
    l.period,
    l.leaf_sum_mwh,
    r.recon_sum_mwh,
    ROUND(ABS(l.leaf_sum_mwh - r.recon_sum_mwh)::numeric, 2) as abs_diff_mwh,
    ROUND((100.0 * ABS(l.leaf_sum_mwh - r.recon_sum_mwh) / GREATEST(r.recon_sum_mwh, 1))::numeric, 4) as pct_diff
FROM leaf_total l
JOIN recon_total r ON l.period = r.period
WHERE abs(l.leaf_sum_mwh - r.recon_sum_mwh) > 0.01
ORDER BY l.period DESC
