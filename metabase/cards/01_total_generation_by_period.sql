-- Q1. Total net generation by month (leaf-only, no double-counting)
-- Answers: How much electricity was generated this month?
-- Source: mart_monthly_generation

SELECT
    period,
    SUM(generation_mwh) as total_generation_mwh
FROM {{ schema }}.mart_monthly_generation
WHERE generation_mwh IS NOT NULL
GROUP BY period
ORDER BY period DESC
