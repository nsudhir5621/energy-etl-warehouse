-- Q3. Generation mix by energy source (latest period)
-- Answers: What's the fuel mix in the most recent month?
-- Source: mart_monthly_generation

SELECT
    fueltypeid,
    energy_source_name,
    SUM(generation_mwh) as total_generation_mwh,
    ROUND((100.0 * SUM(generation_mwh) / NULLIF(SUM(SUM(generation_mwh)) OVER (), 0))::numeric, 2) as pct_share
FROM {{ schema }}.mart_monthly_generation
WHERE generation_mwh IS NOT NULL
  AND period = (SELECT MAX(period) FROM {{ schema }}.mart_monthly_generation)
GROUP BY fueltypeid, energy_source_name
ORDER BY total_generation_mwh DESC
