-- Q2. Top states by total generation (latest 12 months)
-- Answers: Which states generate the most electricity?
-- Source: mart_monthly_generation

SELECT
    location,
    location_name,
    SUM(generation_mwh) as total_generation_mwh
FROM {{ schema }}.mart_monthly_generation
WHERE generation_mwh IS NOT NULL
  AND period >= TO_CHAR(TO_DATE((SELECT MAX(period) FROM {{ schema }}.mart_monthly_generation), 'YYYY-MM') - INTERVAL '11 months', 'YYYY-MM')
GROUP BY location, location_name
ORDER BY total_generation_mwh DESC
LIMIT 20
