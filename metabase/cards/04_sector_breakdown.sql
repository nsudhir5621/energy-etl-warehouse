-- Q4. Generation by sector (latest 12 months)
-- Answers: How does generation break down by EIA sector?
-- Source: mart_monthly_generation

SELECT
    sectorid,
    sector_name,
    SUM(generation_mwh) as total_generation_mwh
FROM {{ schema }}.mart_monthly_generation
WHERE generation_mwh IS NOT NULL
  AND period >= TO_CHAR(TO_DATE((SELECT MAX(period) FROM {{ schema }}.mart_monthly_generation), 'YYYY-MM') - INTERVAL '11 months', 'YYYY-MM')
GROUP BY sectorid, sector_name
ORDER BY total_generation_mwh DESC
