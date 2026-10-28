-- Q6. Net generation trend by sector (last 24 months)
-- Answers: How has generation evolved by sector over time?
-- Source: mart_monthly_generation -- pivot on sectorid for trend visualization

SELECT
    period,
    sectorid,
    sector_name,
    SUM(generation_mwh) as total_generation_mwh
FROM {{ schema }}.mart_monthly_generation
WHERE generation_mwh IS NOT NULL
  AND period >= TO_CHAR(TO_DATE((SELECT MAX(period) FROM {{ schema }}.mart_monthly_generation), 'YYYY-MM') - INTERVAL '23 months', 'YYYY-MM')
GROUP BY period, sectorid, sector_name
ORDER BY period, sectorid
