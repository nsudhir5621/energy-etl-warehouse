SELECT fueltypeid AS "Fuel code", generation_mwh AS "Generation (MWh)",
       CASE WHEN generation_mwh < 0 THEN 'Negative observation preserved'
            ELSE 'Measured zero preserved' END AS "Meaning"
FROM marts.mart_monthly_generation
WHERE generation_mwh <= 0
ORDER BY generation_mwh, fueltypeid;
