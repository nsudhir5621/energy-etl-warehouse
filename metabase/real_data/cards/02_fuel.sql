SELECT fueltypeid AS "Fuel code", generation_mwh AS "Generation (MWh)"
FROM marts.mart_monthly_generation
ORDER BY generation_mwh DESC, fueltypeid;
