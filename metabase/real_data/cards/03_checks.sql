SELECT 'API raw ALL (MWh)' AS "Check", to_char(source_total_mwh,'FM999,999,999,990.00') AS "Value" FROM marts.pilot_reconciliation
UNION ALL SELECT 'Warehouse ALL (MWh)', to_char(warehouse_total_mwh,'FM999,999,999,990.00') FROM marts.pilot_reconciliation
UNION ALL SELECT 'Additive dbt sum (MWh)', to_char(leaf_total_mwh,'FM999,999,999,990.00') FROM marts.pilot_reconciliation
UNION ALL SELECT 'Reconciliation gap (MWh)', to_char(gap_mwh,'FM999,999,999,990.00') FROM marts.pilot_reconciliation
UNION ALL SELECT 'Source/core rows',source_rows::text FROM marts.pilot_reconciliation
UNION ALL SELECT 'Additive analytical rows',leaf_rows::text FROM marts.pilot_reconciliation
UNION ALL SELECT 'Excluded only from analytical sum',excluded_analytical_rows::text FROM marts.pilot_reconciliation;
