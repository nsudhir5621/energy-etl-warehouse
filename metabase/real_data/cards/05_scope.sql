SELECT 'Scope' AS "Detail", 'Texas / Electric Utility sector 1 / July 2026' AS "Value"
UNION ALL SELECT 'Data source','Real EIA API -> warehouse/dbt -> reporting snapshot'
UNION ALL SELECT 'Source observed (UTC)',to_char(source_observed_max AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') FROM marts.snapshot_metadata
UNION ALL SELECT 'Snapshot refreshed (UTC)',to_char(refreshed_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') FROM marts.snapshot_metadata
UNION ALL SELECT 'Refresh behavior','Manual copy; browser refresh only rereads reporting data'
UNION ALL SELECT 'National coverage','Not claimed; sector 95 and national reconciliation unresolved'
UNION ALL SELECT 'Provenance',string_agg(value->>'run_id',', ') FROM marts.snapshot_metadata,jsonb_array_elements(provenance);
