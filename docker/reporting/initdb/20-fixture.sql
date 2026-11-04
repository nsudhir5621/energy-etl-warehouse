-- STATIC SYNTHETIC REPORTING SAMPLE, not an ETL/dbt execution or live EIA load.
-- Analytical projection of the verified six-row/one-row fixture snapshot.
-- Source: data/fixtures/eia/generation_monthly.json, SHA-256:
-- c08217f50ed5991af3f9925a9d7abba8ca228c7a69aa3fb1d5b956727803c12d
-- Values are already MWh. Preserve NULL, measured zero and finite negatives.
-- No historical run UUID or fabricated ingestion provenance is assigned here.
\set ON_ERROR_STOP on
BEGIN;
DO $guard$
BEGIN
    IF current_database() <> 'energy_reporting'
       OR current_user <> 'energy_reporting_owner' THEN
        RAISE EXCEPTION 'This sample may initialize only its new reporting database';
    END IF;
END $guard$;

CREATE SCHEMA marts AUTHORIZATION energy_reporting_owner;
CREATE TABLE marts.mart_monthly_generation (
    period text NOT NULL,
    location text NOT NULL,
    sectorid text NOT NULL,
    fueltypeid text NOT NULL,
    generation_mwh double precision,
    location_name text,
    sector_name text,
    energy_source_name text,
    PRIMARY KEY (period, location, sectorid, fueltypeid)
);
CREATE TABLE marts.mart_reconciliation_total (
    period text NOT NULL,
    location text NOT NULL,
    sectorid text NOT NULL,
    fueltypeid text NOT NULL,
    generation_mwh double precision,
    PRIMARY KEY (period, location, sectorid, fueltypeid)
);

INSERT INTO marts.mart_monthly_generation
    (period, location, sectorid, fueltypeid, generation_mwh) VALUES
    ('2026-06', 'AK', '1', 'NG', 331840.757),
    ('2026-06', 'AK', '1', 'NUC', NULL),
    ('2026-07', 'AK', '1', 'NG', 331840.757),
    ('2026-07', 'TX', '1', 'NG', 8225824.146000001),
    ('2026-07', 'TX', '7', 'NG', -4444000.0),
    ('2026-08', 'AK', '1', 'NG', 0.0);
INSERT INTO marts.mart_reconciliation_total
    (period, location, sectorid, fueltypeid, generation_mwh) VALUES
    ('2026-07', 'US', '1', 'ALL', 412888123.45600003);

GRANT USAGE ON SCHEMA marts TO energy_reporting_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO energy_reporting_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE energy_reporting_owner IN SCHEMA marts
    GRANT SELECT ON TABLES TO energy_reporting_reader;
COMMIT;
