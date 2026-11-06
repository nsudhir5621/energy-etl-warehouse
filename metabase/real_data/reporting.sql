-- Apply only in a NEW identity-verified energy_real_reporting* database.
-- The operator creates its NOLOGIN owner and grants reader access separately.
DO $guard$ BEGIN
    IF current_database() NOT LIKE 'energy_real_reporting%'
       OR current_user NOT LIKE 'energy_real_owner%' THEN
        RAISE EXCEPTION 'Dedicated real-reporting database and owner required';
    END IF;
END $guard$;
CREATE SCHEMA marts;
CREATE TABLE marts.source_records (
    period text NOT NULL, location text NOT NULL, sectorid text NOT NULL,
    fueltypeid text NOT NULL,
    generation_thousand_mwh double precision, generation_mwh double precision,
    is_rollup_geography boolean NOT NULL, is_rollup_sector boolean NOT NULL,
    is_rollup_energy_source boolean NOT NULL,
    run_id uuid NOT NULL, content_hash text NOT NULL,
    location_name text, sector_name text, energy_source_name text,
    year integer NOT NULL, month integer NOT NULL, date_month date NOT NULL,
    is_leaf boolean NOT NULL,
    PRIMARY KEY (period, location, sectorid, fueltypeid)
);
CREATE TABLE marts.snapshot_metadata (
    singleton boolean PRIMARY KEY CHECK (singleton),
    snapshot_id uuid NOT NULL, refreshed_at timestamptz NOT NULL,
    source_observed_min timestamptz NOT NULL, source_observed_max timestamptz NOT NULL,
    period text NOT NULL, location text NOT NULL, sectorid text NOT NULL,
    source_total_mwh double precision NOT NULL,
    source_rows integer NOT NULL, leaf_rows integer NOT NULL,
    provenance jsonb NOT NULL
);
CREATE VIEW marts.mart_monthly_generation AS
SELECT period, location, sectorid, fueltypeid,
       generation_thousand_mwh, generation_mwh,
       is_rollup_geography, is_rollup_sector, is_rollup_energy_source,
       run_id, content_hash, location_name, sector_name, energy_source_name,
       year, month, date_month
FROM marts.source_records
WHERE is_leaf AND sectorid IN ('1','2','3','4','5','6','7') AND fueltypeid <> 'DPV';
CREATE VIEW marts.pilot_reconciliation AS
SELECT m.period, m.location, m.sectorid, m.source_total_mwh,
       (SELECT generation_mwh FROM marts.source_records WHERE fueltypeid='ALL') AS warehouse_total_mwh,
       (SELECT sum(generation_mwh) FROM marts.mart_monthly_generation) AS leaf_total_mwh,
       (SELECT sum(generation_mwh) FROM marts.mart_monthly_generation)-m.source_total_mwh AS gap_mwh,
       m.source_rows, m.leaf_rows, m.source_rows-m.leaf_rows AS excluded_analytical_rows,
       m.refreshed_at, m.source_observed_min, m.source_observed_max, m.snapshot_id
FROM marts.snapshot_metadata m;
