-- Phase 3: PostgreSQL warehouse schema (Stage 1 — infrastructure only)
-- Apply only to an approved, identity-verified disposable database:
-- psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 --single-transaction -f sql/001_schema.sql
-- The repository Compose example provides Metabase, not PostgreSQL.

-- Four schemas, created idempotently. `marts` is an empty placeholder for
-- Phase 4 (dbt analytical models). It is created now so the schema boundary
-- is explicit from day one, not invented later.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS marts;

-- ---------------------------------------------------------------------------
-- raw: verbatim typed landing. Rows land here during staging; they are never
-- edited in place, only truncated-and-reloaded per run_id (see load.py Stage 2).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.stg_generation (
    run_id           UUID,
    content_hash     TEXT,
    period           TEXT,          -- YYYY-MM
    location         TEXT,
    sectorid         TEXT,
    fueltypeid       TEXT,
    generation_thousand_mwh DOUBLE PRECISION,   -- nullable; null ≠ zero
    is_rollup_geography  BOOLEAN,
    is_rollup_sector     BOOLEAN,
    is_rollup_energy_source BOOLEAN,
    parsed_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (run_id, period, location, sectorid, fueltypeid)
);

CREATE INDEX IF NOT EXISTS idx_stg_generation_run_id
    ON raw.stg_generation (run_id);

-- ---------------------------------------------------------------------------
-- core: conformed dimensions + fact.
-- ---------------------------------------------------------------------------

-- dim_date — derived from period (YYYY-MM → first-of-month date)
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_month DATE PRIMARY KEY,
    period     TEXT UNIQUE NOT NULL,
    year       INT  NOT NULL,
    month      INT  NOT NULL CHECK (month BETWEEN 1 AND 12)
);

-- dim_geography — classifier accepts 52 leaf codes and 14 rollup strings.
-- This compatibility vocabulary is not a verified current API facet list.
CREATE TABLE IF NOT EXISTS core.dim_geography (
    geography_sk SERIAL PRIMARY KEY,
    location     TEXT UNIQUE NOT NULL,
    name         TEXT,
    is_rollup    BOOLEAN NOT NULL DEFAULT FALSE
);

-- dim_sector — seven currently classified leaf codes (1–7), with FALSE rollup flags.
-- Eight additional codes were historically reported; they remain UNKNOWN until curated.
CREATE TABLE IF NOT EXISTS core.dim_sector (
    sector_sk    SERIAL PRIMARY KEY,
    sectorid     TEXT UNIQUE NOT NULL,
    name         TEXT,
    is_rollup    BOOLEAN NOT NULL DEFAULT FALSE
);

-- dim_energy_source — 15 leaf + 12 aggregate; is_aggregate flags the latter
CREATE TABLE IF NOT EXISTS core.dim_energy_source (
    energy_source_sk SERIAL PRIMARY KEY,
    fueltypeid   TEXT UNIQUE NOT NULL,
    name         TEXT,
    is_aggregate BOOLEAN NOT NULL DEFAULT FALSE
);

-- fact_generation — partitioned by RANGE(period). Monthly partitions created
-- on demand by load._ensure_partitions() during merge_to_core().
CREATE TABLE IF NOT EXISTS core.fact_generation (
    period                   TEXT,
    location                 TEXT,
    sectorid                 TEXT,
    fueltypeid               TEXT,
    generation_thousand_mwh  DOUBLE PRECISION,   -- nullable; null ≠ zero
    generation_mwh           DOUBLE PRECISION,    -- × 1000 convenience
    is_rollup_geography      BOOLEAN,
    is_rollup_sector         BOOLEAN,
    is_rollup_energy_source  BOOLEAN,
    run_id                   UUID,
    content_hash             TEXT,
    PRIMARY KEY (period, location, sectorid, fueltypeid)
) PARTITION BY RANGE (period);

-- Index on the columns dashboards filter/join on (CLAUDE.md: data-model §13.4)
CREATE INDEX IF NOT EXISTS idx_fact_generation_dim_lookup
    ON core.fact_generation (location, sectorid, fueltypeid);

-- ---------------------------------------------------------------------------
-- audit: one row per ingestion run (run_id is the PK; updates via UPSERT)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.ingestion_run (
    run_id         UUID PRIMARY KEY,
    content_hash   TEXT,
    source         TEXT,
    window_start   TEXT,
    window_end     TEXT,
    extracted_at   TIMESTAMPTZ,
    row_count      INT,
    status         TEXT,   -- staged | succeeded | empty | failed
    hard_findings  INT,
    warn_findings  INT,
    info_findings  INT,
    error_msg      TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now()
);

-- Updated at on every update (tracks audit row mutations across retries)
CREATE OR REPLACE FUNCTION audit._touch_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ingestion_run_updated ON audit.ingestion_run;
CREATE TRIGGER trg_ingestion_run_updated
    BEFORE UPDATE ON audit.ingestion_run
    FOR EACH ROW EXECUTE FUNCTION audit._touch_updated();
