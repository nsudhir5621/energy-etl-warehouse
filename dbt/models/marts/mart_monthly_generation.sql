-- Mart: final analytical table for monthly net electricity generation.
--
-- This utility-scale table excludes small-scale DPV in int_generation_leaf.
-- It contains only leaf members (non-rollup
-- geographies, sectors, and energy sources), so SUM(generation_mwh) produces an
-- accurate total with no double-counting from parent geographies or aggregate
-- fuel codes.
--
-- Materialized as a table for query performance. Refreshed on each dbt run.
--
-- Source semantics preserved:
--   - null means no data, 0 means measured zero (never COALESCE'd)
--   - negatives are valid (pumped storage / CHP net consumption)
--   - business key (period, location, sectorid, fueltypeid) is unique

{{ config(materialized='table', tags=['mart']) }}

select
    period,
    location,
    sectorid,
    fueltypeid,

    generation_thousand_mwh,
    generation_mwh,

    is_rollup_geography,
    is_rollup_sector,
    is_rollup_energy_source,

    run_id,
    content_hash,

    location_name,
    sector_name,
    energy_source_name,

    year,
    month,
    date_month
from {{ ref('int_generation_leaf') }}
