-- Staging model: enrich core.fact_generation with dimension names from the
-- four conformed dimensions. Preserves ALL rows (including rollup geographies,
-- rollup energy sources) per the rollup contract (data-model.md section 4,
-- rule 1: "every source row is preserved"). The is_leaf flag is computed here;
-- aggregate models filter on it (rule 3).
--
-- The raw zone (raw.stg_generation) is a per-run scratch table; this model
-- reads from core.fact_generation, the merged current-state fact.

with fact as (

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
        content_hash
    from {{ source('core', 'fact_generation') }}

)

select
    f.period,
    f.location,
    f.sectorid,
    f.fueltypeid,
    f.generation_thousand_mwh,
    f.generation_mwh,
    f.is_rollup_geography,
    f.is_rollup_sector,
    f.is_rollup_energy_source,
    f.run_id,
    f.content_hash,

    -- Dimension descriptive attributes (may be NULL for unknown codes that
    -- were accepted but never upserted to the dimension; the publish gate
    -- normally blocks unknowns from fact_generation entirely).
    g.name                   as location_name,
    g.is_rollup              as dim_geography_is_rollup,
    s.name                   as sector_name,
    s.is_rollup              as dim_sector_is_rollup,
    e.name                   as energy_source_name,
    e.is_aggregate           as dim_energy_source_is_aggregate,

    -- Date dimension attributes
    d.date_month,
    d.year,
    d.month,

    -- is_leaf: True when the row is a leaf member on every joinable dimension.
    -- Unknown codes are blocked from fact_generation by the publish gate, so
    -- every code is either leaf or rollup — is_leaf is the negation of any
    -- rollup flag being True.
    not (f.is_rollup_geography
         or f.is_rollup_sector
         or f.is_rollup_energy_source) as is_leaf

from fact as f

left join {{ source('core', 'dim_geography') }}         as g
       on f.location = g.location
left join {{ source('core', 'dim_sector') }}           as s
       on f.sectorid = s.sectorid
left join {{ source('core', 'dim_energy_source') }}     as e
       on f.fueltypeid = e.fueltypeid
left join {{ source('core', 'dim_date') }}              as d
       on f.period = d.period
