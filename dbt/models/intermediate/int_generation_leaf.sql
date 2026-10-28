-- Intermediate model: leaf-only generation data.
--
-- This is the "base side" — all leaf geographies x sectors x energy sources.
-- The reconciliation contract (data-model.md section 4, rule 4) requires that
-- summing this side reproduces the US + ALL + all-sectors total.
--
-- Roll-up rows (e.g. US, ALL, census divisions) are excluded here, NOT because
-- they are "wrong" but because they would double-count a leaf row. The full
-- data (including rollups) is available in stg_generation for ad-hoc analysis.

select *
from {{ ref('stg_generation') }}
where is_leaf
