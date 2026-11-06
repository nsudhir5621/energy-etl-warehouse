# Data Model

**Current implementation:** the monthly grain, dimensional warehouse and publication
rules below are implemented. dbt contains four models: staging and intermediate
views, plus monthly-generation and reconciliation mart tables. Sector 95 classification and production reconciliation remain unresolved;
see the [B4.1 source update](source-data.md#b41-live-classification-evidence--5-november-2026).

Dated source checks below are historical reports, not fresh verification results.
See [source-data.md](source-data.md) for their evidence limits and
[portfolio.md](portfolio.md) for the fixture and bounded real-data verification scope.

---

## 1. Fact table grain

One sentence, and it is the definition everything else hangs off:

> **`fact_generation` — one row represents total net electricity generation for one
> reporting geography, in one sector, from one energy source, during one month.**

Derived, not assumed: the source file is plant-level; the API aggregates to
(month, geography, sector, energy source). Verified by reconciliation —
Alaska 2026-07, sector 1, all fuels = 594,571.04 MWh in **both** the file aggregation and
the API. **Uniqueness verified live** (17,392 rows, 17,392 distinct keys, 0 duplicates).

## 2. Business key

> **`(period, location, sectorid, fueltypeid)`**

Natural, composite, surrogate-free. PostgreSQL enforces this key and uses it for
`INSERT ... ON CONFLICT ... DO UPDATE` publication. Current dbt models are views
and tables; they do not declare an incremental `unique_key`. No fact surrogate is
added because the natural key already identifies the source observation.

**Uniqueness status:** ✅ **VERIFIED live** (2026-10-09).
`scripts/verify_api_contract.py --months 1` scanned 2026-07 in full through the production
`EiaClient` + `iter_pages` path: 17,392 rows, 17,392 distinct keys, 0 duplicates. The key is
**frozen**.

**Duplicate handling contract.** The pipeline must guarantee **at most one source row per
business key after batch-level deduplication**:

- Duplicates **within an incoming batch** are detected and resolved *before* publication. Rows
  sharing a key *and* an identical payload collapse to one. Rows sharing a key with
  **conflicting** payloads are a **hard validation failure** — the pipeline will not guess
  which is correct.
- Duplicate delivery **across separate batches** (a re-run, or a backfill re-reading a
  range already held) is checked against accepted provenance before transactional UPSERT.

```
raw batch  →  batch-level deduplication  →  guarded UPSERT on the business key
```

Surrogate keys exist only on dimensions, which are conformed and joined.

## 3. Dimensions

| dimension | grain | key | rollup handling |
|---|---|---|---|
| `dim_date` | one calendar month | `date_month` (`YYYY-MM-01`) | n/a — no rollup concept |
| `dim_geography` | one reporting geography | `geography_sk` (surrogate) | `is_rollup` — 14 accepted strings: 8 non-Pacific division codes, `US`, and 5 Pacific spellings |
| `dim_sector` | one EIA sector | `sector_sk` (surrogate) | `is_rollup` — FALSE for leaf codes 1–8; TRUE for 90/94/96–99; 95 remains UNKNOWN |
| `dim_energy_source` | one energy source | `energy_source_sk` (surrogate) | `is_aggregate` — 19 non-additive source totals/overlapping alternatives; 26 additive components |

No plant dimension: the API does not expose plant, and the fact grain does not need one.

### Referential integrity boundary

The tracked schema enforces primary/business keys but defines **no physical foreign
keys** from facts to dimensions or audit. The loader upserts dimensions and facts
inside one publication transaction and checks accepted run/hash provenance.
Independent pilot checks found zero unmatched dimension/audit rows, zero flag
mismatches and zero duplicate fact or mart keys.

These logical checks support the verified application loading path. They are not
database-enforced protection against arbitrary direct SQL writes; maintenance
users remain trusted. Physical foreign keys and broader enforcement would require
a separately reviewed schema change, not a claim that such constraints exist today.
No annual dimension: the fact is monthly.

## 4. The rollup contract

This is the single most consequential decision in the model.

The source mixes **leaf values and rollup values inside the same facets**:

- `location` — the classifier accepts exactly 52 leaf codes (50 states + DC + PR) and
  14 rollup strings: `NEW MAT SAT ESC WSC ENC WNC MTN`, `US`, and `PAC PCC PCN PACC PACN`.
  The latter two are legacy spellings, wrongly attributed to the bulk file in older
  comments. These 66 accepted strings are not a verified current API facet list;
  the historical facet count was 64 (see [source-data.md](source-data.md#pacific-geography-vocabulary--historical-report-and-current-classifier)).
  Every other string is `UNKNOWN`.
- `fueltypeid` — 26 additive components and 19 non-additive totals/overlapping
  alternatives cover all 45 captured API fuel members. `MLG` is a rollup of
  `LFG + MSB`, not an additional leaf. `OB2` and `BIS` retain separate source
  identities but are excluded from the canonical additive basis. See the
  [captured classification evidence](../data/fixtures/eia/classification_evidence.json).
- `sectorid` — `1–8` are leaf components. `90, 94, 96, 97, 98, 99` are
  overlapping totals. `95` remains `UNKNOWN`: its "Coal Consumption" label does
  not establish the meaning of its observed generation values.

A naive `SUM(generation) GROUP BY period` is therefore wrong **twice over**: roughly 3×
from double-counting leaf geographies with their divisions and `US`, and again from
double-counting base sources with `ALL` and `AOR`. Both mistakes produce plausible-looking
numbers, which is what makes them dangerous.

**The contract:**

1. Every source row is preserved. Nothing is dropped for being a rollup — the analyst may
   legitimately want `US`, `ALL`, or a census division directly.
2. Rollup-ness is a **dimension attribute**, not a row filter at ingest.
3. **Leaf-side aggregate models filter to positively classified leaf members** and
   are tested for it. The current monthly mart is utility-scale and explicitly
   selects sectors `1–7` and excludes `DPV` (estimated small-scale solar).
   Residential/DPV remain valid leaves in core/staging; the reconciliation mart
   retains utility-scale source totals.
4. `US` + all sectors + `ALL` is the designated **reconciliation total**: a test asserts
   that summing the base side reproduces it, within a tolerance. This converts the
   #1 critical risk from "hope it's right" into "a test that fails loudly."
5. FALSE rollup flags alone are not proof of leaf membership. Source code 95
   remains UNKNOWN and blocks the whole run before core writes. The full API
   facet labels are now captured in the public evidence fixture. Historical
   Phase 0 statements about seven sectors and unavailable facet evidence record
   the earlier audit, not the current classification.

### 4a. Three-state classification: leaf, rollup, unknown

A code the current dimension seed does not recognise is **not** a leaf code. It is neither
a leaf nor a rollup — it is `unknown`, and that is a third answer with its own rules.

The failure this prevents is specific and invisible. If an unrecognised `fueltypeid`
defaults to "base", then the day EIA introduces a new aggregate code it is silently summed
into every national total as though it were a fuel. Nothing errors. No row-level test fails.
The numbers are simply too large, and they stay too large until someone reconciles against
the source by hand.

The contract, applied uniformly to all three joinable dimensions:

| stage | unknown code |
|---|---|
| **Raw ingestion** | **allowed.** The row is preserved verbatim, as everything else is. Losing real source data over a missing dimension member is the worse failure. |
| **Staging / classification** | recorded as `UNKNOWN`, visible in the validation report and therefore in the audit record, with a WARN finding naming the codes. |
| **Aggregate / mart publish** | **HARD FAILURE.** An unclassified code may not contribute to any total. `aggregate_publish_gate()` returns a blocking finding listing the offending codes. |

The separation matters: ingestion asks *"can we trust this row enough to keep it?"*, and
for an unknown code the answer is yes. Publishing an aggregate asks *"may this row
contribute to a total?"*, and for an unknown code the answer must be no. A single gate
could not answer both.

`is_leaf` is therefore *not* the negation of `is_rollup`. An unclassified row is not a leaf
row, and is not counted as one — which is what keeps it out of the base side of every
aggregate.

**Resolution.** Classification requires inspectable source evidence, including numerical
checks where aggregation relationships are unresolved. Facet labels alone do not prove
leaf/rollup membership. The classified sets are seeds, not exhaustive API vocabularies.
A new code is assumed to be **neither** base nor aggregate; neither its label nor its
spelling authorizes publication.

## 5. Measures

Only what the source publishes:

| column | type | notes |
|---|---|---|
| `generation_thousand_mwh` | `double precision` | the source value, verbatim; nullable |
| `generation_mwh` | `double precision` | same value × 1000, for unit-free downstream use |
| `is_rollup_geography` | `BOOL` | denormalised from the dim, so the guard travels with the row |
| `is_rollup_sector` | `BOOL` | " |
| `is_rollup_energy_source` | `BOOL` | " |

No cost, capacity, price, fuel-quantity or MMBtu measure — none is on the verified field
list for this route. No invented measures.

**Nullable, and it stays nullable.** `null` means *no data*; `0` means *measured zero*.
These are different facts and staging must not `COALESCE(..., 0)` them. 23.9% of observed
cells are genuine zeros, so collapsing nulls to zero would silently fabricate a quarter of
the dataset.

**Negatives are valid.** 6.15% of monthly cells are negative (3,269 / 53,176), because
pumped storage and CHP plants consume more than they generate. **A `generation >= 0` test is
forbidden.** Values are preserved as published.

## 6. Sparsity

The source grid is sparse by design, not a full cross-product:

```
July 2026:  1,582 populated cells  of  51 x 7 x 18 = 6,426  →  24.6% coverage
Puerto Rico: 3 sectors only (1, 2, 7), 8 fuel codes
```

So completeness is asserted against the **observed key set** (vs. the previous run and vs.
the source's own record count), never against a Cartesian product. Any `PR` completeness
test must be scoped to its 3 sectors or it will fail permanently — a real characteristic
of the source, not a transient gap.

## 7. Storage and revision behaviour

- **Warehouse = current accepted state.** A newer trusted observation may replace a
  value only after immutable run-identity and revision-precedence checks. Older or
  ambiguous replacements fail. No SCD2 history table is used in the analytics layer.
- **Raw zone = full history.** Every vintage is retained as immutable objects, so
  "what were we told, and when" is always answerable and the warehouse is rebuildable from
  raw at any time.
- That split is the whole design: analytics want the best current number, audit wants the
  vintage trail.

### 7a. Raw identity: `run_id` versus `content_hash`

Raw identity carries **two** fields, because "what did we receive" and "when did we receive
it" are different questions and one identifier cannot answer both.

| field | meaning | derived from | changes when |
|---|---|---|---|
| `run_id` | **execution identity** — which run wrote this | nothing; a UUID4 | every extraction run |
| `content_hash` | **content identity** — what the source actually returned | the checksum of every page, in order | the bytes change |

Object layout:

```
raw/eia/electric_power_operational_data/ingestion_date=YYYY-MM-DD/run_id=<run_id>/page_0001.json
```

Objects are keyed on `run_id`, **not** on `content_hash`. That is the point: if the path
were content-addressed, a re-extraction returning byte-identical data would resolve to the
same path and **overwrite the earlier extraction's raw object**. The pipeline would then be
able to answer "what did we receive at some point?" but not "what did we receive, and when?"
— and the second question is the entire reason the raw zone keeps history.

`run_id` is deliberately **not derived from anything** — not from the content, not from a
timestamp. A run is an event, and two executions that observed identical bytes are still
two events; an id computed from what it observed would collapse them into one. The cost is
that a run cannot be re-derived by recomputation: re-running an extraction produces a new
`run_id`, which is the correct behaviour rather than a limitation.

Deduplicating identical content is still available, but it belongs *after* the write, not
in place of it: later runs may record a reference to an existing `content_hash` rather than
copy the bytes, and the audit row is kept either way. Collapsing happens in the audit layer,
never at the point where history is written.

An unchanged same-run retry is idempotent. A newer observation advances accepted fact
provenance even when its values and `content_hash` are unchanged, preventing an
intervening stale replay from replacing it. Equal-time distinct runs with identical
content and per-key values retain existing fact provenance. Content hashes and UUIDs
do not establish freshness: `extracted_at` orders local observations, not authoritative
EIA revisions. Fixture and live observations cannot supersede one another.

### 7b. Where the raw zone physically lives

The raw zone is **local-first**. A run writes to a filesystem under `RAW_ROOT`, and
each run namespace gets a `manifest.json`:

```
data/raw/                                    <- RAW_ROOT, default
  raw/eia/electric_power_operational_data/   <- logical key prefix, backend-independent
    ingestion_date=YYYY-MM-DD/
      run_id=6f1c...-...-.../               <- UUID4, one per execution
        page_0001.json
        page_0002.json
        manifest.json
```

**The key is a logical object key.** `raw/eia/.../page_0001.json` is not a path:
the store resolves it under `RAW_ROOT`. Nothing upstream of the store --
extraction, parsing, validation -- knows where a key lands on disk.

Four rules hold, and each is enforced in code rather than by convention:

| rule | how | enforced by |
|---|---|---|
| different executions never share a namespace | `run_id` is a UUID4 drawn per execution | `RawBatch.namespace` |
| pages of one run share one namespace | every path is derived from `RawBatch.namespace` | `RawBatch.objects()` |
| raw objects are never overwritten | every write is create-only | `O_CREAT\|O_EXCL` |
| the manifest belongs to the run | written to the same namespace, create-only | `RawSink.write()` |

**Unique naming is not the guarantee; the precondition is.** A UUID4 makes a collision
astronomically unlikely, but "unlikely" is not a property a raw zone can rest on -- a
replayed job, a bug, or a hand-typed key can still aim at a live object. So every write
is create-only, and a collision fails loudly with the existing object left byte-for-byte
untouched. On a filesystem that is `os.open(..., O_CREAT | O_EXCL)` -- an atomic
open-or-fail at the OS level. The obvious `if not path.exists(): write(...)` has a race
between the check and the write, and a race in an append-only raw zone is exactly the
failure this contract exists to prevent. We never delete-and-replace: a delete would
itself be a mutation of history.

The one exception is a write that fails *after* the file was created. That object is
incomplete debris rather than history, so it is removed -- otherwise the retry would
collide with it and a recoverable disk error would become a permanently corrupt object.
"This object exists" must keep meaning "this object is complete", which is
why the write is atomic rather than check-then-write.

**The manifest is the completion marker.** Pages are written first, manifest last, and the
manifest is create-only like everything else. So a run that died half-way leaves pages with
no manifest -- visible to anyone listing the raw directory, with no database query. A run
that returned no rows is *also* successful and is distinguishable by its status:
`succeeded` versus `empty`.

**Raw objects hold the bytes we were given.** Numeric strings stay strings, `null` stays
`null`, negatives stay negative, rollup rows and rows with unclassified codes stay in.
Nothing is filtered, transformed, or reordered on the way to disk. Parsing is the derived
layer's job, and a parser bug must not be able to reach back and corrupt the record of
what the source said.

**Authentication: none.** A local run needs no credentials, no cloud account and
no billing. `.gitignore` refuses service-account JSON regardless, so a key file
cannot enter the repository by accident.

### 7c. Verification status -- read this before trusting the raw zone

Two kinds of test, and they are **not** interchangeable.

| | what it proves | what it cannot prove |
|---|---|---|
| **Unit tests** (fake store) | our logic: naming, create-only, ordering, manifest shape, partial failure, payload fidelity | that any real filesystem enforces the contract |
| **Local integration tests** (real filesystem) | that `LocalObjectStore` -- the store every default run uses -- actually holds create-only, byte-exact and manifest-last | durability beyond the disk |

**Historical verification record.** The table below retains the earlier 172-test
raw-storage milestone; it is not a fresh full-project result. See
[portfolio.md](portfolio.md) for the later recorded offline and PostgreSQL results.

| gate | status |
|---|---|
| Full default suite (`pytest`) | PASS -- 172 passed |
| `ruff check` / `ruff format --check` | PASS -- clean |
| Run namespaces use a unique `run_id` | PASS |
| `content_hash` deterministic, `run_id` not | PASS |
| Create-only, real filesystem | PASS -- `tests/integration/test_local_raw_storage.py` |
| Byte-for-byte preservation after a real collision | PASS locally |
| Manifest written last; an incomplete run is detectable on disk | PASS locally |
| Partial failure leaves earlier pages and no manifest | PASS locally |

So the raw zone is integration-verified against the store every default run uses.
The production sink has no delete path and must not gain one -- raw history is
append-only, and cleanup in the write path is how history gets lost.

## 8. Implemented dbt layer and remaining work

The conformed-dimension columns and dbt layout are implemented. `stg_generation`
and `int_generation_leaf` are views; `mart_monthly_generation` and
`mart_reconciliation_total` are tables. Source declarations reference the five
core tables and `audit.ingestion_run`. Schema tests and the leaf-only and
reconciliation SQL tests are present under `dbt/`.

The recorded synthetic-fixture demonstration built all four models and passed
12 applicable tests, excluding national reconciliation because the fixture is
incomplete. Full-source reconciliation, remaining dimension-code curation, the
source revision horizon and source-withdrawal policy still need evidence or an
explicit decision. No additional analytical models or SCD2 behavior are claimed.
