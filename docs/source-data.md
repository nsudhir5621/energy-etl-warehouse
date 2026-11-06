# Phase 0 — Source and Data-Model Discovery

Status: **Phase 0 discovery complete and re-audited against the local-first architecture.**
Ingestion (Phase 1) and the raw zone (Phase 2) are built on top of it; see
[data-model.md](data-model.md) for the model as implemented. Items still marked ⚠️ are
genuinely unverified — the reasons are recorded in §8 and §13 rather than guessed around.

The ✅ markers below record historical verification claims. For sector facets, the
reported live findings are distinguished from independently inspectable artifacts in §5.
The markers describe the historical investigation; the B4.1 update below records
which classifications now have captured evidence and which remain unresolved.


## B4.1 live classification evidence — 5 November 2026

A bounded authenticated scan reproduced **17,423 July 2026 records**, four pages,
17,423 distinct business keys and no hard parsing failures. The API's latest
monthly period was **2026-08** at observation time. Complete facet responses had
64 locations, 15 sectors and 45 fuels. Public labels and selected observations
are captured in [classification_evidence.json](../data/fixtures/eia/classification_evidence.json);
the fixture records observation time and original batch content hash, with no
credentials or authenticated request URLs.

Before correction, 12,676 records contained unknown classifications: all eight
additional sector codes and 18 fuel codes were present. After correction,
**17,301 records have established classifications; 122 sector-95 records remain
unresolved**. The complete batch remains blocked from core publication. These
are classification counts, not claims of database loading or a full live pipeline.

The additive fuel basis is now **26 components**:
`ANT BIT DFO DPV GEO HPS HYC LFG LIG MSB NG NUC OOG ORW OTH PC RC RFO SPV STH SUB WNS WNT WOC WOO WWW`.

The **19 non-additive representations** are:
`ALL AOR BIO BIS COL COW FOS MLG NGO OB2 OBW PEL PET REN SUN TPV TSN WAS WND`.
The rollup flag denotes exclusion from the additive basis, including overlapping
alternative representations; it does not rename or collapse their source keys.
`MLG` was incorrectly retained as a leaf: the source demonstrates
`MLG = LFG + MSB`. Further complete-cell checks establish `WAS = MLG + ORW`,
`OBW = MSB + ORW`, `BIO = WWW + WAS`, and the coal, petroleum, wind and solar
component sums stored in the fixture. `OB2/ORW` and `BIS/BIT` have matching
observations in the captured July 2026 and July 2019 slices; the canonical basis
uses `ORW` and `BIT`, retaining the alternatives as non-additive source rows.
These observations do not assert that aliases will remain numerically identical
through all future source revisions.

Sectors `1–8` are disjoint components; `8` is Residential. Sector rollups are
`90, 94, 96, 97, 98, 99`: complete observations confirm `90 = 1 + 2`,
`94 = 2 + 3`, `96 = 4 + 5`, `97 = 6 + 7`, and `98 = 1 + 2 + 3`.
For utility-scale `ALL`, `99 = 1 + ... + 7`.

**Sector 95 remains UNKNOWN.** Its actual facet label is "Coal Consumption",
but its 122 July observations include positive gas, solar and other generation.
It is absent from the browser's generation-sector hierarchy. Neither a label
guess nor those observations prove a disjoint leaf or aggregate membership.
An EIA clarification or an explicitly designed, auditable quarantine policy is
needed; no rows are silently removed to obtain publication success.

**Coverage:** `DPV` is valid estimated small-scale solar, and Residential is a
valid source sector. They remain available at the core/staging grain when
publication is otherwise eligible. The existing analytical mart is explicitly
**utility-scale**, matching the source `ALL` reconciliation scope:
`int_generation_leaf` selects sectors `1–7` and excludes `DPV`; it does not
misclassify Residential or DPV as rollups.
This is an analytical scope filter, not an ingestion/raw-data filter.

Nulls are not zeros. Numerical witnesses compare only present, non-null source
components, within their documented rounding tolerance. Missing or null
components do not establish numerical closure. July's aggregate across all
leaf geographies also includes Puerto Rico, while the U.S. total does not;
the observed national reconciliation gap remains an open completeness/scope
question. Finite utility-scale leaf observations sum to 452,795,482.55 MWh
including PR, versus 451,966,701.13 MWh in the U.S. source total. Excluding
PR leaves a -778,755.09 MWh gap and 247 null leaf observations; neither
missingness nor geography alone has been established as the full cause.
The dbt reconciliation threshold is unchanged.

Changing a classification can change stored flags even when source content is
identical. Existing published runs must not be silently restaged or reclassified
under the same immutable provenance. No historical database was changed.

### B4.1 resumption — 6 November 2026

After the workstation restart, the protected credential loader and authenticated
API access were checked again. A single bounded July request for `US / 95`
returned 16 rows, identical to the earlier captured slice. The evidence fixture
now includes positive `NG` (389.70677) and `SPV` (107.16746) observations, in
thousand MWh, still labelled "Coal Consumption" by the source. Both remain
UNKNOWN in the sector dimension and fail the independent publication gate.
No additional sector classification is justified by this evidence. A bounded
four-page full-July recheck also reproduced all 17,423 business keys and the
original content hash: 17,301 classified records and 122 unresolved sector-95
records. The entire batch remained blocked; no raw-zone or warehouse writes
were performed.

The retained full-July capture also permits a more precise reconciliation
diagnostic. These are sums of finite observations, never imputed null values:

| Source slice | Rows | Null observations | Finite subtotal (MWh) |
|---|---:|---:|---:|
| U.S. `ALL`, sectors 1–7 | 7 | 0 | 451,966,701.13 |
| State/DC `ALL`, sectors 1–7, excluding PR | 302 | 43 | 451,846,844.41 |
| U.S. utility-scale fuel leaves, sectors 1–7 | 126 | 12 | 451,822,572.52 |
| State/DC utility-scale fuel leaves, excluding PR | 1,682 | 247 | 451,187,946.04 |

The state/DC detailed subtotal is 778,755.09 MWh below the U.S. source total.
Even the state/DC `ALL` subtotal is 119,856.72 MWh below it. These residuals
identify incomplete numerical evidence; they do not assign values to null or
absent cells, prove a particular cause, or authorize relaxed reconciliation.
PR coverage must also be aligned explicitly when comparing a geographic sum to
`US`. Neither reconciliation SQL nor its tolerance was changed in this resumption.

The sections below retain the earlier Phase 0 audit as a historical record.
Their old vocabulary counts, verification markers and unresolved-facet statements
describe that investigation; the B4.1 contract above is the current classification.

---

## 0a. Re-verification (local-first re-audit)

Every quantitative claim above was re-derived from scratch against a fresh download of
`f923_2026.zip`, with no API key, by `scripts/verify_phase0.py`. The script is checked
in and re-runnable:

```bash
curl -sSL -o f923_2026.zip https://www.eia.gov/electricity/data/eia923/xls/f923_2026.zip
python scripts/verify_phase0.py f923_2026.zip
```

| claim | documented | measured | |
|---|---|---|---|
| AK, 2026-07, sector 1, all fuels | 594,571.04 MWh | 594,571.04 MWh | ✅ exact |
| fact cells (month × state × sector × fuel) | 11,109 | 11,109 | ✅ exact |
| plant-level NETGEN cells | 53,176 | 53,176 | ✅ exact |
| zero rate | 23.9% | 23.87% (12,694 / 53,176) | ✅ |
| negative rate | 6.15% | 6.15% (3,269 / 53,176) | ✅ exact |
| July 2026 sparsity | 1,582 of 51×7×18 = 24.6% | 1,582 of 6,426 = 24.6% | ✅ exact |
| `sectorid` vocabulary (bulk file) | exactly 1–7, no rollup | exactly 1–7 | ✅ |
| `sectorid` vocabulary (live API facet) | 15 members: 1–7 + 8, 90, 94–99 | 15 members historically reported; captured facet response unavailable | ⚠️ provenance unresolved |
| MER fuel vocabulary (base, after live reclassification) | 15 leaf + 12 aggregate | SUN/WND/COL moved to aggregate | ✅ live |
| YTD column = sum of monthly | 7,991 / 7,991, 0 mismatches | 7,991 / 7,991, 0 mismatches | ✅ exact |
| fact cells backed by >1 plant row | 5,896 (and 5,213 by exactly 1) | 6,501 (and 4,608 by exactly 1) | ⚠️ see below |
| distinct (state, sector, fuel) combos | 1,598 | 1,598 | ✅ exact |
| fact cells by month (Jan–Jul) | 1,588/1,587/1,588/1,587/1,588/1,589/1,582 | identical, all 7 months | ✅ exact |
| PR `sectorid` members | `1`, `2`, `7` | `['1', '2', '7']` | ✅ exact |
| PR `fueltypeid` members | 8 of the 18 | 8: `COL DFO HYC NG OTH RFO SUN WND` | ✅ exact |
| no-data sentinel spellings | `.` | `.` only (42,716 cells) | ✅ |
| geography members in the file's state column | not previously measured | 52 leaf — no divisions, no `US` | ⚠️ see below |

**One figure does not reproduce as stated, and it is a definition rather than an error.**
The documented 5,896 / 5,213 split is out of the same 11,109 total as what was measured,
so the *grain* claim holds; what differs is whether a plant row carrying EIA's `.`
no-data marker counts as "backing" a cell. Counting only populated plant rows gives
6,501 / 4,608. The claim that matters — that the source is a strict many-to-one map onto
the 4-tuple, with a well-defined sum for all 11,109 cells — reproduces either way. The
documented split is left in place rather than overwritten, with this note beside it.

**A second finding: the file carries no division geographies at all.** The
`Plant State` column of every generation sheet holds exactly the **52 leaf
geographies** (50 states + `DC` + `PR`). No census division code, no `US`,
and no `PACC`/`PACN` appears anywhere in it. The historically reported
64-member `location` facet in §4/§5 therefore concerns the **API**, not the
bulk file; that count does not prove a complete saved facet list. This also means
the §5 table's old "Form EIA-923 bulk file: `PACC`, `PACN`" row was
misattributed — the file cannot be the source of those spellings, because it
contains no division codes. What the API facet itself carries is measured
live by `scripts/verify_api_contract.py` (§6a).

**A third finding: the production client's sort parameters were
rejected by the live API.** `ApiSource` sent flat
`sort[column]=period&sort[direction]=desc`; the API answers
`HTTP 400 — "Invalid sort index 'column' provided. Must be an
integer… Example: 'sort[0][column]=period'"`. Every live page
request would have failed. No test caught it because every test
runs through `FixtureSource`, which never validates parameters —
the fixture path and the API path share pagination logic but not
parameter semantics. Fixed on 2026-10-07 to the indexed form the
API requires (`sort[0][column]`, `sort[0][direction]`), and the
parameter-shape test now pins the live-confirmed format. Found by
`scripts/verify_api_contract.py`'s first live probe; confirmed
against the API's own error body.

### One new source finding

**EIA's bulk XLSX uses `.` (a single period) as its no-data marker, where the API returns
`null`.** Any parser reading this file must treat `.` as absent: `float(".")` raises, and
a parser that does not would turn a missing value into a failure or, worse, a zero. The
API path is unaffected — `records.py` already treats `None` and `""` as null — but a
future CSV/XLSX ingestion path must carry this rule from day one. The verifier encodes it
as `NULLISH`.

---

## 1. Source endpoint

✅ `GET /v2/electricity/electric-power-operational-data/data/`

Verbatim route description returned by the API:

> "Monthly and annual electric power operations by state, sector, and energy source.
> Source: Form EIA-923"

Why this route:

- It is the only route that exposes **generation by energy source** with state and
  sector breakdown. Sibling routes cover only totals (`electric-power-operational-data`
  is the only one with a `fueltypeid` facet; `electric-power-enterprise-summary` has no
  energy-source dimension).
- It serves both monthly and annual, so the same key works for either.
- It is backed by a form (EIA-923) with a documented, stable code list, which makes the
  dimensions defensible rather than guessed.

---

## 2. Frequency

✅ **Monthly is the canonical project grain, and the only frequency this project loads.**

`frequency=monthly` verified working. The route description states monthly *and* annual
are both offered; ⚠️ the annual variant's field set was not verified (rate limit, §9).

**Decision: load monthly only.**

The reasoning, stated accurately:

1. **Monthly is the canonical project grain.** It is the finest granularity EIA publishes
   on this route for generation by energy source, and it is the grain every fact, key and
   incremental strategy in this project is defined against.
2. **Annual is derivable downstream** from monthly facts, by summing. No mart needs a
   second load path to answer an annual question.
3. **Loading both into one fact table would double-count.** Annual and monthly rows would
   share the same conceptual subject — a place, a sector, a fuel, a year — and summing
   them together would count every megawatt-hour twice. They are two *representations*,
   not two facts, and the model must hold exactly one.
4. **They are not a simple duplicate, and this is the part that matters.** The separately
   published EIA annual dataset is built from the same underlying form, but its
   **coverage and finality differ from a sum of our monthly project data**:
   - EIA's annual publication reflects **year-end close**, when respondents have
     completed annual reconciliation. The monthly figures for the same year carry a
     different revision state at every point in time.
   - Coverage can differ. The API reports `null` for combinations that do not apply;
     how many months of a partially-reported year are populated is a property of *when*
     the request was made.
   - Therefore an annual figure derived by summing our monthly facts is **a valid
     derived metric of our own data, and is not guaranteed to equal the EIA-published
     annual figure.** Where the difference matters, both vintages are preserved in the
     immutable raw zone, so the comparison is always possible — that is precisely the
     reason the raw zone keeps every vintage rather than only the latest.

**A derived-annual mart is therefore labelled as derived**, and does not claim to be the
EIA annual series. It is not built unless a consumer needs it; the dashboard's trend view
is monthly with a user-selected rollup.

The implemented Airflow DAG runs monthly; scheduling cadence is separate from source cadence.

---

## 3. Available date range

- ✅ Latest available monthly period as of 2026-09-30: **`2026-07`**.
- ✅ The source lags roughly **two months** behind the current month.
- ⚠️ Earliest available period: not verified (rate limit). Form EIA-923 monthly data
  begins **2001-01** per EIA's own archive index, so the practical floor is 2001-01,
  but the API may expose a different range.
- ✅ Total records available at `frequency=monthly&data[0]=generation`: **4,771,692**.

Derived: if history runs 2001-01 → 2026-07 (307 months), that is ~15,500 records/month
and a full-history extract is **955 pages** at the 5,000-row cap. The project will
therefore default to a bounded, configurable start period rather than full history.

---

## 4. Actual fields

✅ Exact JSON returned for a monthly `generation` query:

| field | type | meaning |
|---|---|---|
| `period` | string `YYYY-MM` | month of observation |
| `location` | string | geography code (see §5 note) |
| `stateDescription` | string | geography name |
| `sectorid` | string | EIA sector (bulk file: 1–7; API facet: 1–7 + 8, 90, 94–99) |
| `sectorDescription` | string | sector name |
| `fueltypeid` | string | energy source code (see §5 note) |
| `fuelTypeDescription` | string | energy source name |
| `generation` | string (numeric or `null`) | net electricity generation |
| `generation-units` | string | always `"thousand megawatthours"` |

⚠️ Other `data[n]` measures on this route (e.g. fuel consumption) were not enumerated —
rate limited. The project will start with `generation` only, which is what the fact needs.

> **Parsing note:** the API returns numbers as **JSON strings**, not numbers.
> `requests.json()` gives `{"generation": "594.57104"}`. This must be cast explicitly.
> Silent pass-through would load everything as STRING and break every downstream aggregate.

---

## 5. Actual grain

✅ **One row represents total net electricity generation (thousand MWh) for one
reporting location, in one sector, from one energy source, in one month.**

Derivation (✅, from the file): the source file is plant-level
(7,991 plant × prime-mover × fuel-type rows for 2026 alone). The API aggregates that
plant-level detail up to (month, state, sector, MER fuel type). The reconciliation in §0
proves the aggregation reproduces exactly.

### ⚠️ The single most important Phase 0 finding

**Both the `location` facet and the `fueltypeid` facet mix leaf values with rollup values.**

The original Phase 0 investigation **reported 64 API `location` members**.
That historical count is not an independently inspectable complete current facet list.
The current classifier accepts 52 explicit leaves (50 states + `DC` + `PR`) and
14 rollup strings, including retained legacy Pacific spellings (see §5).
The bulk file carries only the 52 leaf geographies (§0a).

`fueltypeid` mixes ✅ (verified live against 2026-07 at US/All-Sectors):

**Reclassified live (2026-10-09):** `SUN`, `WND`, `COL` were originally classified as
leaf (base MER codes from the bulk file). The live verification proved they are **aggregates**:

| code | was | now | proof (US/All-Sectors/2026-07) |
|---|---|---|---|
| `SUN` | leaf | **aggregate** | SUN = SPV + STH (41,411.78 = 41,096.84 + 314.94) |
| `WND` | leaf | **aggregate** | WND = WNS + WNT (32,183.93 = 159.55 + 32,024.38) |
| `COL` | leaf | **aggregate** | COL = COW − WOC (75,062.39 = 75,784.96 − 722.57) |

Current seed after reclassification:
- **Leaf (15):** `DFO, GEO, HPS, HYC, MLG, NG, NUC, OOG, ORW, OTH, PC, RFO, WOC, WOO, WWW`
- **Aggregate (12):** `ALL, AOR, SUN, WND, COL, COW, TSN, TPV, FOS, REN, NGO, PET`
- **Unknown (18):** `ANT, BIO, BIS, BIT, DPV, LFG, LIG, MSB, OB2, OBW, PEL, RC, SPV, STH, SUB, WAS, WNS, WNT`

Codes with aggregate names but **not numerically verified** — they remain `UNKNOWN`
(accepted at ingestion, blocked at publish) until a numerical check confirms their
hierarchy. `records.py` now reflects all 12 aggregate codes; the 3 verified
reclassifications were `SUN`, `WND`, `COL`.

**Consequence:** a naive `SELECT SUM(generation) GROUP BY period` is wrong by roughly
3× (leaf + division + US) and is wrong again inside each location (leaf sources + ALL +
AOR). This is the trap that most candidates fall into.

**Handling:** keep the source's own rows intact, and mark rollups as an
attribute of the dimension rather than dropping them.

- `dim_geography.is_rollup` — TRUE for census divisions and `US`
- `dim_energy_source.is_aggregate` — TRUE for `ALL`, `AOR`, and the other aggregate codes
- Leaf-side aggregations filter to positively classified leaves; the reconciliation
  mart separately retains the designated source totals. FALSE rollup flags alone
  do not prove leaf membership, and UNKNOWN codes block core publication.
- `US` + sector + `ALL` is the designated cross-check total for reconciliation

This way the project does not need the complete aggregate code list in advance to be
correct — but the seed **will** be curated once a registered key is available, and the
rollup totals will be used to assert the base-side sum reconciles.

### Energy-source codes (✅ live reclassification applied)

**Originally 18 base MER codes** from EIA's file layout. After the 2026-10-09 live
verification, `SUN`, `WND`, and `COL` were reclassified as **aggregates** (proven
numerically), leaving 15 base leaf codes:

**Leaf (15):** `DFO` Distillate Petroleum · `GEO` Geothermal ·
`HPS` Hydroelectric Pumped Storage · `HYC` Hydroelectric Conventional ·
`MLG` Biogenic MSW and Landfill Gas · `NG` Natural Gas · `NUC` Nuclear ·
`OOG` Other Gases · `ORW` Other Renewables · `OTH` Other · `PC` Petroleum Coke ·
`RFO` Residual Petroleum · `WOC` Waste Coal · `WOO` Waste Oil · `WWW` Wood and Wood Waste

**Reclassified aggregate (3):** `SUN` Solar (aggregate of `SPV + STH`) ·
`WND` Wind (aggregate of `WNS + WNT`) · `COL` Coal (aggregate of `COW − WOC`)

**Additional aggregates from live facet (9):** `ALL` All fuels · `AOR` All renewables ·
`COW` All coal products · `TSN` Total solar · `TPV` Total solar PV ·
`FOS` Fossil fuels · `REN` Renewable · `NGO` Natural gas & other gases ·
`PET` Petroleum

### 7 sectors in the observed bulk-file vocabulary

`1` Electric Utility · `2` NAICS-22 Non-Cogen · `3` NAICS-22 Cogen ·
`4` Commercial NAICS Non-Cogen · `5` Commercial NAICS Cogen ·
`6` Industrial NAICS Non-Cogen · `7` Industrial NAICS Cogen

### `sectorid` — bulk-file vocabulary, reported API facet, and classification

**Historical bulk-file finding:** the observed `sectorid` vocabulary contains only `1–7`,
which the current seed classifies as leaf.
**Historical live-source report:** `/facet/sectorid` returned 15 members; no independently
inspectable captured sector-facet response was found in the repository.

The bulk file's full sector dictionary was enumerated across every Page 1 sheet, and it
contains **exactly** these 7 codes — 1 through 7, no gaps, no extras:

| sheet | geographies | distinct sectors | distinct fuel codes |
|---|---|---|---|
| Page 1 Generation and Fuel Data | 51 (50 states + DC) | 7 (`1`–`7`) | 18 |
| Page 1 Puerto Rico | 1 (`PR`) | **3** (`1`, `2`, `7`) | 8 |
| Page 1 Energy Storage | (subset of Page 1) | 7 | — |

The observed bulk-file vocabulary has no "all sectors" member. Its seven categories are
reported as EIA's "internal consolidated NAICS sectors" and form the current leaf seed.
This bulk-file observation does not establish the complete API vocabulary or the
aggregation relationships of additional API codes.

**So the rollup treatment is dimension-specific, not blanket:**

| dimension | has rollups? | flag |
|---|---|---|
| `dim_geography` | Current classifier: 14 rollup strings — 8 non-Pacific division codes, `US`, and 5 Pacific spellings | `is_rollup` |
| `dim_energy_source` | ✅ yes — `ALL`, `AOR`, and other aggregate codes | `is_aggregate` |
| `dim_sector` | Current seed: 7 leaf codes (`1–7`); 8 additional reported API codes remain `UNKNOWN` | `is_rollup` retained; FALSE for the 7 classified codes |

`dim_sector.is_rollup` is retained for the seven currently classified leaf codes.
A FALSE rollup flag alone does not establish leaf membership for an unknown code;
`aggregate_publish_gate` must continue to block unknown codes from publication.

⚠️ **Historical live-source report (2026-10-09):** `/facet/sectorid` returned **15
members**: `1, 2, 3, 4, 5, 6, 7, 8, 90, 94, 95, 96, 97, 98, 99`. The observed bulk-file
vocabulary contains only `1–7`. The following labels for the eight additional codes come
from an earlier reported live-source investigation:

| Code | Previously reported label |
|---|---|
| `8` | Residential |
| `90` | Electric Power Sector Non-CHP |
| `94` | Independent Power Producers |
| `95` | Coal Consumption |
| `96` | All Commercial |
| `97` | All Industrial |
| `98` | Electric Power |
| `99` | All Sectors |

**These labels are not independently verified.** No independently inspectable captured
sector-facet response was found in the repository. Earlier Git revisions (`4201a45` and
`e26fa2a`) contain conflicting mappings for some codes; neither those assertions nor the
reported labels above establish definitive API labels. `scripts/verify_api_contract.py`
can retrieve and print facet labels, but its implementation is not a captured result.

**All eight codes remain `UNKNOWN`.** Their leaf/rollup membership, overlap with `1–7`,
and numerical aggregation relationships have not been established. Labels alone do not
prove classification. These codes are accepted at ingestion (WARN) and blocked at
aggregate publication by `aggregate_publish_gate` pending evidence and curation.

The historical business-key verification for 2026-07 reported only sectors `1–7` in the
observed data. That finding does not establish classification for the additional codes.

### Pacific geography vocabulary — historical report and current classifier

A live facet read on 2026-10-09 **reported 64 `location` members** and the Pacific
spellings `PAC`, `PCC`, `PCN`. A count alone does not establish a complete saved
facet list or independently verify the current API vocabulary.

The classifier's explicit membership is a separate implementation contract:

| accepted code group | classification | evidence / limitation |
|---|---|---|
| 50 conventional state postal codes + `DC` + `PR` (52) | LEAF | Historical bulk-file evidence (§0a); not a saved complete current facet list |
| `NEW MAT SAT ESC WSC ENC WNC MTN` (8) and `US` | ROLLUP | Historical API/project evidence |
| `PAC PCC PCN` (3) | ROLLUP | Historically reported API spellings |
| `PACC PACN` (2) | ROLLUP | Retained legacy spellings; their old bulk-file attribution was incorrect |
| Every other string | UNKNOWN | No format-based inference; publication blocked pending evidence |

This is **52 leaves + 14 rollup strings = 66 accepted strings**, not a claim of
66 current API facet members. The compatibility set includes two legacy spellings;
this numerical difference does not independently establish the historical facet list.
The bulk file contains no division codes.
All five Pacific spellings are excluded from leaf-side aggregations; no mapping or
numerical relationship between those spellings is inferred. Their source coverage
and dimension labeling still require evidence.

### Sparsity (✅, and it constrains the completeness tests)

The source grid is **deliberately sparse**, not a full cross-product:

```
July 2026:  1,582 populated cells  of  51 x 7 x 18 = 6,426  →  24.6% coverage
```

Puerto Rico is the sparse extreme: **only sectors 1, 2 and 7 exist, and only 8 of the 18
fuel codes are populated** (`COL DFO HYC NG OTH RFO SUN WND` per the bulk file). Consequences:

- A "every geography × sector × fuel combination exists" test is **wrong** and would
  fail on valid data.
- Completeness is asserted on the *observed* key set (compared against the previous run
  and against the source's own record count), not against a Cartesian product.
- Any completeness test applied to `PR` must be scoped to its 3 sectors, or it will fail
  permanently. This is a real, permanent characteristic of the source, not a transient
  data gap.

---

## 6. Natural / business key

✅ **`(period, location, sectorid, fueltypeid)`** — the API's own row identity; the
response contains no surrogate id, so this 4-tuple is the only candidate key.

This is a natural (surrogate-free) composite key. No surrogate key is introduced in the
fact table. PostgreSQL uses these columns for guarded UPSERTs; current dbt models
are views and tables and declare no incremental `unique_key`.

The only derived key in the model is a surrogate on each dimension, because dimensions
are conformed and joined.

**Uniqueness was tested, not assumed — see §6a.**

### 6a. Uniqueness verification

✅ **VERIFIED live (2026-10-09):** `scripts/verify_api_contract.py --months 1`
scanned 2026-07 through the production `EiaClient` + `iter_pages` path:

```
latest period on the route : 2026-07
  2026-07: 4 page(s), 17,392 rows of 17,392 (complete), 17,392 distinct keys
    no duplicate keys
```

The check is implemented in `scripts/verify_api_contract.py`: it pages one
month at a time through the production client and pagination code and counts
the key tuple locally — the literal `GROUP BY period, location, sectorid,
fueltypeid HAVING COUNT(*) > 1`, executed as a bounded scan. One month
costs ~1–4 requests, so it fits comfortably under a registered key's
budget; `--months N` widens the scan to the N most recent months. The
scan also stops before the rate budget is exhausted rather than sleeping
out a lockout.

What *was* verified, on the complete 2026 January–July source data (7,991 plant rows):

```
distinct (state, sector, fuel) dimension combos ......... 1,598
distinct (month, state, sector, fuel) fact cells ....... 11,109
    January 1,588   February 1,587   March 1,588
    April   1,587   May     1,588   June    1,589
    July    1,582
cells backed by >1 plant row (aggregations, expected) . 5,896
cells backed by exactly 1 plant row .................... 5,213
```

Every figure in this block now reproduces from the checked-in
`scripts/verify_phase0.py` — see the §0a table. The 5,896/5,213 split
is the one figure that measures differently (6,501/4,608), for the
definition reason given in §0a.

**Conclusion: the source is a strict many-to-one map onto
`(month, geography, sector, energy source)`.** 5,896 of 11,109 cells collapse multiple
plant rows into one value, and no cell is produced by two conflicting values. Any
conforming aggregate over this data — which the API is, as proven by the §0
reconciliation — has that 4-tuple as a unique key.

**What is still required before the key is frozen, and how to run it.** One bounded
slice is enough to settle it empirically, because a duplicate is a duplicate anywhere.
The check is implemented — `scripts/verify_api_contract.py` pages a single month
across all locations and sectors through the production client and pagination code
and counts the key tuple locally, which is the literal
`GROUP BY period, location, sectorid, fueltypeid HAVING COUNT(*) > 1` executed as
a bounded scan. One month costs ~1–4 requests, so it fits inside DEMO_KEY's
measured budget; a registered key affords the full lookback window (`--months N`):

```bash
EIA_API_KEY=<key> python scripts/verify_api_contract.py --months 1
```

Two further confirmations belong in the same Phase 1 gate:
- `response.total` for that slice equals the number of rows actually paged, which proves
  the pagination is neither skipping nor repeating rows.
- The paged 4-tuple set for `location=AK, sectorid=1, fueltypeid=ALL` reproduces
  594,571.04 MWh, re-running the §0 reconciliation against the API itself.

**If duplicates are found**, the key is *not* revised on a hunch. The cause would have to
be identified first (a source emitting both a base and an aggregate code under one id, or
a genuine re-publication), and only then would the key or the dedup rule change.

**Duplicate handling — the actual rule.** The pipeline must guarantee **at most one source
row per MERGE key after batch-level deduplication**:

- **Duplicate rows within an incoming batch** must be **detected and resolved before
  MERGE**. Resolution is non-arbitrary: rows with an identical key *and* an identical
  payload collapse to one; rows with an identical key but **conflicting** payloads are a
  hard validation failure, because the pipeline cannot know which is correct and guessing
  would be worse than stopping.
- **Duplicate delivery across separate batches** — the same logical record arriving in a
  later run, or a backfill re-reading a range already held — is checked against
  immutable audit provenance before transactional UPSERT (§7a).

The architecture is unchanged by this:

```
raw batch  →  batch-level deduplication  →  MERGE on the business key
```

A key is not accepted on faith, and MERGE is not asked to paper over a batch that was
never checked.

---

## 7. Revision behavior

**Verified (✅):**
- The source is **not append-only**. The file is a *time-series* file: each vintage
  restates every month of the current calendar year, and the archive filename encodes a
  **vintage date** (`..._M_07_2026_21SEP2026` = July 2026 data, published 21 Sep 2026).
- Each month's value is revised across successive vintages as respondents correct
  prior-period submissions and as EIA reclassifies plants.
- Within a single vintage the file is internally consistent — ✅ measured: YTD column
  `CR` equals the sum of the Jan–Jul monthly `NETGEN` columns for **7,991 / 7,991** rows,
  0 mismatches. So revision happens *between* vintages, not within one.

**⚠️ Not yet measured: the magnitude of the revision horizon** — i.e. how many months
back a single new vintage actually changes. This is the number the spec requires before
fixing a lookback window, and I will not invent it.

**Initial operational revision lookback: 24 months.** This is deliberately conservative
because the actual historical revision horizon has not yet been measured. Immutable batches retain observations for future comparisons; per-key change-history
reporting and empirical lookback tuning remain future work.

- The window is configurable via env var; 24 months is the shipped default.
- The justification for it being *sufficiently large* is structural, not empirical: each
  vintage restates the entire current calendar year (✅ proven, §7), and plant/fuel/sector
  reclassification propagates further back than a single year.
- The justification for it being *not larger* is cost: it bounds the per-run page count.
- What is **not** claimed: that 24 months is the revision horizon. It is not measured,
  so it is not asserted. The honest framing is "conservative pending measurement".

**Not SCD2.** The warehouse keeps **current state only** — a revised value overwrites the
old one. Revision *history* is preserved in the immutable raw zone, where every vintage
is retained. This is the correct split: analytics want "the best current number",
audit wants "what we were told and when".

### 7a. Two distinct operating modes

Both modes share one code path — same extract, same raw layout, same load, same MERGE —
and differ only in how the period range is chosen. That is deliberate: a backfill must
not be a second implementation that can drift from the first.

**NORMAL OPERATION** — monthly Airflow schedule, no explicit period range:

```
determine latest available source period
  → calculate configurable rolling lookback (default 24 months)
    → re-fetch that period window
      → store the new extraction immutably (new run_id, new objects, never overwritten)
        → load into the PostgreSQL raw schema
          → deduplicate within the batch
            → MERGE on the verified business key
              → record run provenance, row count and outcome in the audit table
```

Cheap and bounded: ~24 months of data per run regardless of total history held.

**EXPLICIT HISTORICAL RECONCILIATION** — a triggered backfill with an explicit range:

```
accept an arbitrary historical range (start_period, end_period)
  → same steps, with the range supplied instead of derived
```

This is what handles source corrections **older** than the rolling window. If EIA
republishes 2016, the normal run will never see it — that is by design, since re-fetching
all history every run is not affordable. A backfill is the deliberate, operator-initiated
act of re-reading an older range and MERGEing it through the identical path.

Properties that follow from sharing the path:
- A backfill is **idempotent** on exactly the same terms as a normal run, because the
  MERGE key and the dedup rule are the same.
- Publication checks immutable run provenance before changing existing keys; stale
  and ambiguous replacements reject the whole batch. Missing keys do not cause deletions.
- A newer identical observation advances fact provenance; an unchanged same-run retry
  is idempotent. Fixture and live origins cannot supersede one another.
- Cooperating publications share a transaction-scoped advisory lock. Ordering uses
  local `extracted_at`, not an authoritative EIA revision timestamp; raw history retains
  both observations. Caller-owned outer transactions control final commit.

---

## 8. Pagination

✅ Confirmed live:
- **Maximum 5,000 rows per page in JSON format.** The API returns an explicit warning:
  > `{"warnings":[{"warning":"incomplete return","description":"The API can only return
  > 5000 rows in JSON format. Please consider constraining your request with facet,
  > start, or end, or using offset to paginate results."}]}`
- Paging is `offset` + `length`; `response.total` gives the unpaged row count.
- The warning is emitted whenever the result is truncated — the extractor must treat
  this as a **hard signal that more pages exist**, not as informational text.

**Design consequence:** paginate over a **bounded, configured time window**
(`start` / `end` on `period`), not over the whole history. A window is the natural
idempotent, restartable unit: re-running the same window produces the same raw batch
shape and MERGE makes it safe. Full-history = 955 pages and is explicitly out of the
default path.

**Offset-paging hazard, handled:** if the source revises data *while* a multi-page
extraction is in flight, offset paging can skip or duplicate a row. Mitigation is
structural, not clever: pages are MERGEd on the business key and the raw batch is
deduplicated before load, so a duplicated row is a no-op and a skipped row is corrected
by the next run's lookback window.

---

## 9. Rate limiting

✅ Measured, and it is a hard constraint. The API **publishes its own budget in headers**,
which the extractor should read rather than hardcode:

```
HTTP/1.1 429 Too Many Requests
X-Ratelimit-Limit: 10
X-Ratelimit-Remaining: 0
Retry-After: 5793                       # ≈ 96 minutes
```

Body:
```json
{"error":{"code":"OVER_RATE_LIMIT","message":"You have exceeded your rate limit. Try again
 later or contact us for assistance: https://api.eia.gov:443"}}
```

Three separate observations were made over the course of Phase 0, and they each change the
implementation:

1. Roughly a dozen requests with `DEMO_KEY` exhaust the budget.
2. The lockout window is **~1.6–1.8 hours** (`Retry-After` observed at 6548s and again at
   5793s) — not a per-second limit. A naive "retry after 1s, then 2s, then 4s" backoff
   will hammer the API for two hours and never succeed.
3. Two independent client IPs were rate-limited **identically**, so the limit is keyed on
   the API key, not the source IP.

**Consequences:**
- `DEMO_KEY` is **unusable** for this project. A free registered key
  (`https://api.eia.gov/signup/`) is required and must be supplied via `EIA_API_KEY`.
- **Read `Retry-After` and sleep for exactly that long.** Do not substitute a shorter
  backoff, and do not retry a 429 before that deadline. This single behaviour is the
  difference between recovering and burning the whole window.
- Track `X-Ratelimit-Remaining` and self-throttle *before* the budget hits zero, so
  pacing adapts to the key's actual allowance rather than a guessed constant.
- Use capped exponential backoff with jitter for 5xx and transport errors — a different
  failure class from 429, which is a *budget* signal, not a *server* signal.
- 429 is **not** retryable by the data-quality gate; it is retryable by the HTTP client.
  These are different failure classes and get different policies.

---

## 10. Candidate fact

✅ `fact_generation` — grain:

> **One row represents total net electricity generation for one reporting location,
> in one sector, from one energy source, during one month.**

Measures (only those present in the source — §4):
- `generation_thousand_mwh` (double precision) — net generation, net of station use
- `generation_mwh` (double precision) — same value × 1000, for unit-free downstream use
- `is_rollup_energy_source` / `is_rollup_geography` / `is_rollup_sector` — carried so aggregations are safe by default

No derived or invented measures. No cost, no capacity, no price — those are not on this route.

---

## 11. Candidate dimensions

| dimension | source of truth | notes |
|---|---|---|
| `dim_date` | derived from `period` | ✅ month grain. Not annual. No rollup concept. |
| `dim_geography` | `location` + `stateDescription` | Current classifier: 52 explicit leaves + 14 rollup strings (§5); the historical API count of 64 is not the dimension's fixed size |
| `dim_sector` | `sectorid` + `sectorDescription` | Current seed: 7 leaf codes (`1–7`), with FALSE rollup flags; 8 additional reported API codes remain `UNKNOWN` |
| `dim_energy_source` | `fueltypeid` + `fuelTypeDescription` | ✅ 15 leaf + 12 aggregate; `is_aggregate` flag |

**Every joinable dimension carries a rollup flag** (named `is_aggregate` on
`dim_energy_source`, matching the source's own vocabulary). One uniform invariant, applied
to all three, so the rule for correct aggregation is stated once and cannot be forgotten on
one path. Two of the three flags are non-trivial today; all three are enforced by the same
test. The sector flag records known rollup status; the publication gate separately
blocks unknown codes until their classification is established.

No dimension is included that the source does not support. In particular there is
**no plant dimension** — the API does not expose plant, and the fact's grain does not
need one.

---

## 12. Source limitations

1. ✅ **Values can legitimately be negative.** Measured on 2026-01…2026-07:
   **6.15%** of monthly net-generation cells are negative (3,269 of 53,176) and 23.9% are
   zero. Cause: pumped storage and CHP plants consume more than they generate.
   → **A `generation >= 0` data-quality test would fail on valid data and is forbidden.**
   Negative values are preserved as-is.
2. ✅ **`null` is not zero.** The API returns `"generation": null` (✅ observed on
   `AK / 2026-07 / AOR`). Null means *no data*, zero means *measured zero*. These must
   stay distinguishable end to end. Nulls are common for combination codes that do not
   apply to a given location.
3. ✅ **~2-month publication lag**, so a "daily freshness" SLA on the source is wrong.
   Freshness must be measured against *expected latest period*, not wall-clock today.
4. ✅ **Zeros are ~24% of cells.** Many exist because a location does not use a fuel at
   all. The API does not distinguish "not applicable" from "reported zero" — a
   limitation inherited by the warehouse.
5. ✅ **Puerto Rico is on a separate sheet** in the source file. ✅ the API's `location`
   facet includes `PR`. Whether `PR` is fully populated for every sector/period is ⚠️
   unverified and will be checked on first real extraction.
6. ⚠️ **EIA-906/920/923 territory pre-2001** is not on this route; the practical floor
   is 2001-01.
7. ✅ **No daily data.** The source is monthly, and the implemented Airflow DAG is
   scheduled monthly. Scheduler-driven operation is not established by local DAG tests.
8. ✅ The API returns numeric values as **JSON strings** (see §4).

---

## 13. Answers required at Phase 0 exit

### 1. Recommended source route
`/v2/electricity/electric-power-operational-data/data/`, `frequency=monthly`,
`data[0]=generation`, filtered by a configurable `start`/`end` period window.

### 2. Exact source grain
One row = net generation (thousand MWh) for one location × one sector × one energy
source × one month. ✅ verified by reconciliation against the plant-level source file.

### 3. Business key — VERIFIED ✅ (2026-10-09)

`(period, location, sectorid, fueltypeid)` — natural composite, enforced by the
PostgreSQL primary key and `INSERT ... ON CONFLICT ... DO UPDATE`. Current dbt models
are views and tables, not incremental models declaring a `unique_key`. No fact surrogate.

**✅ Verified live:** `scripts/verify_api_contract.py --months 1` scanned 2026-07 in full
(17,392 rows, 17,392 distinct keys, 0 duplicates). The literal
`GROUP BY period, location, sectorid, fueltypeid HAVING COUNT(*) > 1` check is closed.

### 4. Proposed warehouse schema
PostgreSQL is the warehouse. Schemas: `raw` (the ingested, typed landing table), `core`
(the canonical fact + conformed dimensions), `audit` (one row per ingestion run), and
`marts` (analytical models).
Partition the fact by `period`; index `(location, sectorid, fueltypeid)`, which are
exactly the columns dashboards filter and join on.

### 5. Historical proposed incremental strategy

The original proposal below is retained as design history, not current dbt configuration.
The implemented loader applies guarded PostgreSQL UPSERTs; dbt uses two views and two tables.

Rolling window, newest-first, **default 24 months, configurable**.
`materialized='incremental'`, `incremental_strategy='merge'`,
`unique_key=['period','location','sectorid','fueltypeid']`,
incremental filter `period >= (max(period) in warehouse - lookback months)`.
Because the key is a MERGE key and not a "greater than max" filter, revised historical
rows are **updated**, not skipped and not duplicated.

### 6. Historical proposed revision-handling strategy

The original proposal below is historical. Current publication validates immutable run
identity and source origin, rejects stale/equal-time conflicts atomically, and advances
provenance for newer identical observations (§7a; [data-model.md](data-model.md#7-storage-and-revision-behaviour)).
Missing keys are not deleted. Audit stores run-level metadata, not a per-key change history;
source-withdrawal policy and automatic rebuild tooling remain unresolved.

- Raw zone: every vintage written to a new immutable object. History preserved.
- Raw load: append, tagged with `run_id`.
- Warehouse: MERGE to current state. Revised row overwrites; unchanged row is untouched;
  a re-delivered duplicate produces no second row.
- Audit: record which periods changed, to measure the true horizon.
- Deterministic replay: the warehouse can be rebuilt from raw at any time.

### 7. Risks

| # | risk | severity | mitigation |
|---|---|---|---|
| R1 | Rollup members silently double-counted | **critical** → **mitigated** | `is_rollup` on all 3 dims (`is_aggregate` on energy source). **Verified live (2026-10-09):** geography (historically reported 64 API members; classifier accepts 52 leaves + 14 rollup strings, including legacy spellings — §5), energy source (15 leaf + 12 aggregate, 18 UNKNOWN), sector (current seed: 7 leaf codes; 8 additional API codes historically reported, with labels and membership unresolved — see §5). 3 formerly-leaf codes (`SUN`, `WND`, `COL`) reclassified as aggregates after numerical proof. Remaining UNKNOWN codes are blocked at aggregate publish. |
| R1b | Key uniqueness assumed rather than proven | **high** → **closed** | Verified live on 2026-07: 17,392 rows, 17,392 distinct keys, 0 duplicates (§6a) |
| R2 | DEMO_KEY rate limit makes the API unusable | **critical** | require a registered `EIA_API_KEY`; serial requests; honour `Retry-After` |
| R3 | Lookback window too short → silent data loss on old revisions | **high** | 24 months is *conservative and explicitly unproven*; changes are recorded per batch so the horizon is measured from evidence; arbitrary older corrections handled by the explicit backfill path (§7a), not by a wider scheduled window |
| R4 | Negative values rejected by a naive DQ rule | medium | domain rule derived from measurement (§12.1) and pinned by `tests/unit/test_records.py` |
| R5 | Numeric values arrive as strings | medium | explicit cast in staging + a `dbt` type test |
| R6 | Null collapsed to zero upstream | medium | null-preserving staging test; no `COALESCE(...,0)` in staging |
| R7 | Offset paging racing a source revision | medium | MERGE-on-key + dedupe makes it self-correcting; next run's window repairs it |
| R8 | Cost of a long backfill | low | bounded default window; documented page math (§3) |
| R9 | Silent no-op runs mistaken for fresh data | medium | freshness reported as *latest source period*, not *last DAG success* |
| R10 | Raw history lost to a key collision or an overwrite | **critical** | UUID4 `run_id` per execution for the namespace, **plus** create-only on every write (`O_CREAT\|O_EXCL`); a collision fails loudly and preserves the existing object. Enforced in `tests/unit/test_raw_storage.py` and `tests/integration/test_local_raw_storage.py` |

### 8. What must change / what is blocked before Phase 1

**Historical verification status (2026-10-09):** live gate completion was reported;
sector-label evidence and classification of the eight additional codes remain unresolved.

1. ✅ **Business-key uniqueness** — `verify_api_contract.py --months 1`: 2026-07, 17,392 rows, 17,392 distinct keys, 0 duplicates. Key is **VERIFIED**.
2. ✅ **Full `fueltypeid` facet list** — `/facet/fueltypeid`: 45 members. 15 leaf + 12 aggregate (incl. reclassified `SUN`, `WND`, `COL`) match the seed; 18 additional codes are `UNKNOWN`.
3. ⚠️ **`sectorid` facet list** — historically reported as 15 members (`1–7` + `8, 90, 94–99`). No independently inspectable captured sector-facet response was found. The eight additional codes remain `UNKNOWN`; their reported labels are listed with provenance limitations in §5. Leaf/rollup membership and numerical aggregation relationships remain unproven.
4. **Historical Pacific geography report** — `PAC`, `PCC`, `PCN` were reported from the API facet; these and legacy `PACC`/`PACN` remain `ROLLUP`. The legacy spellings were wrongly attributed to the bulk file (§5).
5. ⚠️ Earliest available period → not yet measured (rate budget not spent).
6. ⚠️ Revision-horizon measurement → not yet run (accumulates over ~3 months).

**Design changes required by discovery (these invalidate the spec's illustrative sketch):**
- `dim_region` becomes **`dim_geography`** with a `is_rollup` flag, because "region" is
  ambiguous in this source (state vs census division vs NERC region all exist) and the
  source ships rollups alongside leaves.
- `fact_generation` gains a **`generation_mwh`** measure and null-preserving semantics;
  **no** non-negativity rule.
- **The currently classified sector seed has 7 leaf codes (`1–7`)**, matching the observed bulk-file vocabulary; it is not the complete reported API sector vocabulary.
- A **`dim_energy_source.is_aggregate`** flag is mandatory — not optional polish.

---

## Appendix — reproduction

Bulk-file checks need no API credential. Live checks require a registered EIA key
and explicit authorization; the examples below do not capture or establish verification:

```bash
# The file the API wraps (no key required)
curl -sSL -o f923_2026.zip \
  https://www.eia.gov/electricity/data/eia923/xls/f923_2026.zip

# The API itself (needs a key; DEMO_KEY works but rate-limits after ~a dozen calls)
curl -sG https://api.eia.gov/v2/electricity/electric-power-operational-data/data/ \
  --data-urlencode 'api_key=<KEY>' \
  --data-urlencode 'frequency=monthly' \
  --data-urlencode 'data[0]=generation' \
  --data-urlencode 'length=2'
```

The XLSX is a ZIP of XML, so the discovery scripts were written with stdlib
`zipfile` + `xml.etree` only — no pandas or openpyxl required.

**Sources:**
- [EIA API v2 — electric power operational data](https://api.eia.gov/v2/electricity/electric-power-operational-data/data/)
- [Form EIA-923 data files](https://www.eia.gov/electricity/data/eia923/)
- [EIA API registration](https://api.eia.gov/signup/)
