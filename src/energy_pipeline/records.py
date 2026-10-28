"""Typed records, rollup classification and batch-level deduplication.

Grain (verified in Phase 0, docs/source-data.md):

    One row represents total net electricity generation for one reporting
    geography, in one sector, from one energy source, during one month.

Business key: ``(period, location, sectorid, fueltypeid)`` — **VERIFIED**
live on 2026-10-09 (2026-07: 17,392 rows, 17,392 distinct keys, 0 duplicates;
see `docs/project-state.md` §4).

Two Phase 0 findings drive the parsing rules here, and both would be easy to get
wrong:

* **The API returns numbers as JSON strings.** ``{"generation": "594.57104"}``.
  A pass-through would land STRING in the warehouse and break every aggregate.
* **Negative values are valid.** 6.15% of monthly cells are negative, because
  pumped storage and CHP plants consume more than they generate. Nothing here
  clamps, validates away, or filters a negative.

``null`` is also preserved: ``null`` means *no data*, ``0`` means *measured
zero*. Collapsing one into the other would fabricate roughly a quarter of the
dataset, since 23.9% of observed cells are genuine zeros.

Every joinable dimension code is classified as exactly one of three things:
**leaf**, **rollup**, or **unknown**. The third is the load-bearing one. An
unrecognised code is never silently treated as a leaf — that would fold a
newly introduced aggregate straight into every total, and the only symptom would
be numbers that are too large. Ingestion always accepts the raw row; whether an
unclassified code may *participate in an aggregate* is a separate, hard-gated
decision (see :func:`energy_pipeline.validation.aggregate_publish_gate`).
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from .errors import ValidationError

#: Exact source unit supported by the tracked EIA evidence.
CANONICAL_GENERATION_UNITS = "thousand megawatthours"

#: The nine API fields for a ``generation`` query, exactly as returned.
REQUIRED_FIELDS = (
    "period",
    "location",
    "stateDescription",
    "sectorid",
    "sectorDescription",
    "fueltypeid",
    "fuelTypeDescription",
    "generation",
    "generation-units",
)

#: Descriptive fields are defaulted when absent rather than rejected — they carry
#: no business meaning, and rejecting a row for a missing label would fail the
#: batch over cosmetic data.
_TEXT = ""

PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

#: The seven currently classified leaf sectors, supported by the historical
#: bulk-file vocabulary; this is not the complete API sector vocabulary.
#: Other sector codes remain UNKNOWN: validation warns and publication blocks.
KNOWN_SECTORS = frozenset("1234567")

#: Fourteen accepted rollup strings: eight non-Pacific census divisions, US,
#: and five Pacific spellings. Historical API reports name PCC/PCN/PAC;
#: PACC/PACN are retained legacy spellings, previously misattributed to the bulk
#: file (which carries no division codes). No mapping between spellings is
#: inferred. This compatibility set is not a verified current API facet list.
#: See "Pacific geography vocabulary" in docs/source-data.md §5.
ROLLUP_GEOGRAPHIES = frozenset(
    {
        "US",
        "NEW",
        "MAT",
        "SAT",
        "ESC",
        "WSC",
        "ENC",
        "WNC",
        "MTN",  # census divisions
        "PACC",
        "PACN",  # retained legacy Pacific spellings; source attribution unresolved
        "PCC",
        "PCN",
        "PAC",  # Pacific spellings in historical API reports
    }
)

#: Confirmed aggregate energy-source codes. Each member was verified as a rollup
#: by a live data check (not a name guess) on 2026-07 at US/All-Sectors:
#:
#: - ``ALL`` — observed in a live response; sums all leaf fuels
#: - ``AOR`` — observed in a live response; sums renewable leaves
#: - ``SUN`` — verified: SUN = SPV + STH (41,411.78 = 41,096.84 + 314.94)
#: - ``WND`` — verified: WND = WNS + WNT (32,183.93 = 159.55 + 32,024.38)
#: - ``COL`` — verified: COL = COW - WOC (75,062.39 = 75,784.96 - 722.57);
#:   COW is an aggregate, COL excludes waste-coal
#: - ``COW`` — verified: COW = COL + WOC
#: - ``TSN`` — verified: TSN = SUN + DPV (52,336.84 = 41,411.78 + 10,925.07)
#: - ``TPV`` — verified: TPV = SPV + DPV (52,021.90 = 41,096.84 + 10,925.07)
#: - ``FOS`` — verified: FOS is a superset of NG (fossil fuels aggregate)
#: - ``REN`` — verified: REN is a superset (renewable aggregate)
#: - ``NGO`` — verified: NGO >= NG (natural gas & other gases aggregate)
#: - ``PET`` — verified: PET >= DFO + RFO + PC (petroleum aggregate)
#:
#: Codes with aggregate names but UNVERIFIED numerically (remain UNKNOWN):
#: ``BIO``, ``OB2``, ``OBW``, ``MSB``, ``PEL``, ``RC``.
#: These need the same numerical check before promotion from UNKNOWN to rollup.
KNOWN_ENERGY_SOURCE_AGGREGATES = frozenset(
    {
        "ALL",
        "AOR",
        "SUN",
        "WND",
        "COL",
        "COW",
        "TSN",
        "TPV",
        "FOS",
        "REN",
        "NGO",
        "PET",
    }
)

#: The 15 leaf MER energy-source codes. ``SUN``, ``WND``, ``COL`` were moved to
#: KNOWN_ENERGY_SOURCE_AGGREGATES after the 2026-10-09 live verification proved
#: they are rollups (SUN = SPV + STH, WND = WNS + WNT, COL = COW - WOC).
#: See KNOWN_ENERGY_SOURCE_AGGREGATES comment for verification evidence.
BASE_ENERGY_SOURCES = frozenset(
    {
        "DFO",
        "GEO",
        "HPS",
        "HYC",
        "MLG",
        "NG",
        "NUC",
        "OOG",
        "ORW",
        "OTH",
        "PC",
        "RFO",
        "WOC",
        "WOO",
        "WWW",
    }
)

#: Conventional 50-state postal codes plus DC/PR, supported by historical
#: project bulk-file evidence (docs/source-data.md section 0a). This is not an
#: independently verified list of the complete current EIA API location facet.
KNOWN_LEAF_GEOGRAPHIES = frozenset(
    {
        "AK",
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "HI",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "PR",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    }
)

#: Classification outcomes for a dimension code.
LEAF = "leaf"
ROLLUP = "rollup"
UNKNOWN = "unknown"

#: The three joinable dimensions, in the order they are reported.
DIMENSIONS = ("geography", "sector", "energy_source")


def classify_energy_source(fueltypeid: str) -> str:
    """``base`` -> ``leaf``, known aggregate -> ``rollup``, anything else -> ``unknown``.

    The third branch is the point. Defaulting an unrecognised code to *leaf*
    would double-count it into every aggregate the moment EIA introduces a new
    aggregate, and the error would be invisible: the numbers would simply be
    too big. ``UNKNOWN`` is its own answer, and it never resolves to ``leaf``
    on its own — only a human reading the source's facet list can move a code
    there.
    """
    if fueltypeid in KNOWN_ENERGY_SOURCE_AGGREGATES:
        return ROLLUP
    if fueltypeid in BASE_ENERGY_SOURCES:
        return LEAF
    return UNKNOWN


def classify_geography(location: str) -> str:
    """Same three-way contract for ``location``."""
    if location in ROLLUP_GEOGRAPHIES:
        return ROLLUP
    if location in KNOWN_LEAF_GEOGRAPHIES:
        return LEAF
    return UNKNOWN


def classify_sector(sectorid: str) -> str:
    """Same three-way contract for ``sectorid``."""
    if sectorid in KNOWN_SECTORS:
        return LEAF
    return UNKNOWN


#: One place that answers "which of this row's dimension codes are unclassified".
#: Validation reads this instead of re-deriving the rules, so a change to the
#: classification cannot silently leave a second, drifting copy behind.
_CODE_ACCESSORS = {
    "geography": (classify_geography, lambda r: r.location),
    "sector": (classify_sector, lambda r: r.sectorid),
    "energy_source": (classify_energy_source, lambda r: r.fueltypeid),
}


def unknown_codes(record: GenerationRecord) -> dict[str, str]:
    """Dimension name -> offending code, for codes this seed does not classify."""
    return {
        name: read(record)
        for name, (classify, read) in _CODE_ACCESSORS.items()
        if classify(read(record)) is UNKNOWN
    }


@dataclass(frozen=True, slots=True)
class GenerationRecord:
    """One row at the verified grain."""

    period: str
    location: str
    state_description: str
    sectorid: str
    sector_description: str
    fueltypeid: str
    fuel_type_description: str
    #: Thousand MWh, or None. Never coerced, never clamped, never rounded to zero.
    generation: float | None
    generation_units: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        """The business key. The only identity this record has."""
        return (self.period, self.location, self.sectorid, self.fueltypeid)

    @property
    def geography_class(self) -> str:
        return classify_geography(self.location)

    @property
    def sector_class(self) -> str:
        return classify_sector(self.sectorid)

    @property
    def energy_source_class(self) -> str:
        return classify_energy_source(self.fueltypeid)

    @property
    def is_rollup_geography(self) -> bool:
        return self.geography_class is ROLLUP

    @property
    def is_rollup_sector(self) -> bool:
        """Always False for the 7 verified sectors. Kept as a uniform contract so
        every dimension presents the same question, and so a future EIA sector
        rollup has somewhere to be flagged."""
        return self.sector_class is ROLLUP

    @property
    def is_rollup_energy_source(self) -> bool:
        return self.energy_source_class is ROLLUP

    @property
    def is_rollup(self) -> bool:
        """True if this row is a rollup in *any* dimension. Such rows are retained,
        never dropped — the analyst may legitimately want ``US`` or ``ALL``."""
        return self.is_rollup_geography or self.is_rollup_sector or self.is_rollup_energy_source

    @property
    def is_unknown_code(self) -> bool:
        """True when this row uses a code the current seed does not classify.

        Unknown is *neither* rollup *nor* leaf. A raw row is always ingested; what
        an unclassified code may later be used for is a separate, gated decision.
        """
        return bool(unknown_codes(self))

    @property
    def is_leaf(self) -> bool:
        """True only for a row every dimension positively classified as leaf.

        Not the negation of ``is_rollup``: an unclassified row is not a leaf row,
        and counting it as one is how an unknown code quietly reaches an aggregate.
        """
        return not self.is_rollup and not self.is_unknown_code


def parse_record(row: dict[str, Any]) -> GenerationRecord:
    """Parse one API row. Strict: a contract violation raises rather than guesses."""
    missing = [f for f in REQUIRED_FIELDS if f not in row]
    if missing:
        raise ValidationError(f"row missing required field(s): {', '.join(missing)}")

    period = str(row["period"])
    if not PERIOD_RE.match(period):
        raise ValidationError(f"period {period!r} is not a valid YYYY-MM month")

    generation = validate_generation_contract(row["generation"], row["generation-units"])

    return GenerationRecord(
        period=period,
        location=str(row["location"]),
        state_description=str(row.get("stateDescription", _TEXT)),
        sectorid=str(row["sectorid"]),
        sector_description=str(row.get("sectorDescription", _TEXT)),
        fueltypeid=str(row["fueltypeid"]),
        fuel_type_description=str(row.get("fuelTypeDescription", _TEXT)),
        generation=generation,
        generation_units=str(row.get("generation-units", _TEXT)),
    )


def _parse_generation(value: Any) -> float | None:
    """Cast the source's string-typed number. ``None`` passes through as ``None``.

    A negative number is a legitimate result, not an error, and is returned as-is.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):  # bool is an int subclass; it is never a measure
        raise ValidationError(f"generation must be a number or null, got {value!r}")
    try:
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value).strip())
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValidationError(f"generation {value!r} is neither null nor a number") from exc


def validate_generation_contract(value: Any, units: Any) -> float | None:
    """Require the exact source unit and finite generation, preserving nulls."""
    if not isinstance(units, str) or units != CANONICAL_GENERATION_UNITS:
        raise ValidationError(
            f"generation-units must be {CANONICAL_GENERATION_UNITS!r}, got {units!r}"
        )
    generation = _parse_generation(value)
    if generation is not None and not math.isfinite(generation):
        raise ValidationError(f"generation must be finite or null, got {value!r}")
    return generation


def parse_records(rows: Iterable[dict[str, Any]]) -> tuple[list[GenerationRecord], list[str]]:
    """Parse a page of rows, collecting contract violations instead of aborting.

    One malformed row should surface as a reported finding with a row count, not
    as the silent loss of a whole 5000-row page.
    """
    records: list[GenerationRecord] = []
    errors: list[str] = []
    for index, row in enumerate(rows):
        try:
            records.append(parse_record(row))
        except ValidationError as exc:
            errors.append(f"row {index}: {exc}")
    return records, errors


@dataclass(frozen=True, slots=True)
class DedupeResult:
    """Outcome of batch-level deduplication.

    The contract: **at most one source row per MERGE key after batch-level
    deduplication.** Identical duplicates collapse. Conflicts are reported, never
    resolved by guessing.
    """

    records: list[GenerationRecord]
    identical_duplicates: int
    conflicting_keys: list[tuple[str, str, str, str]]

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicting_keys)


def dedupe(records: Sequence[GenerationRecord]) -> DedupeResult:
    """Collapse duplicates within one batch, before anything reaches MERGE.

    Two rows with the same key and the same payload are the same fact delivered
    twice — collapsing them is unambiguous. Two rows with the same key and
    *different* payloads mean the source contradicted itself, and the pipeline
    has no principled way to pick a winner, so the key is reported and left for
    the caller to fail on.
    """
    kept: dict[tuple[str, str, str, str], GenerationRecord] = {}
    order: list[tuple[str, str, str, str]] = []
    identical = 0
    conflicts: list[tuple[str, str, str, str]] = []

    for record in records:
        existing = kept.get(record.key)
        if existing is None:
            kept[record.key] = record
            order.append(record.key)
            continue
        if existing == record:
            identical += 1
        else:
            if record.key not in conflicts:
                conflicts.append(record.key)

    return DedupeResult(
        records=[kept[key] for key in order],
        identical_duplicates=identical,
        conflicting_keys=conflicts,
    )
