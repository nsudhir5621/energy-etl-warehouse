"""Stage raw zone data into PostgreSQL (Phase 3, Stages 2-3).

Reads raw objects from the immutable raw zone via :class:`ObjectStore`,
parses them through the same record-parsing pipeline as extraction, applies
dimension classification, and writes the results into ``raw.stg_generation``
(Stage 2). Then upserts core dimensions and MERGEs staged rows into
``core.fact_generation`` (Stage 3). Records audit rows with appropriate status.

Stage 2 (staging): one ``run_id`` = one immutable audit identity. A matching
retry claims that identity before replacing its staging rows in the same
transaction. Staging is keyed on ``(run_id, period, location, sectorid, fueltypeid)``.

Stage 3 (merge): reads from ``raw.stg_generation`` for a given ``run_id``,
checks the aggregate publish gate, creates monthly partitions on demand,
upserts the four dimension tables, and MERGEs the staged rows into
``core.fact_generation`` on the business key
``(period, location, sectorid, fueltypeid)`` — distinct from ``run_id`` which
is execution identity.

Invariants preserved from the rest of the pipeline:

- **Null ≠ zero.** ``generation`` is ``None`` or a float; never coerced.
- **Negatives preserved.** No ``CHECK >= 0``; pumped-storage net-consumption
  values pass through untouched.
- **Tri-state classification.** Rollup flags are derived from the same
  ``classify_*`` functions in :mod:`records`, so an unknown code is never
  silently treated as a leaf.
- **run_id ≠ content_hash.** Both pass through from the manifest; execution
  identity and content identity stay distinct.
- **Publish gate.** Unknown dimension codes are accepted at staging but
  blocked from ``core.fact_generation`` by
  :func:`~energy_pipeline.validation.aggregate_publish_gate`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from .config import DEFAULT_FREQUENCY, DEFAULT_MEASURE, DEFAULT_ROUTE
from .errors import ValidationError
from .extract import API_SOURCE_NAME, FIXTURE_SOURCE_NAME, SOURCE_NAME
from .records import (
    DedupeResult,
    GenerationRecord,
    dedupe,
    parse_records,
    validate_generation_contract,
)
from .validation import Severity, ValidationReport, aggregate_publish_gate, validate
from .warehouse import next_period, partition_name

if TYPE_CHECKING:
    from .extract import ExtractionResult

#: Audit statuses this stage can write. ``staged``/``failed`` by Stage 2;
#: ``succeeded``/``empty`` by Stage 3 (MERGE).
STATUS_STAGED = "staged"
STATUS_FAILED = "failed"
STATUS_SUCCEEDED = "succeeded"
STATUS_EMPTY = "empty"

# --- SQL (static; psycopg3 param-styles are %s at execute time) --------------------

#: Idempotent retry: clear this run's staging rows before re-inserting.
_DELETE_STG_FOR_RUN = "DELETE FROM raw.stg_generation WHERE run_id = %s"

#: Insert with ON CONFLICT DO NOTHING as a safety net — dedupe already
#: guarantees no key collisions within a run, but the guard prevents a
#: half-stage from leaving duplicates if dedupe logic ever changes.
_INSERT_STG_ROW = """\
INSERT INTO raw.stg_generation
    (run_id, content_hash, period, location, sectorid, fueltypeid,
     generation_thousand_mwh, is_rollup_geography, is_rollup_sector,
     is_rollup_energy_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, period, location, sectorid, fueltypeid) DO NOTHING
"""

#: Claim a run atomically; only matching identity may update mutable audit state.
_AUDIT_UPSERT = """\
INSERT INTO audit.ingestion_run AS accepted
    (run_id, content_hash, source, window_start, window_end,
     extracted_at, row_count, status, hard_findings, warn_findings,
     info_findings, error_msg)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id) DO UPDATE SET
    row_count      = EXCLUDED.row_count,
    status         = EXCLUDED.status,
    hard_findings  = EXCLUDED.hard_findings,
    warn_findings  = EXCLUDED.warn_findings,
    info_findings  = EXCLUDED.info_findings,
    error_msg      = EXCLUDED.error_msg,
    updated_at     = now()
WHERE accepted.content_hash IS NOT DISTINCT FROM EXCLUDED.content_hash
  AND accepted.source IS NOT DISTINCT FROM EXCLUDED.source
  AND accepted.window_start IS NOT DISTINCT FROM EXCLUDED.window_start
  AND accepted.window_end IS NOT DISTINCT FROM EXCLUDED.window_end
  AND accepted.extracted_at IS NOT DISTINCT FROM EXCLUDED.extracted_at
"""


# -- Pure helpers ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StagingResult:
    """Outcome of a staging load."""

    run_id: str
    content_hash: str
    staged_rows: int
    identical_duplicates: int
    status: str
    parse_errors: tuple[str, ...] = field(default_factory=tuple)
    conflicting_keys: tuple[tuple[str, str, str, str], ...] = field(default_factory=tuple)


def record_to_stage_row(
    record: GenerationRecord,
    run_id: str,
    content_hash: str,
) -> tuple[Any, ...]:
    """Map a ``GenerationRecord`` to a ``raw.stg_generation`` row tuple.

    Classification flags come from the record's own ``is_rollup_*`` properties,
    which delegate to the same ``classify_*`` functions in :mod:`records`.
    Recheck the numeric/unit contract even when a cached validation report is
    OK. Nulls and finite negatives remain valid for the nullable DOUBLE PRECISION
    column.
    """
    generation = validate_generation_contract(record.generation, record.generation_units)
    return (
        run_id,
        content_hash,
        record.period,
        record.location,
        record.sectorid,
        record.fueltypeid,
        generation,
        record.is_rollup_geography,
        record.is_rollup_sector,
        record.is_rollup_energy_source,
    )


def _count_findings(report: ValidationReport) -> tuple[int, int, int]:
    """Return ``(hard, warn, info)`` counts from a validation report."""
    hard = sum(1 for f in report.findings if f.severity is Severity.HARD)
    warn = sum(1 for f in report.findings if f.severity is Severity.WARN)
    info = sum(1 for f in report.findings if f.severity is Severity.INFO)
    return hard, warn, info


def _read_and_parse(
    store: Any,
    manifest: dict[str, Any],
) -> tuple[list[GenerationRecord], list[str], DedupeResult]:
    """Read all page objects from the store, parse, and deduplicate.

    Verifies each page's declared SHA-256 against the bytes read from storage
    before parsing, so a corrupted or modified raw object fails loudly rather
    than silently staging bad data.

    Returns ``(records, parse_errors, dedupe_result)`` where *records* is the
    deduplicated, order-preserving list ready for staging.
    """
    from .raw import sha256_hex

    all_rows: list[dict[str, Any]] = []
    for obj in manifest["objects"]:
        path = obj["path"]
        declared = obj.get("sha256")
        if not declared:
            raise ValidationError(f"manifest object {path!r} is missing its sha256 checksum")
        raw = store.read(path)
        actual = sha256_hex(raw)
        if actual != declared:
            raise ValidationError(
                f"sha256 mismatch for {path!r}: manifest declares {declared}, "
                f"actual content is {actual}"
            )
        rows = json.loads(raw.decode("utf-8"))
        all_rows.extend(rows)

    records, parse_errors = parse_records(all_rows)
    dedupe_result = dedupe(records)
    return dedupe_result.records, parse_errors, dedupe_result


# -- Audit ---------------------------------------------------------------------


def _validate_run_identity(manifest: dict[str, Any]) -> None:
    """Validate persisted identity without granting trust to legacy source labels."""
    for name in ("run_id", "content_hash", "source", "window_start", "window_end", "extracted_at"):
        value = manifest.get(name)
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValidationError(f"manifest missing or invalid identity field: {name!r}")
    try:
        if str(UUID(manifest["run_id"])) != manifest["run_id"]:
            raise ValueError("noncanonical UUID")
    except ValueError as exc:
        raise ValidationError("manifest run_id must be a canonical UUID") from exc
    if not re.fullmatch(r"[0-9a-f]{64}", manifest["content_hash"]):
        raise ValidationError("manifest content_hash must be a SHA-256 hexadecimal digest")
    # Legacy metadata may be recorded unchanged, but does not establish live origin.
    if manifest["source"] not in (SOURCE_NAME, API_SOURCE_NAME, FIXTURE_SOURCE_NAME):
        raise ValidationError("manifest source is not a supported ingestion origin")
    for name in ("window_start", "window_end"):
        value = manifest[name]
        if not re.fullmatch(r"[0-9]{4}-(?:0[1-9]|1[0-2])", value) or value[:4] == "0000":
            raise ValidationError(f"manifest {name} must be a valid YYYY-MM period")
    if manifest["window_start"] > manifest["window_end"]:
        raise ValidationError("manifest window_start must not follow window_end")
    # Bound precision to TIMESTAMPTZ's microseconds; do not silently round identity.
    timestamp = manifest["extracted_at"]
    if not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?"
        r"(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
        timestamp,
    ):
        raise ValidationError("manifest extracted_at must be a timezone-aware ISO timestamp")
    try:
        observed_at = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValidationError("manifest extracted_at is not a valid timestamp") from exc
    if observed_at.utcoffset() is None:
        raise ValidationError("manifest extracted_at must include a timezone")


def _validate_staging_manifest(manifest: dict[str, Any]) -> None:
    """The audit schema supports this fixed monthly-generation dataset only."""
    _validate_run_identity(manifest)
    for name, expected in (
        ("route", DEFAULT_ROUTE),
        ("frequency", DEFAULT_FREQUENCY),
        ("measure", DEFAULT_MEASURE),
    ):
        if manifest.get(name) != expected:
            raise ValidationError(f"manifest {name} must be {expected!r}")


def record_audit_run(
    conn: Any,
    manifest: dict[str, Any],
    report: ValidationReport,
    *,
    status: str = STATUS_STAGED,
    error_msg: str | None = None,
) -> None:
    """Atomically claim identity and update only mutable audit state.

    A matching retry is accepted; conflicting provenance or unknown acceptance
    fails closed. Inside staging's transaction this block is a savepoint, so the
    claim and its row lock remain subject to the caller's outer transaction.
    """
    _validate_run_identity(manifest)
    hard, warn, info = _count_findings(report)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            _AUDIT_UPSERT,
            (
                manifest["run_id"],
                manifest["content_hash"],
                manifest["source"],
                manifest["window_start"],
                manifest["window_end"],
                manifest["extracted_at"],
                report.record_count,
                status,
                hard,
                warn,
                info,
                error_msg,
            ),
        )
        accepted_rows = getattr(cur, "rowcount", None)
        if type(accepted_rows) is not int or accepted_rows != 1:
            raise ValidationError(
                f"audit identity claim rejected or unconfirmed for run {manifest['run_id']} "
                f"(accepted rows: {accepted_rows!r}); provenance must match the accepted run"
            )


# -- Entry points --------------------------------------------------------------


def load_raw_batch(
    store: Any,
    conn: Any,
    manifest_path: str,
) -> StagingResult:
    """Load a raw batch from the raw zone into ``raw.stg_generation``.

    1. Read ``manifest.json`` from the store.
    2. Read each page object, parse records, collect parse errors.
    3. Deduplicate within this run (batch-level dedupe).
    4. Validate the deduplicated batch — HARD findings abort.
    5. Claim immutable audit identity within the staging transaction.
    6. Replace prior staging rows for this ``run_id`` (idempotent retry).

    HARD validation findings attempt a conditional failed-audit write without
    staging. For valid data, the claim and DELETE/INSERT commit together; an
    identity conflict or SQL failure rolls back that attempt, preserving prior
    audit state and staging rows. Caller transaction ownership is unchanged.

    The ``store`` is any :class:`~energy_pipeline.storage.ObjectStore`
    implementation; ``conn`` is a psycopg3 connection or test double.
    """
    raw_manifest = store.read(manifest_path)
    manifest = json.loads(raw_manifest.decode("utf-8"))

    for field_name in ("run_id", "content_hash"):
        if field_name not in manifest:
            raise ValidationError(f"manifest missing required field: {field_name!r}")

    _validate_staging_manifest(manifest)
    run_id = manifest["run_id"]
    content_hash = manifest["content_hash"]

    # 2-3: read pages, parse, deduplicate
    records, parse_errors, dedupe_result = _read_and_parse(store, manifest)

    # 4: validate (HARD findings → abort)
    report = validate(
        records,
        parse_errors=parse_errors,
        dedupe_result=dedupe_result,
        source_record_total=manifest.get("source_record_total"),
        window_start=manifest.get("window_start", ""),
        window_end=manifest.get("window_end", ""),
    )

    if not report.ok:
        hard_msgs = "; ".join(f.message for f in report.hard_failures)
        record_audit_run(
            conn,
            manifest,
            report,
            status=STATUS_FAILED,
            error_msg=hard_msgs,
        )
        raise ValidationError(f"staging aborted: {hard_msgs}")

    # Claim first; a rejected identity must never delete previously staged rows.
    rows = [record_to_stage_row(r, run_id, content_hash) for r in records]
    with conn.transaction(), conn.cursor() as cur:
        record_audit_run(conn, manifest, report, status=STATUS_STAGED)
        cur.execute(_DELETE_STG_FOR_RUN, (run_id,))
        cur.executemany(_INSERT_STG_ROW, rows)

    return StagingResult(
        run_id=run_id,
        content_hash=content_hash,
        staged_rows=len(records),
        identical_duplicates=dedupe_result.identical_duplicates,
        status=STATUS_STAGED,
        parse_errors=tuple(parse_errors),
        conflicting_keys=tuple(dedupe_result.conflicting_keys),
    )


def stage_from_extraction(
    conn: Any,
    result: ExtractionResult,
) -> StagingResult:
    """Stage an in-memory ``ExtractionResult`` into ``stg_generation`` + audit.

    Unlike :func:`load_raw_batch`, this path skips re-parsing from disk because
    the ``ExtractionResult`` already carries parsed, deduplicated records
    (``merge_rows``) and a validation report. Useful when extraction and
    staging run in the same process.
    """
    batch = result.batch
    # Validate before manifest construction derives object paths from these values.
    _validate_staging_manifest(
        {
            name: getattr(batch, name)
            for name in (
                "run_id",
                "content_hash",
                "source",
                "route",
                "frequency",
                "measure",
                "window_start",
                "window_end",
                "extracted_at",
            )
        }
    )
    manifest = batch.manifest()

    if not result.report.ok:
        hard_msgs = "; ".join(f.message for f in result.report.hard_failures)
        record_audit_run(
            conn,
            manifest,
            result.report,
            status=STATUS_FAILED,
            error_msg=hard_msgs,
        )
        raise ValidationError(f"staging aborted: {hard_msgs}")

    rows = [record_to_stage_row(r, batch.run_id, batch.content_hash) for r in result.merge_rows]
    with conn.transaction(), conn.cursor() as cur:
        record_audit_run(conn, manifest, result.report, status=STATUS_STAGED)
        cur.execute(_DELETE_STG_FOR_RUN, (batch.run_id,))
        cur.executemany(_INSERT_STG_ROW, rows)

    return StagingResult(
        run_id=batch.run_id,
        content_hash=batch.content_hash,
        staged_rows=len(result.merge_rows),
        identical_duplicates=result.dedupe_result.identical_duplicates,
        status=STATUS_STAGED,
        parse_errors=tuple(result.parse_errors),
        conflicting_keys=tuple(result.dedupe_result.conflicting_keys),
    )


# -- Stage 3 SQL (staging -> core) ---------------------------------------------

#: Check if a monthly partition of fact_generation already exists.
_PARTITION_EXISTS_SQL = (
    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = %s"
)

#: Create a monthly partition. pname is derived from partition_name() which
#: only accepts the YYYY-MM format. PostgreSQL does not support bind parameters
#: in DDL (FOR VALUES FROM/TO), so the partition bounds are inlined as
#: single-quoted text literals. This is safe because period is always validated
#: as YYYY-MM upstream (from the EIA source's period field).
#:
#: The bounds must be TEXT (not DATE) because core.fact_generation.period is
#: a TEXT column. PostgreSQL casts partition bounds to the partition key's
#: type (TEXT), so DATE '2026-06-01' becomes '2026-06-01'::text which sorts
#: AFTER '2026-06' lexicographically — causing '2026-06' rows to not match
#: any partition. Using TEXT bounds ('2026-06', '2026-07') ensures correct
#: lexicographic range matching.
_CREATE_PARTITION_TEMPLATE = (
    "CREATE TABLE core.{pname} "
    "PARTITION OF core.fact_generation "
    "FOR VALUES FROM ('{start}') TO ('{end}')"
)

#: Upsert dim_date from staging periods (idempotent via ON CONFLICT).
_DIM_DATE_SQL = """\
INSERT INTO core.dim_date (date_month, period, year, month)
SELECT DISTINCT
    make_date(LEFT(period, 4)::INT, RIGHT(period, 2)::INT, 1),
    period,
    LEFT(period, 4)::INT,
    RIGHT(period, 2)::INT
FROM raw.stg_generation
WHERE run_id = %s
ON CONFLICT (period) DO NOTHING
"""

#: Upsert dim_geography (idempotent — overwrites is_rollup flag on conflict).
_DIM_GEOGRAPHY_SQL = """\
INSERT INTO core.dim_geography (location, is_rollup)
SELECT DISTINCT location, is_rollup_geography
FROM raw.stg_generation
WHERE run_id = %s
ON CONFLICT (location) DO UPDATE SET
    is_rollup = EXCLUDED.is_rollup
"""

_DIM_SECTOR_SQL = """\
INSERT INTO core.dim_sector (sectorid, is_rollup)
SELECT DISTINCT sectorid, is_rollup_sector
FROM raw.stg_generation
WHERE run_id = %s
ON CONFLICT (sectorid) DO UPDATE SET
    is_rollup = EXCLUDED.is_rollup
"""

_DIM_ENERGY_SOURCE_SQL = """\
INSERT INTO core.dim_energy_source (fueltypeid, is_aggregate)
SELECT DISTINCT fueltypeid, is_rollup_energy_source
FROM raw.stg_generation
WHERE run_id = %s
ON CONFLICT (fueltypeid) DO UPDATE SET
    is_aggregate = EXCLUDED.is_aggregate
"""

#: MERGE staged rows into fact_generation on business key. Filters by run_id,
#: computes generation_mwh = generation_thousand_mwh * 1000, preserves null
#: and negatives. Unchanged rows are skipped via IS DISTINCT FROM.
_MERGE_FACT_SQL = """\
INSERT INTO core.fact_generation AS tgt (
    period, location, sectorid, fueltypeid,
    generation_thousand_mwh, generation_mwh,
    is_rollup_geography, is_rollup_sector, is_rollup_energy_source,
    run_id, content_hash
)
SELECT
    period, location, sectorid, fueltypeid,
    generation_thousand_mwh, generation_thousand_mwh * 1000,
    is_rollup_geography, is_rollup_sector, is_rollup_energy_source,
    run_id, content_hash
FROM raw.stg_generation
WHERE run_id = %s
ON CONFLICT (period, location, sectorid, fueltypeid) DO UPDATE SET
    generation_thousand_mwh = EXCLUDED.generation_thousand_mwh,
    generation_mwh = EXCLUDED.generation_mwh,
    is_rollup_geography = EXCLUDED.is_rollup_geography,
    is_rollup_sector = EXCLUDED.is_rollup_sector,
    is_rollup_energy_source = EXCLUDED.is_rollup_energy_source,
    run_id = EXCLUDED.run_id,
    content_hash = EXCLUDED.content_hash
    WHERE (
        tgt.run_id = EXCLUDED.run_id
        OR EXISTS (
            SELECT 1
            FROM audit.ingestion_run AS incoming
            JOIN audit.ingestion_run AS published ON published.run_id = tgt.run_id
            WHERE incoming.run_id = EXCLUDED.run_id
              AND incoming.source = published.source
              AND incoming.extracted_at > published.extracted_at
        )
    ) AND (
        tgt.generation_thousand_mwh IS DISTINCT FROM EXCLUDED.generation_thousand_mwh
        OR tgt.is_rollup_geography IS DISTINCT FROM EXCLUDED.is_rollup_geography
        OR tgt.is_rollup_sector IS DISTINCT FROM EXCLUDED.is_rollup_sector
        OR tgt.is_rollup_energy_source IS DISTINCT FROM EXCLUDED.is_rollup_energy_source
        OR tgt.run_id IS DISTINCT FROM EXCLUDED.run_id
        OR tgt.content_hash IS DISTINCT FROM EXCLUDED.content_hash
    )
"""

# One fixed lock shared by cooperating publications, including first inserts.
# This is a lock namespace, never a source-freshness or run-ordering value.
_PUBLICATION_LOCK_SQL = "SELECT pg_advisory_xact_lock(%s, %s)"
_PUBLICATION_LOCK_KEY = (1162757968, 1)

_INCOMING_AUDIT_SQL = """\
SELECT run_id::text, content_hash, source, window_start, window_end,
       extracted_at, row_count, hard_findings
FROM audit.ingestion_run
WHERE run_id = %s
FOR UPDATE
"""

_STAGED_PROVENANCE_SQL = """\
SELECT period, location, sectorid, fueltypeid, generation_thousand_mwh,
       is_rollup_geography, is_rollup_sector, is_rollup_energy_source, content_hash
FROM raw.stg_generation
WHERE run_id = %s
"""

_PUBLISHED_PROVENANCE_SQL = """\
SELECT fact.period, fact.location, fact.sectorid, fact.fueltypeid,
       fact.generation_thousand_mwh, fact.is_rollup_geography,
       fact.is_rollup_sector, fact.is_rollup_energy_source,
       fact.run_id::text, fact.content_hash,
       audit.run_id::text, audit.content_hash, audit.source,
       audit.window_start, audit.window_end, audit.extracted_at
FROM core.fact_generation AS fact
JOIN raw.stg_generation AS staged
  ON (fact.period, fact.location, fact.sectorid, fact.fueltypeid)
   = (staged.period, staged.location, staged.sectorid, staged.fueltypeid)
LEFT JOIN audit.ingestion_run AS audit ON audit.run_id = fact.run_id
WHERE staged.run_id = %s
FOR UPDATE OF fact
"""

_COUNT_STAGED_SQL = "SELECT COUNT(*) FROM raw.stg_generation WHERE run_id = %s"

_DISTINCT_PERIODS_SQL = "SELECT DISTINCT period FROM raw.stg_generation WHERE run_id = %s"

_DISTINCT_CODES_SQL = (
    "SELECT DISTINCT location, sectorid, fueltypeid FROM raw.stg_generation WHERE run_id = %s"
)

_UPDATE_AUDIT_STATUS_SQL = (
    "UPDATE audit.ingestion_run SET status = %s, error_msg = %s WHERE run_id = %s"
)


# -- Stage 3: merge result + helpers -------------------------------------------


@dataclass(frozen=True, slots=True)
class MergeResult:
    """Outcome of a core MERGE (Stage 3)."""

    run_id: str
    merged_rows: int
    partitions_created: int
    status: str  # STATUS_SUCCEEDED or STATUS_EMPTY


def _codes_to_records(
    codes: list[tuple[str, str, str]],
) -> list[GenerationRecord]:
    """Build minimal GenerationRecord objects from dimension-code tuples.

    Only the four business-key fields are populated; the publish gate
    (aggregate_publish_gate) inspects only those via unknown_codes().
    """
    return [
        GenerationRecord(
            period="",
            location=loc,
            state_description="",
            sectorid=sid,
            sector_description="",
            fueltypeid=ft,
            fuel_type_description="",
            generation=None,
            generation_units="",
        )
        for loc, sid, ft in codes
    ]


def _ensure_partitions(cur: Any, run_id: str) -> list[str]:
    """Query staging for distinct periods; create missing monthly partitions.

    Returns the list of partition names that were created.
    """
    cur.execute(_DISTINCT_PERIODS_SQL, (run_id,))
    periods = sorted(row[0] for row in cur.fetchall())
    created: list[str] = []
    for period in periods:
        pname = partition_name(period)
        cur.execute(_PARTITION_EXISTS_SQL, (pname,))
        if cur.fetchone():
            continue
        cur.execute(
            _CREATE_PARTITION_TEMPLATE.format(
                pname=pname,
                start=period,
                end=next_period(period),
            )
        )
        created.append(pname)
    return created


def _upsert_dimensions(cur: Any, run_id: str) -> None:
    """Upsert all four dimension tables from staging (idempotent)."""
    cur.execute(_DIM_DATE_SQL, (run_id,))
    cur.execute(_DIM_GEOGRAPHY_SQL, (run_id,))
    cur.execute(_DIM_SECTOR_SQL, (run_id,))
    cur.execute(_DIM_ENERGY_SOURCE_SQL, (run_id,))


def _publication_identity(row: tuple | None) -> dict[str, Any]:
    """Validate recorded provenance; timestamps order local observations only."""
    if row is None or len(row) != 6:
        raise ValidationError("missing or malformed audit provenance")
    identity = dict(
        zip(
            ("run_id", "content_hash", "source", "window_start", "window_end", "extracted_at"),
            row,
            strict=True,
        )
    )
    if isinstance(identity["extracted_at"], datetime):
        identity["extracted_at"] = identity["extracted_at"].isoformat()
    _validate_run_identity(identity)
    if identity["source"] not in (API_SOURCE_NAME, FIXTURE_SOURCE_NAME):
        raise ValidationError("ambiguous historical source cannot authorize publication")
    identity["extracted_at"] = datetime.fromisoformat(identity["extracted_at"])
    return identity


def _check_publication_precedence(
    incoming: dict[str, Any], staged_rows: list[tuple], published_rows: list[tuple]
) -> None:
    """Reject the whole batch before core writes; equal identical observations are no-ops."""
    staged = {}
    for row in staged_rows:
        if len(row) != 9 or row[8] != incoming["content_hash"]:
            raise ValidationError("staging content hash does not match incoming audit provenance")
        key = row[:4]
        period = key[0]
        if (
            not isinstance(period, str)
            or not re.fullmatch(r"[0-9]{4}-(?:0[1-9]|1[0-2])", period)
            or not incoming["window_start"] <= period <= incoming["window_end"]
            or key in staged
        ):
            raise ValidationError("staging business key is inconsistent with audit provenance")
        staged[key] = row[4:8]

    for row in published_rows:
        if len(row) != 16 or row[:4] not in staged:
            raise ValidationError("malformed published fact provenance")
        key = row[:4]
        published = _publication_identity(row[10:])
        if (
            row[8] != published["run_id"]
            or row[9] != published["content_hash"]
            or not published["window_start"] <= key[0] <= published["window_end"]
        ):
            raise ValidationError("published fact and referenced audit provenance disagree")
        if incoming["source"] != published["source"]:
            raise ValidationError("fixture and live observations cannot supersede one another")
        if incoming["run_id"] == published["run_id"]:
            if incoming != published or staged[key] != row[4:8]:
                raise ValidationError("same-run retry has inconsistent observation content")
        elif incoming["extracted_at"] < published["extracted_at"]:
            raise ValidationError("older observation cannot replace a newer published observation")
        elif incoming["extracted_at"] == published["extracted_at"] and (
            incoming["content_hash"] != published["content_hash"] or staged[key] != row[4:8]
        ):
            raise ValidationError("equal-time conflicting observations are ambiguous")
        if incoming["content_hash"] == published["content_hash"] and staged[key] != row[4:8]:
            raise ValidationError("identical content hash has inconsistent per-key observations")


def _count_staged(conn: Any, run_id: str) -> int:
    """Count rows in staging for the given run_id."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_COUNT_STAGED_SQL, (run_id,))
        return cur.fetchone()[0]


def _record_audit_status(conn: Any, run_id: str, status: str, error_msg: str | None) -> None:
    """Write audit status in a transaction block, nested when the caller owns one."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_UPDATE_AUDIT_STATUS_SQL, (status, error_msg, run_id))


def merge_to_core(conn: Any, run_id: str) -> MergeResult:
    """Publish one run with atomic precedence checks across cooperating loaders.

    The transaction-scoped advisory lock serializes publications, including
    absent fact keys. Locking the incoming audit row coordinates with staging's
    claim-before-replacement protocol. The gate, provenance decisions, core
    writes and success audit share this transaction. Failure is audited after
    rollback. Caller-owned outer transactions still control final durability.

    Equal-time distinct runs with identical content and per-key values retain
    the existing fact provenance. Newer observations advance it even when values
    are identical. ``merged_rows`` counts processed staging rows, not SQL updates.
    """
    created: list[str] = []
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(_PUBLICATION_LOCK_SQL, _PUBLICATION_LOCK_KEY)
            cur.execute(_INCOMING_AUDIT_SQL, (run_id,))
            audit_row = cur.fetchone()
            if audit_row is None or len(audit_row) != 8:
                raise ValidationError("missing or malformed incoming audit provenance")
            incoming = _publication_identity(audit_row[:6])
            expected_rows, hard_findings = audit_row[6:]
            if (
                incoming["run_id"] != run_id
                or type(expected_rows) is not int
                or expected_rows < 0
                or type(hard_findings) is not int
                or hard_findings != 0
            ):
                raise ValidationError("incoming audit identity or validation state is inconsistent")

            cur.execute(_DISTINCT_CODES_SQL, (run_id,))
            findings = aggregate_publish_gate(
                _codes_to_records(cur.fetchall()), context=f"core MERGE (run_id={run_id})"
            )
            if findings:
                raise ValidationError(
                    "core MERGE blocked: " + "; ".join(f.message for f in findings)
                )

            cur.execute(_STAGED_PROVENANCE_SQL, (run_id,))
            staged_rows = cur.fetchall()
            if len(staged_rows) != expected_rows:
                raise ValidationError("staging row count does not match incoming audit provenance")
            cur.execute(_PUBLISHED_PROVENANCE_SQL, (run_id,))
            _check_publication_precedence(incoming, staged_rows, cur.fetchall())

            created = _ensure_partitions(cur, run_id)
            _upsert_dimensions(cur, run_id)
            cur.execute(_MERGE_FACT_SQL, (run_id,))
            row_count = _count_staged(conn, run_id)
            if row_count != expected_rows:
                raise ValidationError("staging changed during publication")
            status = STATUS_SUCCEEDED if row_count else STATUS_EMPTY
            # Keep success under the locks: a delayed success must not erase a
            # later stale-replay rejection after publication releases its locks.
            _record_audit_status(conn, run_id, status, None)
    except Exception as exc:
        _record_audit_status(conn, run_id, STATUS_FAILED, str(exc))
        raise

    return MergeResult(
        run_id=run_id,
        merged_rows=row_count,
        partitions_created=len(created),
        status=status,
    )


__all__ = [
    "STATUS_EMPTY",
    "STATUS_FAILED",
    "STATUS_STAGED",
    "STATUS_SUCCEEDED",
    "MergeResult",
    "StagingResult",
    "load_raw_batch",
    "merge_to_core",
    "record_audit_run",
    "record_to_stage_row",
    "stage_from_extraction",
]
