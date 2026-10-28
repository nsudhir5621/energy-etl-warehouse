"""Unit tests for the raw zone → PostgreSQL staging loader.

Tests ``load_raw_batch`` and ``stage_from_extraction`` with an
``InMemoryObjectStore`` and a stub connection that captures SQL — no real
PostgreSQL required. The integration path (real psycopg3 connection) is
verified separately via ``scripts/checkpoint.py`` during manual verification,
not by this suite.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from energy_pipeline.errors import ValidationError
from energy_pipeline.load import (
    STATUS_FAILED,
    STATUS_STAGED,
    load_raw_batch,
    record_audit_run,
)
from energy_pipeline.records import parse_record
from energy_pipeline.storage import InMemoryObjectStore
from energy_pipeline.validation import validate

# -- Fake connection ----------------------------------------------------------


class FakeCursor:
    """Minimal cursor that records execute/executemany calls."""

    def __init__(self, log: list[tuple[str, tuple | None]], audit_rowcounts: list):
        self._log = log
        self._audit_rowcounts = audit_rowcounts
        self.rowcount = -1

    def execute(self, sql, params=None):
        self._log.append((sql, params))
        self.rowcount = -1  # Unmodeled statements do not imply successful writes.
        if sql.strip().startswith("INSERT INTO audit.ingestion_run"):
            self.rowcount = self._audit_rowcounts.pop(0) if self._audit_rowcounts else 1

    def executemany(self, sql, param_seq):
        self.rowcount = 0
        for p in param_seq:
            self._log.append((sql, p))
            self.rowcount += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    """Minimal connection that logs all cursor activity and counts transactions.

    Supports both ``with conn:`` (transaction context manager) and
    ``conn.cursor()`` (returns a cursor). All SQL + params land in
    ``executed`` for assertions.
    """

    def __init__(self, *, audit_rowcounts=()):
        self.executed: list[tuple[str, tuple | None]] = []
        self._audit_rowcounts = list(audit_rowcounts)
        self.txn_committed = 0
        self.txn_rolled_back = 0

    def cursor(self):
        return FakeCursor(self.executed, self._audit_rowcounts)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.txn_committed += 1
        else:
            self.txn_rolled_back += 1
        return False

    #: Simulate psycopg3's Connection.transaction() which returns a context
    #: manager that commits/rolls back without closing the connection.
    #: Returning self reuses __enter__/__exit__ for transaction counting.
    def transaction(self):
        return self

    # -- assertion helpers ---------------------------------------------------

    @property
    def staged_rows(self) -> list[tuple]:
        """Params from INSERT INTO raw.stg_generation calls."""
        return [
            p
            for sql, p in self.executed
            if "raw.stg_generation" in sql and sql.startswith("INSERT")
        ]

    @property
    def delete_calls(self) -> list[tuple]:
        """Params from DELETE FROM raw.stg_generation calls."""
        return [
            p
            for sql, p in self.executed
            if "raw.stg_generation" in sql and sql.startswith("DELETE")
        ]

    @property
    def audit_calls(self) -> list[tuple]:
        """Params from INSERT INTO audit.ingestion_run calls."""
        return [p for sql, p in self.executed if "audit.ingestion_run" in sql]

    def has_sql(self, fragment: str) -> bool:
        return any(fragment in sql for sql, _ in self.executed)


# -- Test data builders -------------------------------------------------------


_RUN_NS = "raw/eia/electric_power_operational_data"
_RUN_DATE = "ingestion_date=2026-10-13"
_RUN_ID = "00000000-0000-4000-8000-000000000001"
_CONTENT_HASH = hashlib.sha256(b"synthetic audit identity").hexdigest()
# Path to the manifest inside the store (used by both _make_store and tests).
_MANIFEST_PATH = f"{_RUN_NS}/{_RUN_DATE}/run_id={_RUN_ID}/manifest.json"


def _row(**kwargs) -> dict:
    """Build an API-shaped row dict with sensible defaults."""
    defaults = {
        "period": "2026-07",
        "location": "AK",
        "stateDescription": "Alaska",
        "sectorid": "1",
        "sectorDescription": "Electric Utility",
        "fueltypeid": "NG",
        "fuelTypeDescription": "Natural Gas",
        "generation": "331.840757",
        "generation-units": "thousand megawatthours",
    }
    defaults.update(kwargs)
    return defaults


def _make_store(
    rows: list[dict], *, run_id: str = _RUN_ID, content_hash: str = _CONTENT_HASH
) -> tuple[InMemoryObjectStore, str]:
    """Populate an InMemoryObjectStore with a manifest + page objects.

    Returns (store, manifest_path). All rows go in a single page.
    """
    page_path = _MANIFEST_PATH.replace("manifest.json", "page_0001.json")
    page_payload = json.dumps(rows).encode("utf-8")
    page_sha = hashlib.sha256(page_payload).hexdigest()
    manifest = {
        "run_id": run_id,
        "content_hash": content_hash,
        "source": "eia_api_v2",
        "route": "electricity/electric-power-operational-data",
        "frequency": "monthly",
        "measure": "generation",
        "window_start": "2026-07",
        "window_end": "2026-07",
        "extracted_at": "2026-10-13T00:00:00Z",
        "page_count": 1,
        "record_count": len(rows),
        "source_record_total": len(rows),
        "status": "succeeded",
        "uploaded_at": "2026-10-13T00:00:01Z",
        "objects": [
            {
                "path": page_path,
                "record_count": len(rows),
                "sha256": page_sha,
            }
        ],
    }
    store = InMemoryObjectStore()
    store.objects[_MANIFEST_PATH] = json.dumps(manifest).encode("utf-8")
    store.objects[page_path] = page_payload
    return store, _MANIFEST_PATH


# -- _read_and_parse ----------------------------------------------------------


class TestReadAndParse:
    def test_single_page_single_row(self):
        from energy_pipeline.load import _read_and_parse

        rows = [_row(generation="100.5")]
        store, manifest_path = _make_store(rows)
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        records, errors, dedupe_result = _read_and_parse(store, manifest)
        assert len(records) == 1
        assert records[0].generation == 100.5
        assert errors == []
        assert dedupe_result.identical_duplicates == 0

    def test_multi_page_all_rows_collected(self):
        from energy_pipeline.load import _read_and_parse

        page1_rows = [_row(location="AK", fueltypeid="NG")]
        page2_rows = [_row(location="TX", fueltypeid="WND")]
        page1_path = _MANIFEST_PATH.replace("manifest.json", "page_0001.json")
        page2_path = _MANIFEST_PATH.replace("manifest.json", "page_0002.json")
        page1_payload = json.dumps(page1_rows).encode("utf-8")
        page2_payload = json.dumps(page2_rows).encode("utf-8")
        manifest = {
            "run_id": _RUN_ID,
            "content_hash": _CONTENT_HASH,
            "source": "eia_api_v2",
            "route": "electricity/electric-power-operational-data",
            "frequency": "monthly",
            "measure": "generation",
            "window_start": "2026-07",
            "window_end": "2026-07",
            "extracted_at": "2026-10-13T00:00:00Z",
            "page_count": 2,
            "record_count": 2,
            "source_record_total": 2,
            "objects": [
                {
                    "path": page1_path,
                    "record_count": 1,
                    "sha256": hashlib.sha256(page1_payload).hexdigest(),
                },
                {
                    "path": page2_path,
                    "record_count": 1,
                    "sha256": hashlib.sha256(page2_payload).hexdigest(),
                },
            ],
        }
        store = InMemoryObjectStore()
        store.objects[_MANIFEST_PATH] = json.dumps(manifest).encode("utf-8")
        store.objects[page1_path] = page1_payload
        store.objects[page2_path] = page2_payload

        records, _errors, _dedupe_result = _read_and_parse(store, manifest)
        assert len(records) == 2
        assert _errors == []

    def test_identical_duplicates_collapsed(self):
        from energy_pipeline.load import _read_and_parse

        r = _row()
        store, manifest_path = _make_store([r, dict(r)])  # two identical
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        records, _errors, dedupe_result = _read_and_parse(store, manifest)
        assert len(records) == 1  # collapsed
        assert dedupe_result.identical_duplicates == 1

    def test_null_generation_preserved(self):
        from energy_pipeline.load import _read_and_parse

        store, manifest_path = _make_store([_row(fueltypeid="AOR", generation=None)])
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        records, _, _ = _read_and_parse(store, manifest)
        assert records[0].generation is None

    def test_negative_generation_preserved(self):
        from energy_pipeline.load import _read_and_parse

        store, manifest_path = _make_store([_row(fueltypeid="HPS", generation="-722.57")])
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        records, _, _ = _read_and_parse(store, manifest)
        assert records[0].generation == -722.57

    def test_valid_checksum_passes_verification(self):
        """Matching manifest SHA-256 and actual bytes → no error."""
        from energy_pipeline.load import _read_and_parse

        rows = [_row(generation="331.840757")]
        store, manifest_path = _make_store(rows)
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        records, errors, _ = _read_and_parse(store, manifest)
        assert len(records) == 1
        assert errors == []

    def test_modified_bytes_still_valid_json_fails_checksum(self):
        """Bytes changed in the store (even if still valid JSON) → mismatch."""
        from energy_pipeline.errors import ValidationError
        from energy_pipeline.load import _read_and_parse

        rows = [_row(generation="331.840757")]
        store, manifest_path = _make_store(rows)
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))

        # Replace the page bytes with different JSON (still valid, different content)
        page_path = manifest["objects"][0]["path"]
        modified_rows = [_row(generation="999.99")]
        store.objects[page_path] = json.dumps(modified_rows).encode("utf-8")

        with pytest.raises(ValidationError, match="sha256 mismatch"):
            _read_and_parse(store, manifest)

    def test_incorrect_manifest_checksum_fails(self):
        """Manifest declares wrong SHA-256 → mismatch on read."""
        from energy_pipeline.errors import ValidationError
        from energy_pipeline.load import _read_and_parse

        rows = [_row(generation="331.840757")]
        store, manifest_path = _make_store(rows)
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        manifest["objects"][0]["sha256"] = "0" * 64  # wrong hash

        with pytest.raises(ValidationError, match="sha256 mismatch"):
            _read_and_parse(store, manifest)

    def test_missing_checksum_in_manifest_fails(self):
        """Manifest object without sha256 → ValidationError before reading."""
        from energy_pipeline.errors import ValidationError
        from energy_pipeline.load import _read_and_parse

        rows = [_row(generation="331.840757")]
        store, manifest_path = _make_store(rows)
        manifest = json.loads(store.read(manifest_path).decode("utf-8"))
        del manifest["objects"][0]["sha256"]

        with pytest.raises(ValidationError, match="missing its sha256"):
            _read_and_parse(store, manifest)


# -- load_raw_batch ------------------------------------------------------------


class TestLoadRawBatch:
    def test_happy_path_stages_all_rows(self):
        rows = [
            _row(location="AK", fueltypeid="NG", generation="331.840757"),
            _row(location="TX", fueltypeid="NG", generation="-100.0"),
            _row(location="AK", fueltypeid="AOR", generation=None),
            _row(location="US", fueltypeid="ALL", generation="9999.0"),
        ]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        result = load_raw_batch(store, conn, manifest_path)

        assert result.run_id == _RUN_ID
        assert result.content_hash == _CONTENT_HASH
        assert result.staged_rows == 4
        assert result.status == STATUS_STAGED
        assert result.parse_errors == ()
        assert result.conflicting_keys == ()

        # One DELETE for idempotency, one INSERT per staged row
        assert len(conn.delete_calls) == 1
        assert conn.delete_calls[0] == (_RUN_ID,)
        assert len(conn.staged_rows) == 4

        # One audit INSERT
        assert len(conn.audit_calls) == 1
        audit = conn.audit_calls[0]
        assert audit[0] == _RUN_ID  # run_id
        assert audit[1] == _CONTENT_HASH  # content_hash
        assert audit[6] == 4  # row_count
        assert audit[7] == STATUS_STAGED  # status

    def test_null_zero_negative_in_staged_rows(self):
        """Null ≠ zero, negatives preserved — all visible in the staged row tuples."""
        rows = [
            _row(location="AK", fueltypeid="NG", generation="594.57104"),  # positive
            _row(location="AK", fueltypeid="AOR", generation=None),  # null
            _row(
                location="AK", sectorid="2", fueltypeid="NG", generation="0"
            ),  # zero, distinct key
            _row(location="TX", fueltypeid="HPS", generation="-2335"),  # negative
        ]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        load_raw_batch(store, conn, manifest_path)

        staged = conn.staged_rows
        # generation_thousand_mwh is at index 6 in the row tuple
        gen_values = [r[6] for r in staged]
        assert 594.57104 in gen_values  # positive preserved
        assert None in gen_values  # null preserved (not 0)
        assert 0.0 in gen_values  # zero is 0.0, not None
        assert -2335.0 in gen_values  # negative preserved

    def test_rollup_flags_in_staged_rows(self):
        """is_rollup_g/e/s flags are correctly set on staged rows."""
        rows = [
            _row(location="US", fueltypeid="ALL"),  # both geo + energy rollup
            _row(location="AK", fueltypeid="NG"),  # all leaf
        ]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()
        load_raw_batch(store, conn, manifest_path)

        staged = conn.staged_rows
        # (is_rollup_geography, is_rollup_sector, is_rollup_energy_source) at indices 7,8,9
        # US row
        assert staged[0][7] is True  # geo rollup
        assert staged[0][9] is True  # energy source rollup
        # AK/NG row
        assert staged[1][7] is False
        assert staged[1][9] is False

    def test_idempotent_retry_deletes_before_insert(self):
        """Re-loading the same run_id: DELETE first, then INSERT (not duplicate)."""
        rows = [_row(location="AK", fueltypeid="NG")]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        load_raw_batch(store, conn, manifest_path)
        conn.executed.clear()  # reset log
        # Re-run same load (simulates retry with same run_id)
        load_raw_batch(store, conn, manifest_path)

        # Should still have DELETE + INSERT, not accumulated duplicates
        assert len(conn.delete_calls) == 1
        assert len(conn.staged_rows) == 1
        audit_index = next(i for i, (sql, _) in enumerate(conn.executed) if "audit" in sql)
        delete_index = next(
            i for i, (sql, _) in enumerate(conn.executed) if sql.startswith("DELETE")
        )
        insert_index = next(
            i for i, (sql, _) in enumerate(conn.executed) if sql.startswith("INSERT INTO raw.")
        )
        assert audit_index < delete_index < insert_index

    def test_empty_batch_stages_zero_rows(self):
        """A manifest with 0 objects → 0 staged rows, audit still recorded."""
        manifest = {
            "run_id": _RUN_ID,
            "content_hash": _CONTENT_HASH,
            "source": "eia_api_v2",
            "route": "electricity/electric-power-operational-data",
            "frequency": "monthly",
            "measure": "generation",
            "window_start": "2026-07",
            "window_end": "2026-07",
            "extracted_at": "2026-10-13T00:00:00Z",
            "page_count": 0,
            "record_count": 0,
            "source_record_total": 0,
            "objects": [],
        }
        store = InMemoryObjectStore()
        store.objects[_MANIFEST_PATH] = json.dumps(manifest).encode("utf-8")
        conn = FakeConn()

        result = load_raw_batch(store, conn, _MANIFEST_PATH)
        assert result.staged_rows == 0
        assert len(conn.staged_rows) == 0
        assert len(conn.audit_calls) == 1

    def test_hard_finding_aborts_staging(self):
        """Conflicting duplicate keys → HARD finding → abort, audit='failed'."""
        # Same key, different values → dedupe reports conflict → validate adds HARD
        rows = [
            _row(location="AK", fueltypeid="NG", generation="100.0"),
            _row(location="AK", fueltypeid="NG", generation="200.0"),  # same key, diff value
        ]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        with pytest.raises(Exception, match="staging aborted"):
            load_raw_batch(store, conn, manifest_path)

        # No staging INSERTs happened
        assert len(conn.staged_rows) == 0
        # But audit was recorded with status='failed'
        assert len(conn.audit_calls) == 1
        assert conn.audit_calls[0][7] == STATUS_FAILED  # status column

    def test_missing_manifest_field_raises(self):
        """A manifest without run_id or content_hash → ValidationError."""
        store = InMemoryObjectStore()
        bad_manifest = {"objects": []}
        store.objects[_MANIFEST_PATH] = json.dumps(bad_manifest).encode("utf-8")
        conn = FakeConn()

        with pytest.raises(Exception, match="manifest missing"):
            load_raw_batch(store, conn, _MANIFEST_PATH)

    def test_identical_duplicates_collapsed_during_load(self):
        """Two identical rows → one staged row, dedupe count tracked."""
        r = _row()
        store, manifest_path = _make_store([r, dict(r)])
        conn = FakeConn()

        result = load_raw_batch(store, conn, manifest_path)
        assert result.staged_rows == 1
        assert result.identical_duplicates == 1
        assert len(conn.staged_rows) == 1

    def test_connection_not_closed_between_staging_and_audit(self):
        """Regression: psycopg3 3.3.x closes the connection on ``with conn:`` exit.

        ``load_raw_batch`` stages rows in one transaction then records the audit
        row in a separate transaction. If the staging ``with conn:`` block closes
        the connection (as psycopg3 3.3.x does), the audit UPSERT fails with
        ``OperationalError: the connection is closed``.

        This test asserts that ``conn.transaction()`` is used (not ``conn`` directly
        as a context manager), so the connection survives across both transactions.
        """
        rows = [_row(location="AK", fueltypeid="NG")]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        load_raw_batch(store, conn, manifest_path)

        # Staging transaction committed
        assert conn.txn_committed >= 1
        # Audit transaction also committed — connection was NOT closed
        assert conn.txn_committed >= 2
        # Audit row was recorded
        assert len(conn.audit_calls) == 1

    def test_manifest_fields_pass_to_audit(self):
        """Audit row receives run_id, content_hash, source, window, extracted_at."""
        rows = [_row()]
        store, manifest_path = _make_store(rows)
        conn = FakeConn()

        load_raw_batch(store, conn, manifest_path)

        audit = conn.audit_calls[0]
        assert audit[0] == _RUN_ID  # run_id
        assert audit[1] == _CONTENT_HASH  # content_hash
        assert audit[2] == "eia_api_v2"  # source
        assert audit[3] == "2026-07"  # window_start
        assert audit[4] == "2026-07"  # window_end
        assert audit[5] == "2026-10-13T00:00:00Z"  # extracted_at


# -- record_audit_run (standalone) ---------------------------------------------


class TestRecordAuditRun:
    def test_audit_upsert_recorded(self):
        from energy_pipeline.validation import validate

        record = parse_record(_row(generation="100.0"))
        report = validate([record])
        manifest = {
            "run_id": _RUN_ID,
            "content_hash": _CONTENT_HASH,
            "source": "eia_api_v2",
            "window_start": "2026-07",
            "window_end": "2026-07",
            "extracted_at": "2026-10-13T00:00:00Z",
        }
        conn = FakeConn()
        record_audit_run(conn, manifest, report, status=STATUS_STAGED)

        assert len(conn.audit_calls) == 1
        audit = conn.audit_calls[0]
        assert audit[0] == _RUN_ID
        assert audit[6] == 1  # row_count
        assert audit[7] == STATUS_STAGED  # status
        # hard=0, warn=0, info=4 (nulls, negatives, rollups, unknowns)
        assert audit[8] == 0  # hard
        assert audit[9] == 0  # warn (no unknown codes in this record)
        assert audit[10] >= 1  # info (at least null/negative/rollup observations)
        assert audit[11] is None  # error_msg

    def test_audit_failed_status_and_error_msg(self):

        report = validate([])  # empty report, no findings
        manifest = {
            "run_id": _RUN_ID,
            "content_hash": _CONTENT_HASH,
            "source": "eia_api_v2",
            "window_start": "2026-07",
            "window_end": "2026-07",
            "extracted_at": "2026-10-13T00:00:00Z",
        }
        conn = FakeConn()
        record_audit_run(
            conn,
            manifest,
            report,
            status=STATUS_FAILED,
            error_msg="bad data",
        )
        assert conn.audit_calls[0][7] == STATUS_FAILED
        assert conn.audit_calls[0][11] == "bad data"


# -- immutable observation provenance -----------------------------------------


class TestAuditProvenanceSafety:
    @pytest.mark.parametrize(
        "field", ["content_hash", "source", "window_start", "window_end", "extracted_at"]
    )
    def test_same_run_update_is_conditional_on_unchanged_provenance(self, field):
        """Specify a guarded UPSERT contract, not a simulation of PostgreSQL conflicts.

        Actual altered-value rejection and concurrent claims of the same run ID
        require PostgreSQL verification of the predicate and affected-row check.
        """
        from energy_pipeline.load import _AUDIT_UPSERT

        conflict = " ".join(_AUDIT_UPSERT.lower().split()).split("on conflict", 1)[1]
        _, _, guard = conflict.partition(" where ")
        assert guard, "an accepted run ID must not unconditionally accept new provenance"
        references = re.findall(rf"\b(?:\w+\.)?{field}\b", guard)
        assert f"excluded.{field}" in references
        assert any(reference != f"excluded.{field}" for reference in references), (
            f"retry guard must compare accepted and incoming {field}"
        )

    def test_unchanged_same_run_retry_preserves_audit_identity(self):
        """Both accepted UPSERT requests carry identical immutable metadata."""
        store, path = _make_store([_row()])
        manifest = json.loads(store.read(path))
        report = validate([parse_record(_row())])
        conn = FakeConn(audit_rowcounts=[1, 1])

        record_audit_run(conn, manifest, report)
        record_audit_run(conn, dict(manifest), report)

        assert len(conn.audit_calls) == 2
        assert conn.audit_calls[0][:6] == conn.audit_calls[1][:6]
        assert conn.txn_rolled_back == 0

    @pytest.mark.parametrize("accepted_rows", [0, -1, None, 2])
    def test_rejected_audit_upsert_is_an_explicit_failure(self, accepted_rows):
        """A conditional UPSERT accepting zero rows must not look like a successful retry."""
        store, path = _make_store([_row()])
        manifest = json.loads(store.read(path))
        report = validate([parse_record(_row())])
        conn = FakeConn(audit_rowcounts=[accepted_rows])

        with pytest.raises(ValidationError):
            record_audit_run(conn, manifest, report)

        assert conn.txn_committed == 0
        assert conn.txn_rolled_back == 1

    def test_absent_audit_acceptance_evidence_is_rejected(self, monkeypatch):
        store, path = _make_store([_row()])
        manifest = json.loads(store.read(path))
        conn = FakeConn()
        cursor = conn.cursor()
        execute = cursor.execute

        def execute_without_rowcount(sql, params=None):
            execute(sql, params)
            del cursor.rowcount

        monkeypatch.setattr(cursor, "execute", execute_without_rowcount)
        monkeypatch.setattr(conn, "cursor", lambda: cursor)

        with pytest.raises(ValidationError, match="unconfirmed"):
            record_audit_run(conn, manifest, validate([parse_record(_row())]))

        assert conn.txn_committed == 0
        assert conn.txn_rolled_back == 1

    @pytest.mark.parametrize(
        "field, value",
        [
            ("content_hash", None),
            ("content_hash", "short"),
            ("content_hash", "g" * 64),
            ("run_id", "not-a-uuid"),
            ("source", None),
            ("source", "unrecognized-source"),
            ("window_start", "2026-08"),  # After the existing window_end.
            ("window_end", "2026-13"),
            ("extracted_at", None),
            ("extracted_at", ""),
            ("extracted_at", "not-a-time"),
            ("extracted_at", "2026-10-13T00:00:00"),  # Timezone is required.
            ("extracted_at", "2026-02-30T00:00:00Z"),
            ("extracted_at", "2026-10-13T00:00:00.1234567Z"),
        ],
    )
    def test_missing_or_invalid_observation_metadata_is_rejected(self, field, value):
        store, path = _make_store([_row()])
        manifest = json.loads(store.read(path))
        manifest[field] = value
        conn = FakeConn()

        with pytest.raises(ValidationError):
            record_audit_run(conn, manifest, validate([parse_record(_row())]))

        assert conn.audit_calls == []

    @pytest.mark.parametrize("via_raw", [True, False], ids=["raw", "extraction"])
    def test_identity_conflict_prevents_replacing_previously_staged_rows(
        self, monthly_fixture, via_raw
    ):
        """Inject database rejection; require no staging writes after that rejection."""
        from dataclasses import replace

        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        conn = FakeConn(audit_rowcounts=[1, 0])
        if via_raw:
            store, path = _make_store([_row()])
            load_raw_batch(store, conn, path)
            manifest = json.loads(store.read(path))
            manifest["content_hash"] = hashlib.sha256(b"conflicting content").hexdigest()
            store.objects[path] = json.dumps(manifest).encode("utf-8")
        else:
            result = extract_fixture(monthly_fixture)
            stage_from_extraction(conn, result)
            result.batch = replace(result.batch, extracted_at="2026-10-14T00:00:00Z")
        committed_before = conn.txn_committed
        conn.executed.clear()

        with pytest.raises(ValidationError, match="audit identity claim"):
            if via_raw:
                load_raw_batch(store, conn, path)
            else:
                stage_from_extraction(conn, result)

        assert conn.delete_calls == []
        assert conn.staged_rows == []
        assert len(conn.audit_calls) == 1  # Only the rejected claim, no status rewrite.
        assert conn.txn_committed == committed_before
        assert conn.txn_rolled_back >= 1

    @pytest.mark.parametrize("via_raw", [True, False], ids=["raw", "extraction"])
    @pytest.mark.parametrize(
        "field, value",
        [
            ("route", "electricity/another-route"),
            ("frequency", "annual"),
            ("measure", "capacity"),
            ("route", None),
        ],
    )
    def test_unsupported_dataset_is_rejected_before_database_work(
        self, monthly_fixture, via_raw, field, value
    ):
        from dataclasses import replace

        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        conn = FakeConn()
        if via_raw:
            store, path = _make_store([_row()])
            manifest = json.loads(store.read(path))
            manifest[field] = value
            store.objects[path] = json.dumps(manifest).encode("utf-8")
            with pytest.raises(ValidationError, match=field):
                load_raw_batch(store, conn, path)
        else:
            result = extract_fixture(monthly_fixture)
            result.batch = replace(result.batch, **{field: value})
            with pytest.raises(ValidationError, match=field):
                stage_from_extraction(conn, result)

        assert conn.executed == []

    def test_fixture_and_api_observations_have_distinct_source_provenance(self, monthly_fixture):
        """Identical bytes/timestamps cannot turn a fixture into a trusted live observation."""
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import (
            SOURCE_NAME,
            ApiSource,
            FixtureSource,
            PeriodWindow,
            extract,
            extract_fixture,
        )

        settings = Settings(api_key=None, page_size=5000)
        window = PeriodWindow("2026-07", "2026-08")
        observed_at = "2026-10-13T00:00:00Z"
        fixture_source = FixtureSource(monthly_fixture)

        class OfflineClient:
            def get(self, params):
                return fixture_source.fetch_page(params["offset"], params["length"])

        fixture = extract_fixture(
            monthly_fixture, settings, window=window, extracted_at=observed_at
        )
        api = extract(
            ApiSource(OfflineClient(), settings, window),
            settings,
            window,
            extracted_at=observed_at,
        )

        assert fixture_source.requests  # The API path used only the in-memory client.
        assert api.batch.content_hash == fixture.batch.content_hash
        assert api.batch.extracted_at == fixture.batch.extracted_at
        assert api.batch.source != fixture.batch.source
        assert SOURCE_NAME not in (api.batch.source, fixture.batch.source)

    def test_custom_source_keeps_ambiguous_origin_and_original_timestamp(self, monthly_fixture):
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import SOURCE_NAME, FixtureSource, PeriodWindow, extract

        fixture = FixtureSource(monthly_fixture)

        class CustomSource:
            def fetch_page(self, offset, length):
                return fixture.fetch_page(offset, length)

        observed_at = "2026-09-30T00:00:00Z"
        result = extract(
            CustomSource(),
            Settings(api_key=None),
            PeriodWindow("2026-07", "2026-08"),
            extracted_at=observed_at,
        )

        assert result.batch.source == SOURCE_NAME
        assert result.batch.extracted_at == observed_at


# -- stage_from_extraction -----------------------------------------------------


class TestStageFromExtraction:
    @pytest.mark.parametrize(
        "invalid_fields",
        [
            pytest.param({"generation": float("nan")}, id="nan"),
            pytest.param({"generation": float("inf")}, id="infinity"),
            pytest.param({"generation": -float("inf")}, id="negative-infinity"),
            pytest.param({"generation": "1e309"}, id="overflow-string"),
            pytest.param({"generation": True}, id="boolean"),
            pytest.param({"generation": "not-a-number"}, id="malformed-number"),
            pytest.param({"generation_units": "megawatthours"}, id="unsupported-unit"),
            pytest.param({"generation_units": ""}, id="empty-unit"),
            pytest.param({"generation_units": None}, id="null-unit"),
        ],
    )
    def test_invalid_record_with_stale_ok_report_is_rejected_before_writes(
        self, edge_cases_fixture: Path, invalid_fields
    ):
        """The staging mapper must check actual records, not just a cached report."""
        from dataclasses import replace

        from energy_pipeline.config import Settings
        from energy_pipeline.errors import ValidationError
        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        result = extract_fixture(
            edge_cases_fixture,
            Settings(api_key=None, page_size=5000),
            extracted_at="2026-10-13T00:00:00Z",
        )
        cached_report = result.report
        assert cached_report.ok
        result.merge_rows[0] = replace(result.merge_rows[0], **invalid_fields)
        assert result.report is cached_report
        assert result.report.ok  # Deliberately stale: the bad record was never validated.
        conn = FakeConn()

        with pytest.raises(ValidationError, match="generation"):
            stage_from_extraction(conn, result)

        assert conn.executed == []
        assert conn.txn_committed == 0
        assert conn.txn_rolled_back == 0

    def test_explicit_manifest(self, edge_cases_fixture: Path):
        """Stage from a real ExtractionResult using the edge_cases fixture.

        The edge_cases fixture covers null, zero, negative, rollup geography,
        rollup energy source, and unknown codes in one batch.
        """
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import extract_fixture

        settings = Settings(api_key=None, page_size=5000)
        result = extract_fixture(
            edge_cases_fixture,
            settings,
            extracted_at="2026-10-13T00:00:00Z",
        )
        conn = FakeConn()

        from energy_pipeline.load import stage_from_extraction

        staging_result = stage_from_extraction(conn, result)

        assert staging_result.staged_rows == len(result.merge_rows)
        assert staging_result.run_id == result.batch.run_id
        assert staging_result.content_hash == result.batch.content_hash
        assert staging_result.status == STATUS_STAGED

        # One DELETE + N INSERT in staging transaction
        assert len(conn.delete_calls) == 1
        assert conn.delete_calls[0] == (result.batch.run_id,)
        assert len(conn.staged_rows) == len(result.merge_rows)

        # One audit UPSERT
        assert len(conn.audit_calls) == 1

    def test_null_zero_negative_preserved_via_extraction(self, edge_cases_fixture: Path):
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        settings = Settings(api_key=None, page_size=5000)
        result = extract_fixture(
            edge_cases_fixture,
            settings,
            extracted_at="2026-10-13T00:00:00Z",
        )
        conn = FakeConn()
        stage_from_extraction(conn, result)

        staged = conn.staged_rows
        gen_values = [r[6] for r in staged]
        assert None in gen_values  # null preserved (AOR)
        assert 0.0 in gen_values  # zero preserved (SUN 0)
        assert any(v < 0 for v in gen_values if v is not None)  # negative preserved (HPS)

    def test_rollup_flags_via_extraction(self, edge_cases_fixture: Path):
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        settings = Settings(api_key=None, page_size=5000)
        result = extract_fixture(
            edge_cases_fixture,
            settings,
            extracted_at="2026-10-13T00:00:00Z",
        )
        conn = FakeConn()
        stage_from_extraction(conn, result)

        staged = conn.staged_rows
        # Find the US/ALL row → both geo and energy source rollup
        for row in staged:
            if row[2] == "US" and row[5] == "ALL":
                assert row[7] is True  # is_rollup_geography
                assert row[9] is True  # is_rollup_energy_source
            elif row[2] == "NEW":
                assert row[7] is True  # census division rollup
            elif row[2] == "PCC":
                assert row[7] is True  # Pacific API spelling

    def test_no_transaction_rollback_on_success(self, edge_cases_fixture: Path):
        from energy_pipeline.config import Settings
        from energy_pipeline.extract import extract_fixture
        from energy_pipeline.load import stage_from_extraction

        settings = Settings(api_key=None, page_size=5000)
        result = extract_fixture(
            edge_cases_fixture,
            settings,
            extracted_at="2026-10-13T00:00:00Z",
        )
        conn = FakeConn()
        stage_from_extraction(conn, result)

        # The fake counts the outer staging context and nested audit savepoint.
        assert conn.txn_committed >= 2
        assert conn.txn_rolled_back == 0
