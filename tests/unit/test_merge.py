"""Unit tests for Phase 3 Stage 3: staging → core MERGE.

Tests ``merge_to_core`` and its helpers with a stub connection that captures
SQL and returns pre-configured query results — no real PostgreSQL required.
The integration path (real psycopg3 connection) is verified separately via
scripts/checkpoint.py during manual verification, not by this suite.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta

import pytest

from energy_pipeline.errors import ValidationError
from energy_pipeline.extract import API_SOURCE_NAME, FIXTURE_SOURCE_NAME, SOURCE_NAME
from energy_pipeline.load import (
    _INCOMING_AUDIT_SQL,
    _PUBLICATION_LOCK_SQL,
    _PUBLISHED_PROVENANCE_SQL,
    _STAGED_PROVENANCE_SQL,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    MergeResult,
    merge_to_core,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"
OTHER_RUN_ID = "00000000-0000-4000-8000-000000000002"
THIRD_RUN_ID = "00000000-0000-4000-8000-000000000003"
CONTENT_HASH = hashlib.sha256(b"observation").hexdigest()
REVISED_HASH = hashlib.sha256(b"revised observation").hexdigest()
OBSERVED_AT = datetime(2026, 10, 13, tzinfo=UTC)


def _audit_row(run_id=RUN_ID, *, content_hash=CONTENT_HASH, source=API_SOURCE_NAME, at=OBSERVED_AT):
    return (run_id, content_hash, source, "2026-07", "2026-07", at)


def _staged_row(codes=("AK", "1", "NG"), *, generation=1.25, content_hash=CONTENT_HASH):
    location, sector, fuel = codes
    return (
        "2026-07",
        location,
        sector,
        fuel,
        generation,
        location in ("US", "NEW"),
        False,
        fuel in ("ALL", "AOR"),
        content_hash,
    )


def _published_row(staged, audit):
    return (*staged[:8], audit[0], audit[1], *audit)


# Known leaf / rollup codes (all classify successfully)
KNOWN_LEAF_CODES = [("AK", "1", "NG"), ("TX", "7", "WND")]
KNOWN_ROLLUP_CODES = [("US", "1", "ALL"), ("NEW", "1", "AOR")]
# Unknown codes (would block the publish gate)
UNKNOWN_CODES = [("ZZ", "8", "BIO")]


class FakeCursor:
    """Cursor that records SQL and returns pre-configured query results.

    Query results are consumed in FIFO order: each ``fetchone()`` returns the
    next item from the fetchone queue, each ``fetchall()`` returns the next
    item from the fetchall queue.
    """

    def __init__(self, log: list[tuple[str, tuple | None]]):
        self._log = log
        self._fetchall_queue: list = []
        self._fetchone_queue: list = []
        self.rowcount = 0
        self._last_sql = ""
        self.incoming_audit = None
        self.staged_provenance: list[tuple] = []
        self.published_provenance: list[tuple] = []

    def execute(self, sql, params=None):
        self._log.append((sql, params))
        self._last_sql = sql

    def executemany(self, sql, param_seq):
        for p in param_seq:
            self._log.append((sql, p))
        self.rowcount = len(param_seq)

    def fetchone(self):
        if self._last_sql == _INCOMING_AUDIT_SQL:
            return self.incoming_audit
        if self._fetchone_queue:
            return self._fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self._last_sql == _STAGED_PROVENANCE_SQL:
            return self.staged_provenance
        if self._last_sql == _PUBLISHED_PROVENANCE_SQL:
            return self.published_provenance
        if self._fetchall_queue:
            return self._fetchall_queue.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    """Connection that shares a single cursor and tracks transactions."""

    def __init__(self):
        self.executed: list[tuple[str, tuple | None]] = []
        self._cursor = FakeCursor(self.executed)
        self.txn_committed = 0
        self.txn_rolled_back = 0
        self.transaction_depth = 0

    @property
    def cursor_obj(self) -> FakeCursor:
        """The shared cursor — tests pre-configure results here."""
        return self._cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def __enter__(self):
        self.transaction_depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.txn_committed += 1
        else:
            self.txn_rolled_back += 1
        self.transaction_depth -= 1
        return False

    #: Simulate psycopg3's Connection.transaction() — same counting semantics as
    #: __enter__/__exit__ but does not close the connection.
    def transaction(self):
        return self

    # -- assertion helpers ---------------------------------------------------

    @property
    def merge_sql(self) -> list[str]:
        return [
            sql
            for sql, _ in self.executed
            if sql.strip().startswith("INSERT INTO core.fact_generation")
        ]

    @property
    def audit_updates(self) -> list[tuple]:
        return [p for sql, p in self.executed if sql.strip().startswith("UPDATE audit")]

    @property
    def create_partition_sql(self) -> list[str]:
        return [
            sql for sql, _ in self.executed if "CREATE TABLE core." in sql and "PARTITION OF" in sql
        ]

    @property
    def dimension_upserts(self) -> list[str]:
        return [sql for sql, _ in self.executed if sql.strip().startswith("INSERT INTO core.dim_")]

    def has_sql(self, fragment: str) -> bool:
        return any(fragment in sql for sql, _ in self.executed)


def _configure(conn, fetchall_results=None, fetchone_results=None):
    """Pre-load a FakeConn's cursor with query-result queues."""
    if fetchall_results is not None:
        conn.cursor_obj._fetchall_queue = list(fetchall_results)
        # Explicit provenance fixtures for the existing dimension-code scenarios.
        # These are query responses, not a SQL evaluator or stored database state.
        codes = fetchall_results[0] if fetchall_results else []
        conn.cursor_obj.staged_provenance = [_staged_row(row) for row in codes if len(row) == 3]
        conn.cursor_obj.incoming_audit = (*_audit_row(), len(conn.cursor_obj.staged_provenance), 0)
    if fetchone_results is not None:
        conn.cursor_obj._fetchone_queue = list(fetchone_results)
    return conn


# -- _ensure_partitions ------------------------------------------------------


class TestEnsurePartitions:
    def test_creates_partitions_for_new_periods(self):
        from energy_pipeline.load import _ensure_partitions

        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [("2026-07",), ("2026-08",)],  # distinct periods
            ],
            fetchone_results=[None, None],
        )  # both partitions absent

        cur = conn.cursor()
        created = _ensure_partitions(cur, RUN_ID)

        assert created == ["fact_generation_2026_07", "fact_generation_2026_08"]
        assert len(conn.create_partition_sql) == 2

    def test_skips_existing_partitions(self):
        from energy_pipeline.load import _ensure_partitions

        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [("2026-07",)],  # period
            ],
            fetchone_results=[(1,)],
        )  # partition exists

        cur = conn.cursor()
        created = _ensure_partitions(cur, RUN_ID)

        assert created == []
        assert len(conn.create_partition_sql) == 0

    def test_partition_sql_uses_text_bounds(self):
        """Partition bounds must be TEXT literals, not DATE literals.

        core.fact_generation.period is TEXT, so PostgreSQL casts partition
        bounds to TEXT. DATE '2026-06-01' becomes '2026-06-01'::text which
        sorts AFTER '2026-06' lexicographically — causing rows with period
        '2026-06' to not match any partition. TEXT bounds ('2026-06') fix this.
        """
        from energy_pipeline.load import _ensure_partitions

        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [("2026-06",)],
            ],
            fetchone_results=[None],
        )

        cur = conn.cursor()
        _ensure_partitions(cur, RUN_ID)

        assert len(conn.create_partition_sql) == 1
        sql = conn.create_partition_sql[0]
        # Must use TEXT literals, not DATE literals
        assert "DATE" not in sql
        assert "FROM ('2026-06')" in sql
        assert "TO ('2026-07')" in sql  # next_period("2026-06") = "2026-07"

    def test_multiple_periods_all_partitions_created(self):
        """All distinct periods in staging get partitions created."""
        from energy_pipeline.load import _ensure_partitions

        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [("2026-06",), ("2026-07",), ("2026-08",)],
            ],
            fetchone_results=[None, None, None],
        )

        cur = conn.cursor()
        created = _ensure_partitions(cur, RUN_ID)

        assert created == [
            "fact_generation_2026_06",
            "fact_generation_2026_07",
            "fact_generation_2026_08",
        ]
        assert len(conn.create_partition_sql) == 3

    def test_partition_sql_no_bind_params(self):
        """Generated partition DDL must not use %s bind parameters, which
        PostgreSQL does not support in DDL FOR VALUES clauses. The SQL should
        inline TEXT 'YYYY-MM' literals for the partition bounds (matching the
        TEXT partition key type)."""
        from energy_pipeline.load import _ensure_partitions

        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [("2026-07",)],
            ],
            fetchone_results=[None],
        )

        cur = conn.cursor()
        _ensure_partitions(cur, RUN_ID)

        assert len(conn.create_partition_sql) == 1
        sql = conn.create_partition_sql[0]
        # Must not contain bind parameter placeholders
        assert "%s" not in sql
        # Must use TEXT 'YYYY-MM' literals for the partition bounds (not DATE)
        assert "DATE" not in sql
        assert "FROM ('2026-07')" in sql
        assert "TO ('2026-08')" in sql  # next_period("2026-07") = "2026-08"
        # Must be a valid CREATE TABLE ... PARTITION OF statement
        assert "CREATE TABLE core.fact_generation_2026_07" in sql
        assert "PARTITION OF core.fact_generation" in sql
        assert "FOR VALUES FROM" in sql


# -- publish gate ------------------------------------------------------------


class TestPublishGate:
    def test_unknown_codes_block_merge(self):
        """Unknown dimension codes block merge; audit set to failed."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(UNKNOWN_CODES[0])],  # distinct codes with unknown
            ],
            fetchone_results=[],
        )  # not reached

        with pytest.raises(ValidationError, match="core MERGE blocked"):
            merge_to_core(conn, RUN_ID)

        assert conn.audit_updates
        assert conn.audit_updates[0][0] == STATUS_FAILED  # status column

    def test_known_codes_pass_gate(self):
        """All leaf/rollup codes → publish gate passes, proceeds to MERGE."""
        all_codes = KNOWN_LEAF_CODES + KNOWN_ROLLUP_CODES
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in all_codes],  # distinct codes (all known)
                [("2026-07",)],  # distinct periods
            ],
            fetchone_results=[
                None,  # partition doesn't exist → create
                (4,),  # one staged row for each distinct code combination
            ],
        )

        result = merge_to_core(conn, RUN_ID)

        assert result.status == STATUS_SUCCEEDED
        assert conn.audit_updates[-1][0] == STATUS_SUCCEEDED

    def test_empty_staging_passes_gate(self):
        """No codes at all → gate trivially passes."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [],  # no distinct codes
                [],  # no distinct periods
            ],
            fetchone_results=[
                (0,),  # staged count = 0
            ],
        )

        result = merge_to_core(conn, RUN_ID)

        assert result.status == STATUS_EMPTY


# -- dimension upserts -------------------------------------------------------


class TestDimensionUpserts:
    def test_all_four_dimensions_upserted(self):
        """Merge executes INSERT for dim_date, dim_geography, dim_sector, dim_energy_source."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],  # codes
                [("2026-07",)],  # periods
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        upsert_names = [sql.split("(")[0] for sql in conn.dimension_upserts]
        assert any("dim_date" in s for s in upsert_names)
        assert any("dim_geography" in s for s in upsert_names)
        assert any("dim_sector" in s for s in upsert_names)
        assert any("dim_energy_source" in s for s in upsert_names)

    def test_geography_upsert_uses_is_rollup_flag(self):
        """dim_geography upsert writes is_rollup from staging's is_rollup_geography."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_ROLLUP_CODES],  # includes US/ALL rollup
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        geo_sql = [s for s in conn.dimension_upserts if "dim_geography" in s]
        assert len(geo_sql) == 1


# -- fact MERGE --------------------------------------------------------------


class TestMergeFactSQL:
    def test_merge_uses_insert_on_conflict_not_merge_syntax(self):
        """_MERGE_FACT_SQL must use INSERT ... ON CONFLICT, not MERGE INTO.

        PostgreSQL 15's MERGE does not support WHERE on WHEN MATCHED THEN
        UPDATE SET, so the project uses INSERT ... ON CONFLICT ... DO UPDATE
        instead."""
        from energy_pipeline.load import _MERGE_FACT_SQL

        assert _MERGE_FACT_SQL.strip().startswith("INSERT INTO core.fact_generation")
        assert "MERGE INTO" not in _MERGE_FACT_SQL
        assert "ON CONFLICT (period, location, sectorid, fueltypeid)" in _MERGE_FACT_SQL
        assert "DO UPDATE SET" in _MERGE_FACT_SQL
        assert "WHERE" in _MERGE_FACT_SQL  # IS DISTINCT FROM guard

    def test_merge_targets_correct_business_key(self):
        """ON CONFLICT clause uses (period, location, sectorid, fueltypeid)."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        assert len(conn.merge_sql) == 1
        sql = conn.merge_sql[0]
        assert "ON CONFLICT (period, location, sectorid, fueltypeid)" in sql

    def test_merge_computes_generation_mwh(self):
        """generation_mwh = generation_thousand_mwh * 1000 in both INSERT and UPDATE."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        sql = conn.merge_sql[0]
        assert "generation_thousand_mwh * 1000" in sql

    def test_merge_filters_by_run_id(self):
        """SELECT filters stg_generation by run_id via WHERE."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        sql = conn.merge_sql[0]
        assert "raw.stg_generation" in sql
        assert "WHERE run_id = %s" in sql

    def test_merge_skips_unchanged_rows(self):
        """DO UPDATE has IS DISTINCT FROM guard — unchanged rows skipped."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        merge_to_core(conn, RUN_ID)

        sql = conn.merge_sql[0]
        assert "IS DISTINCT FROM" in sql


# -- end-to-end --------------------------------------------------------------


class TestMergeToCore:
    def test_happy_path_returns_succeeded(self):
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        result = merge_to_core(conn, RUN_ID)

        assert isinstance(result, MergeResult)
        assert result.run_id == RUN_ID
        assert result.merged_rows == 2
        assert result.partitions_created == 1
        assert result.status == STATUS_SUCCEEDED
        # Staging tx + audit-failed-tx (none) + audit-succeeded-tx = 2 commits
        assert conn.txn_committed >= 2
        assert conn.txn_rolled_back == 0

    def test_empty_staging_updates_audit_empty(self):
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [],  # no codes
                [],  # no periods
            ],
            fetchone_results=[(0,)],
        )

        result = merge_to_core(conn, RUN_ID)

        assert result.merged_rows == 0
        assert result.partitions_created == 0
        assert result.status == STATUS_EMPTY
        assert conn.has_sql("INSERT INTO core.fact_generation")

    def test_idempotent_rerun_same_sql(self):
        """Re-running merge_to_core with same staging → same SQL structure."""
        codes = [tuple(c) for c in KNOWN_LEAF_CODES]
        periods = [("2026-07",)]

        conn1 = _configure(
            conn1 := FakeConn(), fetchall_results=[codes, periods], fetchone_results=[None, (2,)]
        )
        first = merge_to_core(conn1, RUN_ID)

        conn2 = _configure(
            conn2 := FakeConn(), fetchall_results=[codes, periods], fetchone_results=[None, (2,)]
        )
        retry = merge_to_core(conn2, RUN_ID)

        # Same MERGE SQL executed both times
        assert len(conn1.merge_sql) == 1
        assert len(conn2.merge_sql) == 1
        assert conn1.merge_sql[0] == conn2.merge_sql[0]
        assert first.status == retry.status == STATUS_SUCCEEDED
        for conn in (conn1, conn2):
            assert [p for sql, p in conn.executed if sql in conn.merge_sql] == [(RUN_ID,)]

    def test_merge_failure_updates_audit_failed(self):
        """Merge transaction raises → audit set to failed."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(c) for c in KNOWN_LEAF_CODES],
                [("2026-07",)],
            ],
            fetchone_results=[None, (2,)],
        )

        # Make the cursor raise on CREATE TABLE (simulating partition failure)
        original_execute = conn.cursor_obj.execute

        def raising_execute(sql, params=None):
            if "CREATE TABLE" in sql and "PARTITION" in sql:
                raise RuntimeError("partition creation failed")
            return original_execute(sql, params)

        conn.cursor_obj.execute = raising_execute

        with pytest.raises(RuntimeError, match="partition creation failed"):
            merge_to_core(conn, RUN_ID)

        assert conn.audit_updates
        assert conn.audit_updates[0][0] == STATUS_FAILED

    def test_publish_gate_failure_no_merge_executed(self):
        """When publish gate blocks, no fact merge or dimension SQL is executed."""
        conn = _configure(
            conn := FakeConn(),
            fetchall_results=[
                [tuple(UNKNOWN_CODES[0])],  # unknown code → blocked
            ],
            fetchone_results=[],
        )

        with pytest.raises(ValidationError, match="core MERGE blocked"):
            merge_to_core(conn, RUN_ID)

        assert not conn.merge_sql
        assert not conn.dimension_upserts
        assert not conn.create_partition_sql


# -- observation provenance --------------------------------------------------


class TestObservationProvenance:
    @pytest.mark.parametrize("field", ["extracted_at", "source"])
    def test_publication_consults_observation_provenance_before_fact_replacement(self, field):
        """Require a provenance read, without pretending the fake compares timestamps.

        Newer-revision, older-replay and equal-time winner semantics still need
        PostgreSQL tests. A timestamp read alone does not prove correct ordering.
        """
        conn = _configure(
            FakeConn(),
            fetchall_results=[KNOWN_LEAF_CODES, [("2026-07",)]],
            fetchone_results=[(1,), (2,)],
        )

        merge_to_core(conn, RUN_ID)

        before_replacement = []
        for sql, _ in conn.executed:
            before_replacement.append(" ".join(sql.lower().split()))
            if sql in conn.merge_sql:
                break
        assert any(
            "audit.ingestion_run" in sql and re.search(rf"\b{field}\b", sql)
            for sql in before_replacement
        ), f"fact replacement must consult recorded {field}, not run UUID or load order"

    def test_staged_rows_without_audit_provenance_are_rejected(self, monkeypatch):
        """No audit row is supplied; known dimensions must not authorize publication."""
        conn = _configure(
            FakeConn(),
            fetchall_results=[KNOWN_LEAF_CODES, [("2026-07",)]],
            fetchone_results=[(1,), (2,)],
        )
        cursor = conn.cursor_obj
        execute, fetchone, fetchall = cursor.execute, cursor.fetchone, cursor.fetchall
        last_sql = ""

        def remember_query(sql, params=None):
            nonlocal last_sql
            last_sql = sql.lower()
            execute(sql, params)

        # Only audit lookups report absence; partition/count results stay valid.
        monkeypatch.setattr(cursor, "execute", remember_query)
        monkeypatch.setattr(
            cursor, "fetchone", lambda: None if "audit.ingestion_run" in last_sql else fetchone()
        )
        monkeypatch.setattr(
            cursor, "fetchall", lambda: [] if "audit.ingestion_run" in last_sql else fetchall()
        )

        with pytest.raises(ValidationError):
            merge_to_core(conn, RUN_ID)

        assert not conn.merge_sql
        assert not conn.dimension_upserts
        assert not conn.create_partition_sql
        assert not any(p[0] == STATUS_SUCCEEDED for p in conn.audit_updates)

    def test_fact_upsert_can_advance_provenance_when_generation_is_identical(self):
        """Protect lineage propagation; actual newer-identical eligibility needs PostgreSQL."""
        from energy_pipeline.load import _MERGE_FACT_SQL

        update = " ".join(_MERGE_FACT_SQL.lower().split()).split("do update set", 1)[1]
        assignments = update.partition(" where ")[0]
        for field in ("run_id", "content_hash"):
            assert re.search(rf"\b{field}\s*=\s*excluded\.{field}\b", assignments)

    @pytest.mark.parametrize("reason", ["older observation", "equal-time conflicting content"])
    def test_observation_rejection_propagates_without_success_audit(self, monkeypatch, reason):
        """Inject rejection, not a fake ordering decision; protect rollback/error reporting."""
        conn = _configure(
            FakeConn(),
            fetchall_results=[KNOWN_LEAF_CODES, [("2026-07",)]],
            fetchone_results=[(1,), (2,)],
        )
        execute = conn.cursor_obj.execute

        def reject_fact_publication(sql, params=None):
            execute(sql, params)
            if sql.strip().startswith("INSERT INTO core.fact_generation"):
                raise ValidationError(reason)

        monkeypatch.setattr(conn.cursor_obj, "execute", reject_fact_publication)

        with pytest.raises(ValidationError, match=reason):
            merge_to_core(conn, RUN_ID)

        assert conn.txn_rolled_back == 1
        assert conn.audit_updates[-1][0] == STATUS_FAILED
        assert not any(p[0] == STATUS_SUCCEEDED for p in conn.audit_updates)

    def test_partial_run_does_not_delete_absent_core_keys(self):
        """Protect the no-delete SQL boundary; retained row contents need PostgreSQL."""
        conn = _configure(
            FakeConn(),
            fetchall_results=[[KNOWN_LEAF_CODES[0]], [("2026-07",)]],
            fetchone_results=[(1,), (1,)],
        )

        assert merge_to_core(conn, RUN_ID).status == STATUS_SUCCEEDED

        for sql, _ in conn.executed:
            normalized = " ".join(sql.lower().split())
            assert not re.search(
                r"\b(?:delete from|truncate(?: table)?)\s+core\.fact_generation\b", normalized
            )


# -- timestamp decisions with supplied database responses --------------------


class TestRevisionPrecedence:
    def _conn(self, *, incoming=None, staged=None, published=()):
        """Supply query results only; do not evaluate SQL or simulate persisted facts."""
        rows = [_staged_row()] if staged is None else staged
        periods = sorted({row[0] for row in rows})
        conn = _configure(
            FakeConn(),
            fetchall_results=[sorted({row[1:4] for row in rows}), [(p,) for p in periods]],
            fetchone_results=[(1,)] * len(periods) + [(len(rows),)],
        )
        conn.cursor_obj.incoming_audit = (
            *(_audit_row() if incoming is None else incoming),
            len(rows),
            0,
        )
        conn.cursor_obj.staged_provenance = rows
        conn.cursor_obj.published_provenance = list(published)
        return conn

    def _assert_rejected(self, conn, *, match=None):
        with pytest.raises(ValidationError, match=match):
            merge_to_core(conn, RUN_ID)
        assert not conn.merge_sql
        assert not conn.dimension_upserts
        assert not conn.create_partition_sql
        assert conn.txn_rolled_back == 1
        assert conn.audit_updates[-1][0] == STATUS_FAILED
        assert not any(p[0] == STATUS_SUCCEEDED for p in conn.audit_updates)

    @pytest.mark.parametrize("source", [API_SOURCE_NAME, FIXTURE_SOURCE_NAME])
    def test_newer_revision_is_accepted_within_the_same_source(self, source):
        incoming = _audit_row(content_hash=REVISED_HASH, source=source)
        old = _audit_row(OTHER_RUN_ID, source=source, at=OBSERVED_AT - timedelta(days=1))
        conn = self._conn(
            incoming=incoming,
            staged=[_staged_row(generation=1.5, content_hash=REVISED_HASH)],
            published=[_published_row(_staged_row(), old)],
        )

        result = merge_to_core(conn, RUN_ID)

        assert result.status == STATUS_SUCCEEDED
        assert result.merged_rows == 1
        assert len(conn.merge_sql) == 1
        assert conn.audit_updates[-1][0] == STATUS_SUCCEEDED

    @pytest.mark.parametrize("days", [-1, 0], ids=["older", "equal-conflicting"])
    def test_older_or_equal_conflicting_observation_rejects_before_core_writes(self, days):
        incoming = _audit_row(content_hash=REVISED_HASH, at=OBSERVED_AT + timedelta(days=days))
        conn = self._conn(
            incoming=incoming,
            staged=[_staged_row(generation=1.5, content_hash=REVISED_HASH)],
            published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))],
        )

        self._assert_rejected(conn, match="older|equal-time")

    def test_unchanged_same_run_retry_is_accepted(self):
        conn = self._conn(published=[_published_row(_staged_row(), _audit_row())])

        assert merge_to_core(conn, RUN_ID).status == STATUS_SUCCEEDED

    def test_same_run_retry_cannot_restore_a_superseded_run(self):
        newer = _audit_row(
            OTHER_RUN_ID, content_hash=REVISED_HASH, at=OBSERVED_AT + timedelta(days=1)
        )
        conn = self._conn(published=[_published_row(_staged_row(generation=1.5), newer)])

        self._assert_rejected(conn, match="older")

    def test_newer_identical_observation_is_eligible_to_advance_lineage(self):
        incoming = _audit_row(at=OBSERVED_AT + timedelta(days=1))
        conn = self._conn(
            incoming=incoming,
            published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))],
        )

        assert merge_to_core(conn, RUN_ID).status == STATUS_SUCCEEDED
        # Persistence of this advancement is a PostgreSQL integration requirement.
        sql = " ".join(conn.merge_sql[0].lower().split())
        assert "run_id = excluded.run_id" in sql
        assert "incoming.extracted_at > published.extracted_at" in sql

    def test_intermediate_replay_is_rejected_against_advanced_identical_provenance(self):
        advanced = _audit_row(OTHER_RUN_ID, at=OBSERVED_AT + timedelta(days=1))
        conn = self._conn(
            incoming=_audit_row(at=OBSERVED_AT + timedelta(hours=12)),
            published=[_published_row(_staged_row(), advanced)],
        )

        self._assert_rejected(conn, match="older")

    def test_equal_time_distinct_identical_observation_is_an_equivalent_no_op(self):
        conn = self._conn(published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))])

        assert merge_to_core(conn, RUN_ID).status == STATUS_SUCCEEDED
        # SQL permits a different run to replace only at a strictly later time.
        # Retained fact provenance itself must be checked against PostgreSQL.
        sql = " ".join(conn.merge_sql[0].lower().split())
        assert "tgt.run_id = excluded.run_id" in sql
        assert "incoming.extracted_at > published.extracted_at" in sql
        assert "incoming.extracted_at >= published.extracted_at" not in sql

    @pytest.mark.parametrize("generation", [None, 0.0, -1.25])
    def test_equal_hash_does_not_hide_conflicting_per_key_values(self, generation):
        conn = self._conn(
            staged=[_staged_row(generation=generation)],
            published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))],
        )

        self._assert_rejected(conn, match="equal-time")

    def test_same_run_with_changed_payload_is_inconsistent_identity(self):
        conn = self._conn(
            staged=[_staged_row(generation=1.5)],
            published=[_published_row(_staged_row(), _audit_row())],
        )

        self._assert_rejected(conn, match="same-run")

    def test_newer_run_cannot_change_payload_under_identical_content_identity(self):
        conn = self._conn(
            incoming=_audit_row(at=OBSERVED_AT + timedelta(days=1)),
            staged=[_staged_row(generation=1.5)],
            published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))],
        )

        self._assert_rejected(conn, match="content hash")

    @pytest.mark.parametrize("source", [API_SOURCE_NAME, FIXTURE_SOURCE_NAME])
    def test_fixture_and_live_sources_cannot_replace_each_other(self, source):
        other_source = FIXTURE_SOURCE_NAME if source == API_SOURCE_NAME else API_SOURCE_NAME
        old = _audit_row(OTHER_RUN_ID, source=other_source, at=OBSERVED_AT - timedelta(days=1))
        conn = self._conn(
            incoming=_audit_row(source=source),
            published=[_published_row(_staged_row(), old)],
        )

        self._assert_rejected(conn, match="fixture and live")

    @pytest.mark.parametrize(
        "index, value",
        [(0, None), (1, None), (2, SOURCE_NAME), (4, "2026-13"), (5, None), (5, "bad-time")],
    )
    def test_missing_or_malformed_incoming_identity_fails_closed(self, index, value):
        incoming = list(_audit_row())
        incoming[index] = value
        self._assert_rejected(self._conn(incoming=tuple(incoming)))

    @pytest.mark.parametrize("damage", ["missing-audit", "legacy-source", "hash", "time"])
    def test_invalid_existing_fact_provenance_fails_closed(self, damage):
        audit = _audit_row(OTHER_RUN_ID, at=OBSERVED_AT - timedelta(days=1))
        published = list(_published_row(_staged_row(), audit))
        if damage == "missing-audit":
            published[10:] = [None] * 6
        elif damage == "legacy-source":
            published[12] = SOURCE_NAME
        elif damage == "hash":
            published[9] = REVISED_HASH
        else:
            published[15] = None

        self._assert_rejected(self._conn(published=[tuple(published)]))

    def test_staging_hash_must_match_its_locked_audit_identity(self):
        conn = self._conn(staged=[_staged_row(content_hash=REVISED_HASH)])
        self._assert_rejected(conn, match="staging content hash")

    @pytest.mark.parametrize("field, value", [(6, None), (6, 2), (7, 1)])
    def test_inconsistent_audit_count_or_validation_state_is_rejected(self, field, value):
        conn = self._conn()
        row = list(conn.cursor_obj.incoming_audit)
        row[field] = value
        conn.cursor_obj.incoming_audit = tuple(row)

        self._assert_rejected(conn)

    def test_mixed_eligible_and_stale_keys_cannot_partially_publish(self):
        staged = [
            _staged_row(content_hash=REVISED_HASH),
            _staged_row(("TX", "7", "WND"), content_hash=REVISED_HASH),
        ]
        published = [
            _published_row(staged[0], _audit_row(OTHER_RUN_ID, at=OBSERVED_AT - timedelta(days=1))),
            _published_row(staged[1], _audit_row(THIRD_RUN_ID, at=OBSERVED_AT + timedelta(days=1))),
        ]
        conn = self._conn(
            incoming=_audit_row(content_hash=REVISED_HASH), staged=staged, published=published
        )

        self._assert_rejected(conn, match="older")

    def test_lock_and_success_audit_stay_inside_the_publication_transaction(self, monkeypatch):
        conn = self._conn(published=[_published_row(_staged_row(), _audit_row(OTHER_RUN_ID))])
        events = []
        execute = conn.cursor_obj.execute

        def capture(sql, params=None):
            events.append((sql, conn.transaction_depth))
            execute(sql, params)

        monkeypatch.setattr(conn.cursor_obj, "execute", capture)

        merge_to_core(conn, RUN_ID)

        sql = [statement for statement, _ in events]
        order = [
            sql.index(statement)
            for statement in (
                _PUBLICATION_LOCK_SQL,
                _INCOMING_AUDIT_SQL,
                _STAGED_PROVENANCE_SQL,
                _PUBLISHED_PROVENANCE_SQL,
                conn.merge_sql[0],
            )
        ]
        assert order == sorted(order)
        assert all(events[index][1] == 1 for index in order)
        success = next(
            (statement, depth)
            for statement, depth in events
            if statement.startswith("UPDATE audit")
        )
        assert success[1] == 2  # Nested status block, before outer publication exit.
        assert conn.transaction_depth == 0
        assert "FOR UPDATE" in _INCOMING_AUDIT_SQL
        assert "FOR UPDATE OF fact" in _PUBLISHED_PROVENANCE_SQL


# -- helpers -----------------------------------------------------------------


class TestCodesToRecords:
    def test_creates_minimal_records_for_publish_gate(self):
        from energy_pipeline.load import _codes_to_records

        codes = [("AK", "1", "NG"), ("US", "7", "ALL")]
        records = _codes_to_records(codes)

        assert len(records) == 2
        assert records[0].location == "AK"
        assert records[0].sectorid == "1"
        assert records[0].fueltypeid == "NG"
        assert records[0].generation is None  # not needed for publish gate
        assert records[1].location == "US"

    def test_empty_codes_returns_empty_list(self):
        from energy_pipeline.load import _codes_to_records

        assert _codes_to_records([]) == []
