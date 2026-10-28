"""Opt-in verification against an already provisioned, disposable PostgreSQL 18.

Disabled tests are SKIPPED, never evidence of successful PostgreSQL verification.
Provisioning is separate: apply sql/001_schema.sql as energy_pg_runner to a new,
empty energy_pg_test database in a private Unix-socket-only cluster. The test
role must own its application objects without administrative privileges.

Enable only with ENERGY_TEST_PG=1 and all of these independently supplied inputs:
ENERGY_TEST_PG_CLUSTER_ROOT, ENERGY_TEST_PG_PORT, ENERGY_TEST_PG_CLUSTER_NAME,
ENERGY_TEST_PG_DATABASE_OID, and ENERGY_TEST_PG_DSN. The cluster name must begin
with energy_pg_test_. The private root and its socket/ child must belong to the
non-root test user, have mode 0700, and have no symlink or writable ancestry.
The explicit nondefault port and exact canonical DSN prevent libpq fallbacks,
aliases, duplicate keys, passwords, extra options or missing connection fields.
Obtain the database OID independently during separately approved provisioning.

Never read .env. Never provision/apply schemas, drop databases or clean files here.
Successful test data remains for inspection; reruns require an externally prepared
empty database. Run sequentially, without xdist. All raw objects stay in memory.
"""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from time import monotonic

import pytest

from energy_pipeline.config import Settings
from energy_pipeline.errors import StorageError, ValidationError
from energy_pipeline.extract import API_SOURCE_NAME, FixtureSource, PeriodWindow, extract
from energy_pipeline.load import load_raw_batch, merge_to_core, stage_from_extraction
from energy_pipeline.raw import canonical_json, sha256_hex
from energy_pipeline.storage import InMemoryObjectStore, RawSink
from energy_pipeline.warehouse import Warehouse

DATABASE = "energy_pg_test"
ROLE = "energy_pg_runner"


@dataclass(frozen=True)
class _PgConfig:
    dsn: str
    expected_oid: int
    cluster_root: Path
    port: int
    cluster_name: str

    @property
    def socket(self) -> Path:
        return self.cluster_root / "socket"


def _config_from_env(env: Mapping[str, str]) -> _PgConfig:
    """Validate explicit connection inputs without opening a database connection."""
    if env.get("ENERGY_TEST_PG") != "1":
        raise ValueError("ENERGY_TEST_PG must be exactly 1 to enable integration")
    root = env.get("ENERGY_TEST_PG_CLUSTER_ROOT", "")
    if (
        not re.fullmatch(r"/[A-Za-z0-9_./-]+", root)
        or str(Path(root)) != root
        or ".." in Path(root).parts
    ):
        raise ValueError("Supply an explicit normalized absolute private cluster root")
    raw_port = env.get("ENERGY_TEST_PG_PORT", "")
    if not re.fullmatch(r"[1-9][0-9]{3,4}", raw_port):
        raise ValueError("Supply an explicit nondefault private PostgreSQL port")
    port = int(raw_port)
    if not 1024 <= port <= 65535 or port == 5432:
        raise ValueError("Private PostgreSQL port must be nondefault and unprivileged")
    cluster_name = env.get("ENERGY_TEST_PG_CLUSTER_NAME", "")
    if not re.fullmatch(r"energy_pg_test_[a-z0-9_]{1,40}", cluster_name):
        raise ValueError("Supply an explicit test-specific PostgreSQL cluster name")
    oid = env.get("ENERGY_TEST_PG_DATABASE_OID", "")
    if not re.fullmatch(r"[1-9][0-9]{0,9}", oid) or int(oid) > 4294967295:
        raise ValueError("Supply the independently recorded test database OID")
    socket = Path(root) / "socket"
    dsn = (
        f"host={socket} port={port} dbname={DATABASE} user={ROLE} "
        "connect_timeout=5 passfile=/dev/null"
    )
    if env.get("ENERGY_TEST_PG_DSN") != dsn:
        raise ValueError("Explicit test DSN missing or differs from the canonical test DSN")
    return _PgConfig(dsn, int(oid), Path(root), port, cluster_name)


CORE_TABLES = (
    "core.dim_date",
    "core.dim_geography",
    "core.dim_sector",
    "core.dim_energy_source",
    "core.fact_generation",
)
pytestmark = pytest.mark.skipif(
    os.environ.get("ENERGY_TEST_PG") in (None, "", "0"),
    reason="PostgreSQL integration deliberately disabled; no database verification performed",
)


def _query(conn, sql, params=()):
    """Finish every observation transaction before calling a loader entry point."""
    from psycopg.pq import TransactionStatus

    assert conn.info.transaction_status == TransactionStatus.IDLE
    with conn.transaction():
        return conn.execute(sql, params).fetchall()


@contextmanager
def _connection(config):
    """Check each writer/observer connection before allowing any test data writes."""
    with Warehouse(config.dsn).connect() as conn:
        identity = _query(
            conn,
            "SELECT current_database(), current_user, session_user, "
            "current_setting('port'), current_setting('server_version_num')::int, "
            "current_setting('cluster_name'), inet_server_addr(), d.oid::bigint, "
            "pg_get_userbyid(d.datdba), "
            "r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication "
            "OR r.rolbypassrls "
            "FROM pg_database d JOIN pg_roles r ON r.rolname = current_user "
            "WHERE d.datname = current_database()",
        )[0]
        expected = (
            DATABASE,
            ROLE,
            ROLE,
            str(config.port),
        )
        if (
            identity[:4] != expected
            or not 180000 <= identity[4] < 190000
            or identity[5:] != (config.cluster_name, None, config.expected_oid, ROLE, False)
            or conn.info.host != str(config.socket)
            or conn.info.port != config.port
        ):
            pytest.fail("Disposable PostgreSQL identity mismatch; refusing writes", pytrace=False)
        if _query(conn, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'energy')")[0][0]:
            pytest.fail("Cluster contains energy; refusing integration writes", pytrace=False)
        if _query(conn, "SELECT current_setting('listen_addresses')") != [("",)]:
            pytest.fail("Disposable cluster must have TCP disabled", pytrace=False)
        with conn.transaction():
            conn.execute("SET statement_timeout = '5s'")
            conn.execute("SET lock_timeout = '1s'")
            conn.execute("SET idle_in_transaction_session_timeout = '15s'")
            conn.execute("SET search_path = pg_catalog")
        yield conn


@pytest.fixture(scope="module")
def pg_config():
    try:
        config = _config_from_env(os.environ)
    except ValueError as exc:
        pytest.fail(str(exc), pytrace=False)
    if os.getuid() == 0:
        pytest.fail("Run disposable integration as a non-root user", pytrace=False)
    if "PYTEST_XDIST_WORKER" in os.environ:
        pytest.fail("This bounded integration module must run sequentially", pytrace=False)
    try:
        for directory in (config.cluster_root, config.socket):
            metadata = directory.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or directory.resolve() != directory
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                pytest.fail("Private socket directories are unsafe", pytrace=False)
        for parent in config.cluster_root.parents:
            metadata = parent.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid not in (0, os.getuid())
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                pytest.fail("Private socket ancestry is unsafe", pytrace=False)
        endpoint = (config.socket / f".s.PGSQL.{config.port}").lstat()
        if not stat.S_ISSOCK(endpoint.st_mode) or endpoint.st_uid != os.getuid():
            pytest.fail("Expected private PostgreSQL socket is absent or unsafe", pytrace=False)
    except OSError:
        pytest.fail("Private PostgreSQL directories or socket are unavailable", pytrace=False)
    with pytest.MonkeyPatch.context() as patch:
        # Do not let libpq inherit production host/service/options/password settings.
        for name in tuple(os.environ):
            if name.startswith("PG") or name == "POSTGRES_DSN":
                patch.delenv(name)
        with _connection(config) as conn:
            for table in (*CORE_TABLES, "raw.stg_generation", "audit.ingestion_run"):
                if _query(conn, "SELECT to_regclass(%s)::text", (table,))[0][0] is None:
                    pytest.fail("Apply the tracked schema separately before testing", pytrace=False)
                if _query(conn, f"SELECT EXISTS (SELECT 1 FROM {table})")[0][0]:
                    pytest.fail("Integration requires empty, pre-provisioned tables", pytrace=False)
            if _query(
                conn,
                "SELECT EXISTS (SELECT 1 FROM pg_inherits "
                "WHERE inhparent = 'core.fact_generation'::regclass)",
            )[0][0]:
                pytest.fail("Integration requires no existing fact partitions", pytrace=False)
            owners = _query(
                conn,
                "SELECT nspname FROM pg_namespace "
                "WHERE nspname IN ('raw', 'core', 'audit', 'marts') "
                "AND pg_get_userbyid(nspowner) = current_user",
            )
            assert {row[0] for row in owners} == {"raw", "core", "audit", "marts"}
            assert (
                _query(
                    conn,
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname IN ('raw', 'core', 'audit', 'marts') "
                    "AND c.relkind IN ('r', 'p', 'S') "
                    "AND pg_get_userbyid(c.relowner) <> current_user",
                )
                == []
            )
        yield config


class _Rows(FixtureSource):
    """In-memory fixture using the real fixture paging and explicit origin label."""

    def __init__(self, rows):
        self.rows = rows
        self.requests = []


def _row(period, **changes):
    row = {
        "period": period,
        "location": "AK",
        "stateDescription": "Synthetic geography",
        "sectorid": "1",
        "sectorDescription": "Synthetic sector",
        "fueltypeid": "NG",
        "fuelTypeDescription": "Synthetic fuel",
        "generation": "1.25",
        "generation-units": "thousand megawatthours",
    }
    return row | changes


def _raw(rows, start, end=None, *, extracted_at="2026-10-25T00:00:00Z"):
    result = extract(
        _Rows(rows),
        Settings(api_key=None, raw_prefix="test-only/b1"),
        PeriodWindow(start, end or start),
        extracted_at=extracted_at,
    )
    store = InMemoryObjectStore()
    upload = RawSink(store, clock=lambda: "2026-10-25T00:00:01Z").write(result.batch)
    return result, store, upload.manifest_path


def _publish(conn, result):
    assert result.report.ok
    stage_from_extraction(conn, result)
    return merge_to_core(conn, result.batch.run_id)


def _facts(conn, period):
    return _query(
        conn,
        "SELECT location, sectorid, fueltypeid, generation_thousand_mwh, generation_mwh, "
        "is_rollup_geography, is_rollup_sector, is_rollup_energy_source, "
        "run_id::text, content_hash FROM core.fact_generation "
        "WHERE period = %s ORDER BY location, sectorid, fueltypeid",
        (period,),
    )


def _audit(conn, run_id):
    return _query(
        conn,
        "SELECT status, row_count, error_msg FROM audit.ingestion_run WHERE run_id = %s",
        (run_id,),
    )


def _core_snapshot(conn):
    return {
        table: _query(
            conn,
            f"SELECT * FROM {table} ORDER BY "
            + ("1, 2, 3, 4" if table == "core.fact_generation" else "1, 2"),
        )
        for table in CORE_TABLES
    }


def test_initial_publication_conversion_and_idempotent_repeat(pg_config):
    period = "2001-01"
    result, store, manifest = _raw(
        [
            _row(period),
            _row(period, fueltypeid="SUN", generation="0"),
            _row(period, fueltypeid="HPS", generation="-0.5"),
            _row(period, location="DC", generation=None),
        ],
        period,
    )
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        assert load_raw_batch(store, writer, manifest).staged_rows == 4
        assert _audit(observer, result.batch.run_id) == [("staged", 4, None)]
        assert merge_to_core(writer, result.batch.run_id).status == "succeeded"
        facts = _facts(observer, period)
        assert [(r[0], r[2], r[3], r[4]) for r in facts] == [
            ("AK", "HPS", -0.5, -500.0),
            ("AK", "NG", 1.25, 1250.0),
            ("AK", "SUN", 0.0, 0.0),
            ("DC", "NG", None, None),
        ]
        # SUN remains a fuel rollup even when its generation is zero.
        assert [row[5:8] for row in facts] == [
            (False, False, False),  # AK / HPS
            (False, False, False),  # AK / NG
            (False, False, True),  # AK / SUN
            (False, False, False),  # DC / NG
        ]
        dimensions = {
            table: rows
            for table, rows in _core_snapshot(observer).items()
            if table != "core.fact_generation"
        }
        load_raw_batch(store, writer, manifest)
        assert merge_to_core(writer, result.batch.run_id).merged_rows == 4
        assert _facts(observer, period) == facts
        assert all(_core_snapshot(observer)[table] == rows for table, rows in dimensions.items())
        assert (
            _query(
                observer,
                "SELECT period, location, sectorid, fueltypeid FROM core.fact_generation "
                "GROUP BY period, location, sectorid, fueltypeid HAVING COUNT(*) > 1",
            )
            == []
        )
    with _connection(pg_config) as reopened:
        assert _facts(reopened, period) == facts
        assert _audit(reopened, result.batch.run_id) == [("succeeded", 4, None)]


def test_newer_revision_commits_and_older_replay_is_rejected(pg_config):
    period = "2001-02"
    original, _, _ = _raw([_row(period)], period)
    revised, _, _ = _raw(
        [_row(period, generation="1.5")], period, extracted_at="2026-10-26T00:00:00Z"
    )
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        for result, expected in [(original, 1.25), (revised, 1.5)]:
            _publish(writer, result)
            facts = _facts(observer, period)
            assert len(facts) == 1
            assert facts[0][3:5] == (expected, expected * 1000)
            assert facts[0][8:] == (result.batch.run_id, result.batch.content_hash)
        before = _core_snapshot(observer)
        with pytest.raises(ValidationError, match="older observation"):
            _publish(writer, original)
        assert _core_snapshot(observer) == before
        assert _audit(observer, original.batch.run_id)[0][0] == "failed"
        assert _audit(observer, revised.batch.run_id) == [("succeeded", 1, None)]


@pytest.mark.parametrize(
    "changes, warning",
    [
        ({"location": "ZZ"}, "geography"),
        ({"location": "US", "sectorid": "8"}, "sector"),
        ({"location": "US", "fueltypeid": "BIO"}, "energy_source"),
        ({"sectorid": "8", "fueltypeid": "ALL"}, "sector"),
    ],
)
def test_unknown_codes_block_publication_before_core_writes(pg_config, changes, warning):
    result, _, _ = _raw([_row("2001-03", **changes)], "2001-03")
    assert warning in result.report.unknown_codes
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        before = _core_snapshot(observer)
        stage_from_extraction(writer, result)
        with pytest.raises(ValidationError, match="core MERGE blocked"):
            merge_to_core(writer, result.batch.run_id)
        assert _core_snapshot(observer) == before
        assert _audit(observer, result.batch.run_id)[0][0] == "failed"
        assert _query(observer, "SELECT to_regclass('core.fact_generation_2001_03')") == [(None,)]
        assert _query(
            observer,
            "SELECT COUNT(*) FROM raw.stg_generation WHERE run_id = %s",
            (result.batch.run_id,),
        ) == [(1,)]


def test_staging_sql_failure_restores_previously_committed_rows(pg_config):
    from psycopg.errors import NotNullViolation

    result, _, _ = _raw([_row("2001-04"), _row("2001-04", fueltypeid="SUN")], "2001-04")
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        stage_from_extraction(writer, result)
        before = _query(
            observer,
            "SELECT * FROM raw.stg_generation WHERE run_id = %s ORDER BY fueltypeid",
            (result.batch.run_id,),
        )
        # Bypass parsing deliberately: valid first insert, then a PK NOT NULL failure.
        result.merge_rows[1] = replace(result.merge_rows[1], period=None)
        assert result.report.ok
        with pytest.raises(NotNullViolation):
            stage_from_extraction(writer, result)
        assert (
            _query(
                observer,
                "SELECT * FROM raw.stg_generation WHERE run_id = %s ORDER BY fueltypeid",
                (result.batch.run_id,),
            )
            == before
        )
        # Current staging SQL failure handling leaves the prior audit state intact.
        assert _audit(observer, result.batch.run_id) == [("staged", 2, None)]


def test_core_overflow_rolls_back_dimensions_partition_and_fact(pg_config):
    from psycopg.errors import NumericValueOutOfRange

    period = "2001-05"
    result, _, _ = _raw([_row(period, location="PR", fueltypeid="NUC", generation="1e308")], period)
    assert result.report.ok  # Finite source number; overflow occurs in SQL's x1000.
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        stage_from_extraction(writer, result)
        before = _core_snapshot(observer)
        with pytest.raises(NumericValueOutOfRange):
            merge_to_core(writer, result.batch.run_id)
        assert _core_snapshot(observer) == before
        assert _query(observer, "SELECT to_regclass('core.fact_generation_2001_05')") == [(None,)]
        status, count, message = _audit(observer, result.batch.run_id)[0]
        assert (status, count) == ("failed", 1)
        assert message


def test_outer_transaction_is_not_mistaken_for_durable_publication(pg_config):
    period = "2001-06"
    original, _, _ = _raw([_row(period)], period)
    result, _, _ = _raw(
        [_row(period, generation="1.5")], period, extracted_at="2026-10-26T00:00:00Z"
    )
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        # Commit partition creation first: uncommitted partition DDL could block
        # the observer on the parent lock, hiding the transaction visibility check.
        _publish(writer, original)
        before = _facts(observer, period)
        writer.execute("SELECT 1")  # Open a default psycopg transaction deliberately.
        assert _publish(writer, result).status == "succeeded"
        assert writer.execute(
            "SELECT generation_mwh FROM core.fact_generation WHERE period = %s", (period,)
        ).fetchone() == (1500.0,)
        assert _facts(observer, period) == before
        assert _audit(observer, result.batch.run_id) == []
        writer.rollback()
        assert _facts(observer, period) == before
        assert _audit(observer, result.batch.run_id) == []
        assert _audit(observer, original.batch.run_id) == [("succeeded", 1, None)]


def test_empty_run_has_durable_empty_audit(pg_config):
    result, store, manifest = _raw([], "2001-07")
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        before = _core_snapshot(observer)
        load_raw_batch(store, writer, manifest)
        merged = merge_to_core(writer, result.batch.run_id)
        assert (merged.status, merged.merged_rows, merged.partitions_created) == ("empty", 0, 0)
        assert _core_snapshot(observer) == before
        assert _audit(observer, result.batch.run_id) == [("empty", 0, None)]


def test_monthly_partition_routing_catalog_and_rollup_control(pg_config):
    periods = ("2001-12", "2002-01")
    result, _, _ = _raw(
        [_row(period, location="US", fueltypeid="ALL") for period in periods], *periods
    )
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        assert _publish(writer, result).partitions_created == 2
        for period, end in zip(periods, ("2002-01", "2002-02"), strict=True):
            name = "fact_generation_" + period.replace("-", "_")
            assert _query(
                observer,
                "SELECT tableoid::regclass::text FROM core.fact_generation WHERE period = %s",
                (period,),
            ) == [("core." + name,)]
            bounds = _query(
                observer,
                "SELECT p.oid::regclass::text, pg_get_expr(c.relpartbound, c.oid) "
                "FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid "
                "JOIN pg_class p ON p.oid = i.inhparent WHERE c.oid = to_regclass(%s)",
                ("core." + name,),
            )
            assert bounds == [
                ("core.fact_generation", f"FOR VALUES FROM ('{period}') TO ('{end}')")
            ]
            assert _facts(observer, period)[0][5:8] == (True, False, True)
        assert _query(
            observer, "SELECT is_rollup FROM core.dim_geography WHERE location = 'US'"
        ) == [(True,)]
        assert _query(
            observer, "SELECT is_aggregate FROM core.dim_energy_source WHERE fueltypeid = 'ALL'"
        ) == [(True,)]


@pytest.mark.parametrize("via_raw", [True, False], ids=["raw", "stale-report"])
@pytest.mark.parametrize(
    "changes, typed_fields",
    [
        ({"generation": "NaN"}, {"generation": float("nan")}),
        ({"generation": "Infinity"}, {"generation": float("inf")}),
        ({"generation-units": "megawatthours"}, {"generation_units": "megawatthours"}),
    ],
)
def test_invalid_generation_or_units_never_enter_staging(pg_config, via_raw, changes, typed_fields):
    rows = [_row("2001-08", **(changes if via_raw else {}))]
    result, store, manifest = _raw(rows, "2001-08")
    if not via_raw:
        result.merge_rows[0] = replace(result.merge_rows[0], **typed_fields)
        assert result.report.ok
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        before = _core_snapshot(observer)
        with pytest.raises(ValidationError, match="generation"):
            if via_raw:
                load_raw_batch(store, writer, manifest)
            else:
                stage_from_extraction(writer, result)
        assert _query(
            observer,
            "SELECT COUNT(*) FROM raw.stg_generation WHERE run_id = %s",
            (result.batch.run_id,),
        ) == [(0,)]
        assert _core_snapshot(observer) == before
        audit = _audit(observer, result.batch.run_id)
        assert audit[0][0] == "failed" if via_raw else audit == []


@pytest.mark.parametrize(
    "damage, error",
    [
        ("missing-manifest", StorageError),
        ("malformed-manifest", json.JSONDecodeError),
        ("missing-page", StorageError),
        ("checksum", ValidationError),
        ("page-json", json.JSONDecodeError),
    ],
)
def test_raw_corruption_causes_no_database_writes(pg_config, damage, error):
    result, store, manifest = _raw([_row("2001-09")], "2001-09")
    metadata = json.loads(store.read(manifest))
    page = metadata["objects"][0]["path"]
    if damage == "missing-manifest":
        del store.objects[manifest]
    elif damage == "malformed-manifest":
        store.objects[manifest] = b"{"
    elif damage == "missing-page":
        del store.objects[page]
    elif damage == "checksum":
        store.objects[page] = b"[]"
    else:
        store.objects[page] = b"{"
        metadata["objects"][0]["sha256"] = sha256_hex(store.objects[page])
        store.objects[manifest] = canonical_json(metadata).encode()
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        before = _core_snapshot(observer)
        with pytest.raises(error):
            load_raw_batch(store, writer, manifest)
        assert _core_snapshot(observer) == before
        assert _audit(observer, result.batch.run_id) == []
        assert _query(
            observer,
            "SELECT COUNT(*) FROM raw.stg_generation WHERE run_id = %s",
            (result.batch.run_id,),
        ) == [(0,)]


@pytest.mark.parametrize("via_raw", [True, False], ids=["raw", "extraction"])
@pytest.mark.parametrize(
    "changed_identity",
    [
        {"content_hash": sha256_hex(b"conflicting synthetic content")},
        {"source": API_SOURCE_NAME},
        {"extracted_at": "2026-10-26T00:00:00Z"},
        {"window_start": "1999-01"},
        {"window_end": "2004-12"},
    ],
    ids=["hash", "origin", "observation-time", "window-start", "window-end"],
)
def test_run_identity_conflict_preserves_audit_and_staging(pg_config, via_raw, changed_identity):
    result, store, manifest = _raw([_row("2002-02")], "2002-02")
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        stage_from_extraction(writer, result)
        run_id = result.batch.run_id
        staging_sql = "SELECT * FROM raw.stg_generation WHERE run_id = %s"
        audit_sql = "SELECT * FROM audit.ingestion_run WHERE run_id = %s"
        staging_before = _query(observer, staging_sql, (run_id,))
        audit_before = _query(observer, audit_sql, (run_id,))
        core_before = _core_snapshot(observer)
        if via_raw:
            # Deliberately forge only this in-memory manifest, never real raw data.
            metadata = json.loads(store.read(manifest)) | changed_identity
            store.objects[manifest] = canonical_json(metadata).encode()
        elif "content_hash" in changed_identity:
            # Hashes derive from pages: change the observation, then reuse its run ID.
            conflicting, _, _ = _raw(
                [_row("2002-02", generation="2")],
                "2002-02",
                extracted_at=result.batch.extracted_at,
            )
            conflicting.batch = replace(conflicting.batch, run_id=run_id)
            assert conflicting.batch.content_hash != result.batch.content_hash
            result = conflicting
        else:
            result.batch = replace(result.batch, **changed_identity)
        with pytest.raises(ValidationError, match="audit identity claim rejected"):
            if via_raw:
                load_raw_batch(store, writer, manifest)
            else:
                stage_from_extraction(writer, result)
        assert _query(observer, staging_sql, (run_id,)) == staging_before
        assert _query(observer, audit_sql, (run_id,)) == audit_before
        assert _core_snapshot(observer) == core_before


def test_equal_time_conflict_rejects_without_publication(pg_config):
    period = "2002-03"
    original, _, _ = _raw([_row(period)], period)
    conflicting, _, _ = _raw([_row(period, generation="2")], period)
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, original)
        before = _core_snapshot(observer)
        with pytest.raises(ValidationError, match="equal-time conflicting"):
            _publish(writer, conflicting)
        assert _core_snapshot(observer) == before
        assert _audit(observer, conflicting.batch.run_id)[0][0] == "failed"


def test_newer_identical_observation_advances_provenance_and_blocks_intervening_replay(pg_config):
    period = "2002-04"
    original, _, _ = _raw([_row(period)], period)
    newer, _, _ = _raw([_row(period)], period, extracted_at="2026-10-27T00:00:00Z")
    intervening, _, _ = _raw(
        [_row(period, generation="2")], period, extracted_at="2026-10-26T00:00:00Z"
    )
    assert original.batch.content_hash == newer.batch.content_hash
    assert original.batch.run_id != newer.batch.run_id
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, original)
        _publish(writer, newer)
        facts = _facts(observer, period)
        assert facts[0][3:5] == (1.25, 1250.0)
        assert facts[0][8:] == (newer.batch.run_id, newer.batch.content_hash)
        with pytest.raises(ValidationError, match="older observation"):
            _publish(writer, intervening)
        assert _facts(observer, period) == facts
        assert _audit(observer, intervening.batch.run_id)[0][0] == "failed"


def test_equal_identical_observation_retains_existing_fact_provenance(pg_config):
    period = "2002-05"
    original, _, _ = _raw([_row(period)], period)
    equivalent, _, _ = _raw([_row(period)], period)
    assert original.batch.content_hash == equivalent.batch.content_hash
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, original)
        before = _facts(observer, period)
        assert _publish(writer, equivalent).status == "succeeded"
        assert _facts(observer, period) == before
        assert _audit(observer, equivalent.batch.run_id) == [("succeeded", 1, None)]


@pytest.mark.parametrize("missing", ["incoming", "published"])
def test_missing_audit_provenance_blocks_publication_with_scoped_injection(pg_config, missing):
    period = "2003-01" if missing == "incoming" else "2003-02"
    original, _, _ = _raw([_row(period)], period)
    newer, _, _ = _raw([_row(period, generation="2")], period, extracted_at="2026-10-26T00:00:00Z")
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, original)
        stage_from_extraction(writer, newer)
        before = _core_snapshot(observer)
        audits_before = [_audit(observer, r.batch.run_id) for r in (original, newer)]
        target = newer if missing == "incoming" else original
        # The missing-audit fault and its failed status are rolled back together.
        with (
            pytest.raises(RuntimeError, match="undo missing-audit injection"),
            writer.transaction(),
        ):
            writer.execute(
                "DELETE FROM audit.ingestion_run WHERE run_id = %s", (target.batch.run_id,)
            )
            with pytest.raises(ValidationError):
                merge_to_core(writer, newer.batch.run_id)
            assert writer.execute(
                "SELECT generation_thousand_mwh, run_id::text FROM core.fact_generation "
                "WHERE period = %s",
                (period,),
            ).fetchall() == [(1.25, original.batch.run_id)]
            assert writer.execute(
                "SELECT status FROM audit.ingestion_run WHERE run_id = %s",
                (newer.batch.run_id,),
            ).fetchall() == ([] if missing == "incoming" else [("failed",)])
            raise RuntimeError("undo missing-audit injection")
        assert _core_snapshot(observer) == before
        assert [_audit(observer, r.batch.run_id) for r in (original, newer)] == audits_before


def test_mixed_stale_batch_rejects_all_updates_and_first_inserts(pg_config):
    older, _, _ = _raw([_row("2002-07")], "2002-07")
    latest, _, _ = _raw([_row("2002-08")], "2002-08", extracted_at="2026-10-27T00:00:00Z")
    mixed, _, _ = _raw(
        [
            _row("2002-07", generation="2"),
            _row("2002-08", generation="2"),
            _row("2002-09", location="PR", fueltypeid="NUC"),
        ],
        "2002-07",
        "2002-09",
        extracted_at="2026-10-26T00:00:00Z",
    )
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, older)
        _publish(writer, latest)
        before = _core_snapshot(observer)
        partitions_sql = (
            "SELECT inhrelid FROM pg_inherits "
            "WHERE inhparent = 'core.fact_generation'::regclass ORDER BY inhrelid"
        )
        partitions_before = _query(observer, partitions_sql)
        with pytest.raises(ValidationError, match="older observation"):
            _publish(writer, mixed)
        assert _core_snapshot(observer) == before
        assert _query(observer, partitions_sql) == partitions_before
        assert _query(observer, "SELECT to_regclass('core.fact_generation_2002_09')") == [(None,)]
        assert _audit(observer, mixed.batch.run_id)[0][:2] == ("failed", 3)


def test_newer_response_omissions_do_not_delete_published_keys(pg_config):
    period = "2002-10"
    original, _, _ = _raw([_row(period), _row(period, fueltypeid="HPS")], period)
    newer, _, _ = _raw([_row(period, generation="2")], period, extracted_at="2026-10-26T00:00:00Z")
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        _publish(writer, original)
        _publish(writer, newer)
        facts = _facts(observer, period)
        assert [(r[2], r[3], r[8]) for r in facts] == [
            ("HPS", 1.25, original.batch.run_id),
            ("NG", 2.0, newer.batch.run_id),
        ]


@pytest.mark.parametrize("first_is_newer", [False, True], ids=["older-first", "newer-first"])
def test_competing_first_inserts_serialize_and_recheck_precedence(pg_config, first_is_newer):
    period = "2002-12" if first_is_newer else "2002-11"
    older, _, _ = _raw([_row(period)], period)
    newer, _, _ = _raw([_row(period, generation="2")], period, extracted_at="2026-10-26T00:00:00Z")
    first, second = (newer, older) if first_is_newer else (older, newer)
    ready, release, poll = Event(), Event(), Event()
    pids, outcomes = Queue(), {}

    def publish(name, result, hold):
        try:
            with _connection(pg_config) as conn:
                with conn.transaction():
                    conn.execute("SET lock_timeout = '8s'")
                    conn.execute("SET statement_timeout = '10s'")
                    conn.execute("SET idle_in_transaction_session_timeout = '20s'")
                pids.put((name, conn.info.backend_pid))
                if hold:
                    with conn.transaction():
                        merged = merge_to_core(conn, result.batch.run_id)
                        ready.set()
                        assert release.wait(15), "Timed out waiting to release first publisher"
                else:
                    merged = merge_to_core(conn, result.batch.run_id)
                outcomes[name] = merged
        except BaseException as exc:
            outcomes[name] = exc
            if hold:
                ready.set()

    workers = [
        Thread(target=publish, args=("first", first, True), daemon=True),
        Thread(target=publish, args=("second", second, False), daemon=True),
    ]
    with _connection(pg_config) as writer, _connection(pg_config) as observer:
        stage_from_extraction(writer, older)
        stage_from_extraction(writer, newer)
        assert _facts(observer, period) == []
        workers[0].start()
        try:
            assert ready.wait(5), "First publisher did not reach its held transaction"
            assert "first" not in outcomes, repr(outcomes)
            name, first_pid = pids.get(timeout=1)
            assert name == "first"
            workers[1].start()
            name, second_pid = pids.get(timeout=5)
            assert name == "second"
            deadline = monotonic() + 3
            while monotonic() < deadline:
                blocked = _query(
                    observer,
                    "SELECT %s = ANY(pg_blocking_pids(%s)), EXISTS ("
                    "SELECT 1 FROM pg_stat_activity WHERE pid = %s "
                    "AND wait_event_type = 'Lock' AND wait_event = 'advisory')",
                    (first_pid, second_pid, second_pid),
                )
                if blocked == [(True, True)]:
                    break
                poll.wait(0.01)  # Bounded polling; success requires observed lock evidence.
            else:
                pytest.fail(f"Second publisher did not wait on first advisory lock: {outcomes!r}")
        finally:
            release.set()
            for worker in workers:
                if worker.ident is not None:
                    worker.join(timeout=15)
        assert all(not worker.is_alive() for worker in workers), "Publisher did not terminate"
        assert not isinstance(outcomes["first"], BaseException), repr(outcomes)
        assert outcomes["first"].status == "succeeded"
        if first_is_newer:
            assert isinstance(outcomes["second"], ValidationError), repr(outcomes)
            assert "older observation" in str(outcomes["second"])
            assert _audit(observer, second.batch.run_id)[0][0] == "failed"
        else:
            assert not isinstance(outcomes["second"], BaseException), repr(outcomes)
            assert outcomes["second"].status == "succeeded"
        facts = _facts(observer, period)
        assert len(facts) == 1
        assert facts[0][3:5] == (2.0, 2000.0)
        assert facts[0][8:] == (newer.batch.run_id, newer.batch.content_hash)
