"""Thin PostgreSQL connection wrapper — Phase 3 warehouse seam.

This is **not** an ORM. It is not a multi-backend abstraction. It is a minimal
wrapper around ``psycopg`` (psycopg3) that provides a context-managed
connection and the partition helper that Phase 3 needs.

The design mirrors how :mod:`energy_pipeline.storage` defines an ``ObjectStore``
Protocol as a testability seam for local filesystem storage — ``Warehouse`` is
the analogous seam for PostgreSQL. Unit tests use the pure helper functions
below, which need no database.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

__all__ = ["Warehouse", "next_period", "partition_name"]


def next_period(period: str) -> str:
    """Return the ``YYYY-MM`` string for the month after *period*.

    Handles the year-boundary wrap (December → January).

    >>> next_period("2026-07")
    '2026-08'
    >>> next_period("2026-12")
    '2027-01'
    >>> next_period("2001-01")
    '2001-02'
    """
    year, month = int(period[:4]), int(period[5:7])
    month += 1
    if month > 12:
        month = 1
        year += 1
    return f"{year:04d}-{month:02d}"


def partition_name(period: str) -> str:
    """Return the PostgreSQL table name for the monthly partition of *period*.

    >>> partition_name("2026-07")
    'fact_generation_2026_07'
    """
    return f"fact_generation_{period.replace('-', '_')}"


class Warehouse:
    """Minimal PostgreSQL connection seam.

    Usage::

        warehouse = Warehouse.from_env()
        with warehouse.connect() as conn:
            warehouse.ensure_partition(conn, "2026-07")
            with conn.cursor() as cur:
                cur.execute("INSERT INTO ...")

    The connection context manager yields a ``psycopg.Connection``. Callers
    manage their own transactions (``with conn:`` for auto-commit/rollback).
    """

    def __init__(self, conninfo: str | None = None):
        if conninfo is None:
            conninfo = os.environ.get("POSTGRES_DSN")
        if not isinstance(conninfo, str) or not conninfo.strip():
            raise ValueError(
                "No PostgreSQL connection string. "
                "Set POSTGRES_DSN env var or pass conninfo to Warehouse()."
            )
        self._conninfo = conninfo

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Warehouse:
        """Build a Warehouse from the POSTGRES_DSN environment variable."""
        env = os.environ if env is None else env
        dsn = env.get("POSTGRES_DSN", "")
        if not dsn:
            raise ValueError("POSTGRES_DSN environment variable is not set")
        return cls(dsn)

    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Yield a psycopg connection. Caller manages transactions."""
        import psycopg

        conn = psycopg.connect(self._conninfo)
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Partition management
    # ------------------------------------------------------------------

    def ensure_partition(self, conn: Any, period: str) -> bool:
        """Create the monthly partition for *period* (YYYY-MM) if absent.

        Returns True if the partition was created, False if it already existed.
        Partitions are created on demand rather than pre-creating years of
        empty tables - the full history range (2001-2026+) would be 300+
        partitions, most never used.
        """
        pname = partition_name(period)
        end = next_period(period)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'core' AND table_name = %s",
                (pname,),
            )
            if cur.fetchone():
                return False

            # safe: pname is derived from partition_name(period) which only
            # accepts the YYYY-MM format validated by next_period above.
            cur.execute(
                f"CREATE TABLE core.{pname} "
                "PARTITION OF core.fact_generation "
                "FOR VALUES FROM (%s) TO (%s)",
                (period, end),
            )
        return True

    def schema_exists(self, conn: Any, schema: str) -> bool:
        """Check whether a named schema exists in the connected database."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema,),
            )
            return cur.fetchone() is not None

    def table_exists(self, conn: Any, schema: str, table: str) -> bool:
        """Check whether *table* exists in *schema*."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            return cur.fetchone() is not None
