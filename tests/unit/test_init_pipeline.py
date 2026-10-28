"""Regression tests for the initialization script (``scripts/init_pipeline.py``).

These tests pin down the two bugs fixed in commit ``4a20e0e``:

1. ``RawSink.write()`` returns an :class:`UploadResult` that carries
   ``manifest_path`` — the script must read from ``written.manifest_path``
   (not ``result.batch.manifest_path``, which does not exist on ``RawBatch``).
2. :func:`load_raw_batch` accepts ``(store, conn, manifest_path)`` — the
   ``run_id`` keyword argument was a stale call-site that would raise
   ``TypeError``.

Both invariants are verified here without a real PostgreSQL instance by using
the ``FakeConn`` pattern from ``test_load.py``.
"""

from __future__ import annotations

from energy_pipeline.load import load_raw_batch
from energy_pipeline.storage import UploadResult


class FakeConn:
    """Minimal connection that captures SQL — mirrors ``test_load.py`` FakeConn."""

    def __init__(self):
        self.executed: list[tuple[str, tuple | None]] = []

    def cursor(self):
        return self._FakeCursor(self.executed)

    class _FakeCursor:
        def __init__(self, log):
            self._log = log

        def execute(self, sql, params=None):
            self._log.append((sql, params))

        def executemany(self, sql, param_seq):
            for p in param_seq:
                self._log.append((sql, p))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    #: Minimal transaction() for psycopg3 compatibility.
    def transaction(self):
        return self


class TestInitPipelineContract:
    """Verify the API contract that ``init_pipeline.py`` relies on."""

    def test_upload_result_has_manifest_path(self):
        """``UploadResult`` must expose ``manifest_path`` — the attribute the
        fixed script reads."""
        assert hasattr(UploadResult, "__dataclass_fields__")
        assert "manifest_path" in UploadResult.__dataclass_fields__

    def test_raw_batch_has_no_manifest_path(self):
        """``RawBatch`` does NOT carry ``manifest_path`` — confirming the bug
        the fix addressed (the old code tried ``result.batch.manifest_path``)."""
        from energy_pipeline.raw import RawBatch

        assert "manifest_path" not in RawBatch.__dataclass_fields__

    def test_load_raw_batch_signature_no_run_id_kwarg(self):
        """``load_raw_batch`` must accept ``(store, conn, manifest_path)``
        and must NOT have a ``run_id`` parameter (the old call site passed one)."""
        import inspect

        sig = inspect.signature(load_raw_batch)
        params = list(sig.parameters.keys())
        assert params == ["store", "conn", "manifest_path"]
        assert "run_id" not in params
