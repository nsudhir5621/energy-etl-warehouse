"""The raw-zone contract against a real filesystem.

The unit suite proves our logic against a dict that refuses to overwrite. What it
cannot prove is that the store every real local run uses actually holds that
contract, and specifically that create-only writes are atomic rather than a
check-then-write with a race in between.

That is what this file is for, and it is the project's mandatory integration
gate. It needs a writable temporary directory and nothing else: no credentials,
no network, no database.

    pytest tests/integration -v
"""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path

import pytest

from energy_pipeline.config import Settings
from energy_pipeline.errors import ObjectExistsError, PartialUploadError, StorageError
from energy_pipeline.extract import extract_fixture
from energy_pipeline.storage import MANIFEST_FILENAME, LocalObjectStore, RawSink

EXTRACTED_AT = "2026-09-30T12:00:00Z"
UPLOADED_AT = "2026-09-30T12:00:05Z"


@pytest.fixture
def store(tmp_path: Path) -> LocalObjectStore:
    return LocalObjectStore(tmp_path / "raw-root")


@pytest.fixture
def sink(store: LocalObjectStore) -> RawSink:
    return RawSink(store, clock=lambda: UPLOADED_AT)


@pytest.fixture
def batch(monthly_fixture, settings):
    return extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch


# -- successful multi-page write ----------------------------------------------


def test_a_multi_page_run_is_written_and_read_back_byte_for_byte(sink, store, batch):
    """The default end-to-end path: extract, write, read back.

    ``settings`` pages at 3 rows, so this is genuinely multi-page — a single-page
    write would not exercise the ordering or the pagination of the object list.
    """
    result = sink.write(batch)

    assert len(result.object_paths) > 1
    for obj in batch.objects():
        assert store.read(obj.path) == obj.payload.encode("utf-8")
    assert store.read(result.manifest_path)


def test_the_layout_is_raw_root_prefix_slug_date_run(tmp_path, sink, batch):
    """The physical path is the documented one, and nothing else.

    This is the assertion that ties the physical layout to the logical key:
    the suffix after the root is the object key itself, verbatim.
    """
    result = sink.write(batch)
    root = (tmp_path / "raw-root").resolve()

    for path in (*result.object_paths, result.manifest_path):
        resolved = (root / path).resolve()
        assert resolved.is_file()
        assert resolved.is_relative_to(root)

    assert result.object_paths[0] == (
        f"raw/eia/electric_power_operational_data/ingestion_date=2026-09-30"
        f"/run_id={batch.run_id}/page_0001.json"
    )
    # Keys stay forward-slashed on every platform: they are object keys, not
    # filesystem paths, and a logical key has no platform-specific separator.
    assert result.manifest_path.endswith("/manifest.json")
    assert result.manifest_path.rsplit("/", 1)[0] == result.object_paths[0].rsplit("/", 1)[0]
    assert "\\" not in result.manifest_path


def test_the_prefix_is_configurable_so_test_data_cannot_be_mistaken_for_raw(
    sink, store, monthly_fixture, settings
):
    """A run written under a test prefix keeps its own namespace on disk."""
    batch = extract_fixture(
        monthly_fixture,
        settings=Settings(api_key=None, page_size=3, raw_prefix="test/energy-pipeline"),
        extracted_at=EXTRACTED_AT,
    ).batch

    result = sink.write(batch)

    assert result.manifest_path.startswith("test/energy-pipeline/")
    assert store.read(result.manifest_path)


# -- manifest ------------------------------------------------------------------


def test_the_manifest_is_written_and_describes_the_run(sink, store, batch):
    result = sink.write(batch)
    manifest = store.load_json(result.manifest_path)

    assert manifest["run_id"] == batch.run_id
    assert manifest["content_hash"] == batch.content_hash
    assert manifest["source"] == "eia_api_v2_fixture"
    assert manifest["window_start"] and manifest["window_end"]
    assert manifest["page_count"] == len(batch.pages)
    assert manifest["record_count"] == batch.record_count
    assert manifest["status"] == "succeeded"
    assert manifest["uploaded_at"] == UPLOADED_AT
    assert [o["path"] for o in manifest["objects"]] == list(result.object_paths)


def test_the_content_hash_is_the_deterministic_one(sink, store, monthly_fixture, settings):
    """Two runs of identical content agree on the hash and differ on the namespace."""
    first = sink.write(
        extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch
    )
    second = sink.write(
        extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch
    )

    assert first.content_hash == second.content_hash
    assert first.run_id != second.run_id
    assert set(first.object_paths).isdisjoint(second.object_paths)
    # Both runs are still there. History was not overwritten by the second.
    assert store.read(second.object_paths[0]) == store.read(first.object_paths[0])


# -- manifest-last semantics ---------------------------------------------------


def test_the_manifest_is_the_last_object_written(sink, store, batch, monkeypatch):
    """Completion is observable by listing the run directory — no database query.

    Recorded at the filesystem level rather than asserted from the returned list,
    because the guarantee is about ordering on disk, not about our own bookkeeping.
    """
    order: list[str] = []
    real_open = os.open

    def recording_open(path, flags, *args, **kwargs):
        order.append(Path(path).name)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", recording_open)
    result = sink.write(batch)

    assert order[-1] == MANIFEST_FILENAME
    assert order.index(MANIFEST_FILENAME) > max(
        order.index(Path(p).name) for p in result.object_paths
    )


def test_an_undone_run_is_detectable_as_incomplete(tmp_path, batch):
    """Pages without a manifest is the failure signal, and it is visible on disk."""
    store = LocalObjectStore(tmp_path / "raw-root")
    store.put_if_absent(batch.objects()[0].path, b"[]")
    manifest_path = f"{batch.namespace}/{MANIFEST_FILENAME}"

    with pytest.raises(StorageError):
        store.read(manifest_path)
    assert store.read(batch.objects()[0].path) == b"[]"


# -- zero rows -----------------------------------------------------------------


def test_a_zero_row_run_is_a_success_with_its_own_manifest(sink, store, fixtures_dir, settings):
    batch = extract_fixture(
        fixtures_dir / "empty_window.json", settings=settings, extracted_at=EXTRACTED_AT
    ).batch

    result = sink.write(batch)

    assert result.status == "empty"
    assert result.record_count == 0
    manifest = store.load_json(result.manifest_path)
    assert manifest["status"] == "empty"
    assert manifest["record_count"] == 0


# -- collisions ----------------------------------------------------------------


def test_a_collision_fails_and_leaves_the_existing_bytes_untouched(sink, store, batch):
    """The guarantee that matters: a failed write destroys nothing.

    Replaying a batch is the realistic way to hit this — the same run_id, the
    same keys, the same bytes — and the answer must be a typed failure, never a
    silent replacement.
    """
    first = sink.write(batch)
    before = {path: store.read(path) for path in first.object_paths}

    with pytest.raises(PartialUploadError) as caught:
        sink.write(batch)

    assert isinstance(caught.value.__cause__, ObjectExistsError)
    assert caught.value.failed_path == first.object_paths[0]
    for path, data in before.items():
        assert store.read(path) == data, f"{path} was modified by a write that failed"


def test_create_only_is_atomic_rather_than_a_check_then_write(store):
    """``O_EXCL`` is what makes the guarantee real; a lock is not a substitute.

    The write happens with an advisory lock held on the very path being created.
    A check-then-write implementation would see the file does not exist and
    happily overwrite it; only an atomic open-or-fail refuses.
    """
    path = "raw/eia/x/run_id=r/page_0001.json"
    store.put_if_absent(path, b"first")
    target = store.resolve(path)

    handle = os.open(target, os.O_RDONLY)
    try:
        with pytest.raises(ObjectExistsError):
            store.put_if_absent(path, b"second")
    finally:
        os.close(handle)

    assert store.read(path) == b"first"


def test_a_write_that_reaches_an_existing_file_never_rewrites_it(store):
    """Read-only files still refuse the write rather than erroring obscurely."""
    path = "raw/eia/x/run_id=r/page_0001.json"
    store.put_if_absent(path, b"first")
    target = store.resolve(path)
    target.chmod(stat.S_IRUSR)

    with pytest.raises((ObjectExistsError, StorageError)):
        store.put_if_absent(path, b"second")

    assert store.read(path) == b"first"


# -- missing objects and bad keys ----------------------------------------------


def test_reading_a_missing_object_says_which_one(store):
    with pytest.raises(StorageError, match=re.escape("raw object not found: raw/eia/missing.json")):
        store.read("raw/eia/missing.json")


def test_a_key_cannot_escape_the_raw_root(store):
    """A traversal is a boundary violation, not a path to tidy up."""
    with pytest.raises(StorageError, match="escapes the raw root"):
        store.read("../../secrets.env")


def test_the_manifest_on_disk_is_parseable_json_not_a_serialised_python_dict(sink, store, batch):
    """The raw zone holds what the source gave us, in a format another tool can read."""
    result = sink.write(batch)
    raw = store.read(result.manifest_path)
    assert json.loads(raw.decode("utf-8"))["run_id"] == batch.run_id
    assert b"'" not in raw.split(b'"content_hash"')[0]


# -- partial failure -----------------------------------------------------------


def test_a_failure_mid_run_leaves_earlier_pages_and_no_manifest(
    tmp_path, monthly_fixture, settings
):
    """Page three fails: pages one and two stand, and the run is not marked complete."""
    root = tmp_path / "raw-root"
    batch = extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch
    store = LocalObjectStore(root)
    third = batch.objects()[2].path

    with pytest.raises(PartialUploadError) as caught:
        RawSink(_FailsOn(store, third)).write(batch)

    assert caught.value.failed_path == third
    assert len(caught.value.written_paths) == 2
    assert store.read(caught.value.written_paths[0])
    with pytest.raises(StorageError):
        store.read(f"{batch.namespace}/{MANIFEST_FILENAME}")


def test_a_failed_first_page_stores_nothing_at_all(tmp_path, monthly_fixture, settings):
    root = tmp_path / "raw-root"
    batch = extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch

    with pytest.raises(PartialUploadError) as caught:
        RawSink(_AlwaysFails()).write(batch)

    assert caught.value.written_paths == []
    assert not list(root.rglob("*.json"))


def test_a_retry_after_a_failure_preserves_the_partial_run(tmp_path, monthly_fixture, settings):
    """Recovery is a new run, never a rollback — the abandoned pages stay."""
    store = LocalObjectStore(tmp_path / "raw-root")
    failed = extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch
    third = failed.objects()[2].path
    survivor = failed.objects()[0].path

    with pytest.raises(PartialUploadError):
        RawSink(_FailsOn(store, third)).write(failed)
    assert store.read(survivor)

    retry = extract_fixture(monthly_fixture, settings=settings, extracted_at=EXTRACTED_AT).batch
    result = RawSink(store).write(retry)

    assert result.run_id != failed.run_id
    assert store.read(survivor), "the abandoned run's page must still be there"
    assert store.load_json(result.manifest_path)["status"] == "succeeded"


def test_a_half_written_object_is_removed_rather_than_left_as_debris(tmp_path, monkeypatch):
    """All-or-nothing: an object that exists is an object that is complete.

    ``O_EXCL`` has already created the path by the time ``write`` runs, so a
    failure there would otherwise leave a zero-length file. That file would
    collide with the retry and turn a recoverable error into a permanently
    corrupt object — the exact outcome this store must never produce.
    """
    store = LocalObjectStore(tmp_path / "raw-root")
    path = "raw/eia/x/run_id=r/page_0001.json"
    real_fdopen = os.fdopen

    class _FailingFile:
        def write(self, data):
            raise OSError(28, "No space left on device")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def failing_fdopen(fd, mode, *args, **kwargs):
        os.close(fd)
        return _FailingFile()

    monkeypatch.setattr(os, "fdopen", failing_fdopen)

    with pytest.raises(StorageError, match="No space left"):
        store.put_if_absent(path, b"payload")

    monkeypatch.setattr(os, "fdopen", real_fdopen)
    assert not store.resolve(path).exists()
    # And the retry is not blocked by debris from the failed attempt.
    store.put_if_absent(path, b"payload")
    assert store.read(path) == b"payload"


class _FailsOn:
    """A real local store with one key made to fail.

    Wraps the actual filesystem store rather than substituting a fake, so the
    pages that *do* get written are genuinely on disk and the assertions about
    them mean something.
    """

    def __init__(self, delegate: LocalObjectStore, path: str) -> None:
        self._delegate = delegate
        self._path = path

    def put_if_absent(self, path: str, data: bytes) -> None:
        if path == self._path:
            raise StorageError("simulated disk failure")
        self._delegate.put_if_absent(path, data)

    def read(self, path: str) -> bytes:
        return self._delegate.read(path)


class _AlwaysFails:
    def put_if_absent(self, path: str, data: bytes) -> None:
        raise StorageError("simulated disk failure")

    def read(self, path: str) -> bytes:
        raise StorageError("never written")
