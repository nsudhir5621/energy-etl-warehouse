"""The raw-zone contract, enforced against a store.

Two properties, and both are about not losing history:

* **Two runs never share a namespace.** ``run_id`` is a UUID4 per execution, so a
  re-extraction of byte-identical data lands somewhere new.
* **Nothing is ever overwritten.** Unique naming alone would only make a
  collision *unlikely*; the create-only contract makes it *impossible*, and a
  collision surfaces as a failure with the existing object untouched.

The default store here is a fake that obeys the create-only contract — a dict
that refuses to overwrite. That is deliberate: the logic under test is ours, so
emulating more of a storage service would test less of ours. The same contract is
exercised against a real filesystem in ``tests/integration/test_local_raw_storage.py``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from energy_pipeline.config import Settings
from energy_pipeline.errors import (
    ObjectExistsError,
    PartialUploadError,
    StorageConfigError,
)
from energy_pipeline.extract import extract_fixture
from energy_pipeline.storage import (
    MANIFEST_FILENAME,
    STATUS_EMPTY,
    STATUS_SUCCEEDED,
    InMemoryObjectStore,
    LocalObjectStore,
    RawSink,
)

EXTRACTED_AT = "2026-09-30T12:00:00Z"
UPLOADED_AT = "2026-09-30T12:00:05Z"
PREFIX = "raw/eia/electric_power_operational_data/ingestion_date=2026-09-30"


def _sink(store=None) -> RawSink:
    return RawSink(store or InMemoryObjectStore(), clock=lambda: UPLOADED_AT)


def _batch(fixture, settings, **kwargs):
    return extract_fixture(fixture, settings=settings, extracted_at=EXTRACTED_AT, **kwargs).batch


# -- object naming -------------------------------------------------------------


def test_two_runs_write_to_different_paths(monthly_fixture, settings):
    """Different execution identity, therefore a different namespace."""
    first = _batch(monthly_fixture, settings)
    second = _batch(monthly_fixture, settings)
    assert first.run_id != second.run_id

    store = InMemoryObjectStore()
    a, b = _sink(store).write(first), _sink(store).write(second)

    assert set(a.object_paths).isdisjoint(b.object_paths)
    written = {a.manifest_path, b.manifest_path}
    assert set(a.object_paths) | set(b.object_paths) | written == set(store.objects)


def test_identical_content_shares_a_hash_but_not_a_path(monthly_fixture, settings):
    """The whole point of separating the two identities."""
    first = _batch(monthly_fixture, settings)
    second = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    _sink(store).write(first)
    _sink(store).write(second)

    assert first.content_hash == second.content_hash, "the source returned the same bytes"
    assert first.run_id != second.run_id, "but they were two executions"
    for path in store.objects:
        assert f"run_id={first.run_id}" in path or f"run_id={second.run_id}" in path


def test_pages_of_one_run_share_its_namespace_and_the_layout_is_derived(monthly_fixture, settings):
    """The path is deterministic from the run metadata, and every page of a run
    lands in one namespace."""
    batch = _batch(monthly_fixture, settings)
    result = _sink().write(batch)

    assert len(result.object_paths) > 1, "the test settings page at 3 rows"
    for page, obj in zip(batch.pages, batch.objects(), strict=True):
        assert obj.path == f"{PREFIX}/run_id={batch.run_id}/{page.filename}"
    assert result.manifest_path == f"{PREFIX}/run_id={batch.run_id}/{MANIFEST_FILENAME}"


# -- create-only / no-overwrite ------------------------------------------------


def test_a_second_write_to_an_existing_path_fails_and_preserves_the_object(
    monthly_fixture, settings
):
    """An intentional collision, forced by replaying the same batch.

    The precondition must fail *and* leave the stored bytes exactly as they were.
    A sink that deleted and re-uploaded would satisfy a weaker test and quietly
    destroy history."""
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()

    _sink(store).write(batch)
    before = dict(store.objects)
    assert before, "the first write must have stored something"

    with pytest.raises(PartialUploadError) as caught:
        _sink(store).write(batch)

    assert isinstance(caught.value.__cause__, ObjectExistsError)
    assert store.objects == before, "the existing raw objects must be untouched"


def test_a_run_with_different_content_cannot_replace_live_history(
    monthly_fixture, settings, edge_cases_fixture
):
    """The stronger case: same path, *different* bytes.

    Aiming a second, genuinely different extraction at Run A's namespace must fail
    and leave Run A's objects exactly as Run A wrote them."""
    run_a = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    _sink(store).write(run_a)
    before = dict(store.objects)

    impostor = dataclasses.replace(_batch(edge_cases_fixture, settings), run_id=run_a.run_id)
    assert impostor.content_hash != run_a.content_hash, "the payloads must genuinely differ"

    with pytest.raises(PartialUploadError):
        _sink(store).write(impostor)

    assert store.objects == before, "Run A's raw objects are unchanged"


# -- content identity ----------------------------------------------------------


def test_the_stored_payload_is_what_the_source_returned(monthly_fixture, settings):
    """Raw is not transformed on its way to the store."""
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    _sink(store).write(batch)

    for obj in batch.objects():
        assert store.objects[obj.path] == obj.payload.encode("utf-8")


def test_numeric_strings_and_nulls_are_not_rewritten(monthly_fixture, settings):
    """A parsed float must never appear in a raw object. That is the derived
    layer's job, and a parser bug must not be able to reach back and corrupt what
    we were told."""
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    _sink(store).write(batch)

    payload = b"".join(store.objects[obj.path] for obj in batch.objects())
    assert b'"generation":"331.840757"' in payload, "numeric strings stay strings in raw"
    assert b'"generation":null' in payload, "nulls stay null in raw"


def test_awkward_rows_survive_the_round_trip(edge_cases_fixture, settings):
    """Rollups, nulls, negatives and unclassified codes are all still there after
    storage. Nothing is filtered on the way in."""
    batch = _batch(edge_cases_fixture, settings)
    store = InMemoryObjectStore()
    _sink(store).write(batch)

    stored = b"".join(store.objects[obj.path] for obj in batch.objects())
    assert b"null" in stored
    assert b"-2335" in stored, "negative generation is valid data"
    assert b'"ALL"' in stored and b'"AOR"' in stored, "rollup energy sources survive"
    assert b'"US"' in stored and b'"NEW"' in stored, "rollup geographies survive"


# -- manifest ------------------------------------------------------------------


def test_the_manifest_answers_what_did_this_extraction_produce(monthly_fixture, settings):
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    result = _sink(store).write(batch)

    manifest = store.load_json(result.manifest_path)
    assert manifest["run_id"] == batch.run_id
    assert manifest["content_hash"] == batch.content_hash
    assert manifest["source"] == "eia_api_v2_fixture"
    assert manifest["extracted_at"] == EXTRACTED_AT
    assert manifest["uploaded_at"] == UPLOADED_AT
    assert manifest["window_start"] and manifest["window_end"]
    assert manifest["page_count"] == len(batch.pages)
    assert manifest["record_count"] == batch.record_count
    assert manifest["status"] == STATUS_SUCCEEDED

    assert [o["path"] for o in manifest["objects"]] == list(result.object_paths)
    assert [o["record_count"] for o in manifest["objects"]] == [p.record_count for p in batch.pages]
    for obj in manifest["objects"]:
        assert obj["path"] in store.objects, "every path it names must actually exist"


# -- failure behaviour ---------------------------------------------------------


def test_a_failure_on_page_three_keeps_pages_one_and_two(monthly_fixture, settings):
    batch = _batch(monthly_fixture, settings)
    objects = batch.objects()
    assert len(objects) >= 3, "this test needs a third page to fail on"

    store = InMemoryObjectStore()
    store.fail_on = {objects[2].path}

    with pytest.raises(PartialUploadError) as caught:
        _sink(store).write(batch)

    assert caught.value.written_paths == [o.path for o in objects[:2]]
    assert caught.value.failed_path == objects[2].path
    assert set(store.objects) == {o.path for o in objects[:2]}
    assert not any(MANIFEST_FILENAME in p for p in store.objects)


def test_a_failed_run_is_not_marked_successful(monthly_fixture, settings):
    """No manifest means not finished. Anyone listing the store can tell."""
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    store.fail_on = {batch.objects()[0].path}

    with pytest.raises(PartialUploadError):
        _sink(store).write(batch)

    assert store.objects == {}, "a run that fails on page one stores nothing at all"


def test_rerunning_after_a_failure_preserves_the_partial_run(monthly_fixture, settings):
    """Recovery is a new execution, not a rollback. Raw history is append-only, so
    the abandoned run's objects are left exactly where they are."""
    batch = _batch(monthly_fixture, settings)
    store = InMemoryObjectStore()
    store.fail_on = {batch.objects()[2].path}
    with pytest.raises(PartialUploadError):
        _sink(store).write(batch)

    abandoned = dict(store.objects)
    assert abandoned, "the partial run must have left something behind"
    store.fail_on = set()

    retry = _batch(monthly_fixture, settings)  # a fresh extraction: new run id
    result = _sink(store).write(retry)

    for path, data in abandoned.items():
        assert store.objects[path] == data, "the earlier attempt must be left intact"
    assert set(abandoned).isdisjoint(result.object_paths)


# -- empty run -----------------------------------------------------------------


def test_an_empty_run_is_a_success_not_a_failure(fixtures_dir, settings):
    """A window with no rows is a legitimate outcome — EIA may simply not have
    published a new period yet. It must be distinguishable from a run that never
    happened, which it is by having a manifest at all."""
    batch = _batch(fixtures_dir / "empty_window.json", settings)
    store = InMemoryObjectStore()
    result = _sink(store).write(batch)

    assert result.status == STATUS_EMPTY
    assert result.record_count == 0

    manifest = store.load_json(result.manifest_path)
    assert manifest["status"] == STATUS_EMPTY
    assert manifest["record_count"] == 0
    assert manifest["run_id"] == batch.run_id
    assert manifest["content_hash"] == batch.content_hash
    assert all(o["record_count"] == 0 for o in manifest["objects"])


def test_an_empty_run_records_that_the_source_answered_with_nothing(fixtures_dir, settings):
    """The one page the source did return is stored verbatim — an empty list. The
    run asked, the run got an answer, and that is worth keeping."""
    batch = _batch(fixtures_dir / "empty_window.json", settings)
    store = InMemoryObjectStore()
    result = _sink(store).write(batch)

    assert len(result.object_paths) == 1
    assert store.objects[result.object_paths[0]] == b"[]"


def test_an_empty_run_still_gets_a_manifest_in_its_own_namespace(fixtures_dir, settings):
    batch = _batch(fixtures_dir / "empty_window.json", settings)
    result = _sink().write(batch)

    assert result.manifest_path == f"{PREFIX}/run_id={batch.run_id}/{MANIFEST_FILENAME}"


# -- configuration boundary ----------------------------------------------------


def test_the_raw_storage_defaults_are_a_plain_local_filesystem():
    """A fresh clone runs with no database and no credentials."""
    settings = Settings.from_env({})
    assert settings.raw_root == Path("data/raw")
    assert settings.raw_prefix == "raw/eia"


def test_a_local_store_with_no_root_fails_before_any_write():
    with pytest.raises(StorageConfigError, match="RAW_ROOT"):
        LocalObjectStore("   ")
