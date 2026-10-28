"""Proof 8: extraction metadata is produced deterministically.

If the metadata that identifies a batch, its raw objects and their checksums
varies between identical runs, then nothing downstream can tell a re-read from a
correction — and traceability, idempotency and revision handling all depend on
being able to tell those apart.
"""

from __future__ import annotations

import hashlib
import uuid

from energy_pipeline.extract import extract_fixture
from energy_pipeline.raw import RAW_PREFIX, route_slug, sha256_hex

EXTRACTED_AT = "2026-09-30T12:00:00Z"


def test_identical_runs_differ_only_in_execution_identity(monthly_fixture):
    """Content is deterministic; execution identity deliberately is not.

    Everything except the run id and the paths it namespaces must be reproducible,
    or nothing downstream can tell a re-read from a correction."""
    first = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT)
    second = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT)

    assert first.batch.run_id != second.batch.run_id
    assert first.batch.content_hash == second.batch.content_hash

    differs = {"run_id", "objects"}
    a = {k: v for k, v in first.batch.manifest().items() if k not in differs}
    b = {k: v for k, v in second.batch.manifest().items() if k not in differs}
    assert a == b


def test_manifest_carries_everything_needed_to_trace_the_extraction(monthly_fixture):
    manifest = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch.manifest()

    for key in (
        "run_id",
        "content_hash",
        "source",
        "route",
        "frequency",
        "measure",
        "window_start",
        "window_end",
        "extracted_at",
        "page_count",
        "record_count",
        "source_record_total",
        "objects",
    ):
        assert key in manifest, key

    assert manifest["extracted_at"] == EXTRACTED_AT
    assert manifest["source_record_total"] == 10
    assert manifest["record_count"] == 10


def test_object_paths_are_unique_and_carry_ingestion_date_and_run(monthly_fixture):
    batch = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch
    paths = [obj.path for obj in batch.objects()]

    assert len(paths) == len(set(paths))
    for path in paths:
        assert path.startswith(f"{RAW_PREFIX}/electric_power_operational_data/")
        assert f"ingestion_date={batch.ingestion_date}" in path
        assert f"run_id={batch.run_id}" in path
        assert path.endswith(".json")


def test_pages_are_numbered_from_one(monthly_fixture):
    batch = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch
    names = [p.filename for p in batch.pages]

    assert names[0] == "page_0001.json"
    assert names == sorted(names)


def test_content_hash_is_content_addressed(monthly_fixture):
    """Same content, same hash. Different content, different hash. A random uuid
    here would make a re-read indistinguishable from a correction."""
    a = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT)
    b = extract_fixture(monthly_fixture, extracted_at="2026-10-01T00:00:00Z")
    c = extract_fixture(monthly_fixture.parent / "revised_2026_07_v1.json")

    assert a.batch.content_hash == b.batch.content_hash, "extraction time must not change content"
    assert a.batch.content_hash != c.batch.content_hash, "different content is different lineage"


def test_run_id_is_a_uuid_and_not_derived_from_content(monthly_fixture):
    """If the id were a hash of the content, two identical extractions would
    collapse onto one path — which is the history loss this design exists to
    prevent. Assert the shape, not just the inequality."""
    runs = {extract_fixture(monthly_fixture).batch.run_id for _ in range(20)}

    assert len(runs) == 20, "run ids must not repeat"
    for value in runs:
        assert uuid.UUID(value).version == 4


def test_checksums_match_the_stored_payload(monthly_fixture):
    batch = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch

    for obj in batch.objects():
        assert obj.sha256 == hashlib.sha256(obj.payload.encode("utf-8")).hexdigest()
    assert sha256_hex("abc") == hashlib.sha256(b"abc").hexdigest()


def test_content_hash_covers_every_page(monthly_fixture):
    batch = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch
    assert len(batch.content_hash) == 64
    assert batch.content_hash != batch.pages[0].sha256


def test_raw_payload_preserves_the_source_values_verbatim(monthly_fixture):
    """Raw is what the source said. A parsed float must not appear here — that is
    the derived layer's job, and a parser bug must not be able to corrupt it."""
    batch = extract_fixture(monthly_fixture, extracted_at=EXTRACTED_AT).batch
    payload = batch.pages[0].payload

    assert '"generation":"331.840757"' in payload, "numeric strings stay strings in raw"
    assert '"generation":null' in payload, "nulls stay null in raw"


def test_route_slug_is_stable_and_readable():
    assert route_slug("electricity/electric-power-operational-data") == (
        "electric_power_operational_data"
    )
