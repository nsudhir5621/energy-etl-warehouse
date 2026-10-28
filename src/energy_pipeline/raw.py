"""Raw batch representation: what the source returned, and nothing else.

The raw zone answers *"what exactly did the source return us, and when?"*. That
question has two halves, and conflating them is the bug this module avoids:

* **`run_id` — execution identity.** A UUID4, unique per extraction *run* and
  derived from nothing. It is what the raw object path is keyed on, so a
  re-extraction of byte-identical content lands in its own objects and the
  earlier extraction's history is never overwritten.
* **`content_hash` — content identity.** A deterministic hash of the payload. Two
  runs that received identical bytes share it, which is how a re-read is told
  apart from a genuine correction.

The two are never mixed. Execution identity must not be derivable from the thing
it observed: if it were, two runs returning identical content would resolve to
one path, and the audit layer could no longer answer "what did we receive, and
when?" — only "what did we receive at some point". Identical content may be
deduplicated *later*, or many audit rows pointed at one content hash; it is never
collapsed at the point where history is written.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .pagination import Page

#: Where raw objects live, as a *logical object key* prefix. The store resolves
#: it under ``<RAW_ROOT>/``; nothing upstream of the store knows where a key
#: lands on disk.
RAW_PREFIX = "raw/eia"

_ROUTE_SLUG_RE = re.compile(r"[^a-z0-9]+")


def route_slug(route: str) -> str:
    """``electricity/electric-power-operational-data`` -> ``electric_power_operational_data``.

    Only the last path segment is used: the leading category is the same for
    every electricity route and would only add noise to the path.
    """
    leaf = route.rstrip("/").rsplit("/", 1)[-1]
    return _ROUTE_SLUG_RE.sub("_", leaf.lower()).strip("_")


def sha256_hex(payload: str | bytes) -> str:
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class RawObject:
    """One immutable raw object, not yet uploaded anywhere."""

    path: str
    payload: str
    record_count: int
    sha256: str


@dataclass(frozen=True, slots=True)
class RawPage:
    page_number: int
    offset: int
    payload: str
    record_count: int
    sha256: str
    warnings: tuple[str, ...] = ()

    @property
    def filename(self) -> str:
        return f"page_{self.page_number:04d}.json"


@dataclass(frozen=True, slots=True)
class RawBatch:
    """A single extraction, with the metadata needed to trace and reproduce it."""

    run_id: str
    source: str
    route: str
    frequency: str
    measure: str
    window_start: str
    window_end: str
    extracted_at: str
    pages: tuple[RawPage, ...] = field(default_factory=tuple)
    #: Records the source claims exist in the window, from ``response.total``.
    source_record_total: int | None = None
    #: Logical key prefix, backend-independent. Configurable so that test objects
    #: can be written under a prefix that cannot be mistaken for production raw
    #: data. Same value in every real run.
    prefix: str = RAW_PREFIX

    @property
    def ingestion_date(self) -> str:
        """Date component of the partition. Derived, never separately supplied."""
        return self.extracted_at[:10]

    @property
    def record_count(self) -> int:
        """Rows actually retrieved across all pages."""
        return sum(page.record_count for page in self.pages)

    @property
    def content_hash(self) -> str:
        """Deterministic identity of the *content*, independent of when it arrived.

        Two runs with the same hash received byte-identical pages; without a
        match, different content. That single fact answers "is this batch new
        data, a re-read, or a correction?" — the question ``run_id`` cannot.
        """
        return compute_content_hash(self.pages)

    @property
    def slug(self) -> str:
        return route_slug(self.route)

    @property
    def namespace(self) -> str:
        """This run's logical key prefix, without a trailing slash.

        The single place a run's location is derived. Pages, the manifest and
        every test that asserts on "where did this run go" read it from here, so
        the layout is defined once and cannot drift between them.
        """
        return (
            f"{self.prefix}/{self.slug}/ingestion_date={self.ingestion_date}/run_id={self.run_id}"
        )

    def objects(self) -> tuple[RawObject, ...]:
        """The immutable objects this batch consists of, with their final paths.

        Keyed on ``run_id`` so no two extractions — identical content or not —
        can land on the same path.
        """
        return tuple(
            RawObject(
                path=f"{self.namespace}/{page.filename}",
                payload=page.payload,
                record_count=page.record_count,
                sha256=page.sha256,
            )
            for page in self.pages
        )

    def manifest(self) -> dict[str, Any]:
        """Serialisable extraction metadata, suitable for the audit layer.

        Records both identities deliberately. The audit row must be able to say
        "this run received content X" without the content ever being rewritten
        by a later run.
        """
        return {
            "run_id": self.run_id,
            "content_hash": self.content_hash,
            "source": self.source,
            "route": self.route,
            "frequency": self.frequency,
            "measure": self.measure,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "extracted_at": self.extracted_at,
            "page_count": len(self.pages),
            "record_count": self.record_count,
            "source_record_total": self.source_record_total,
            "objects": [
                {
                    "path": obj.path,
                    "record_count": obj.record_count,
                    "sha256": obj.sha256,
                }
                for obj in self.objects()
            ],
        }


def canonical_json(value: Any) -> str:
    """Stable serialisation. Used for content-addressing, so key order in the
    source's JSON must not change the content hash."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def compute_content_hash(pages: Sequence[RawPage]) -> str:
    """Deterministic identity of an extraction's content.

    A hash over every page's checksum, in order: identical pages in an identical
    sequence give an identical value, and any change to any byte changes it. This
    is the value reported as ``RawBatch.content_hash``.
    """
    digest = hashlib.sha256()
    for page in pages:
        digest.update(page.sha256.encode("ascii"))
    return digest.hexdigest()


def new_run_id() -> str:
    """A genuinely unique identifier for one extraction *execution*.

    A UUID4, deliberately unrelated to the content. Execution identity answers
    "which run wrote this", and a run is an event — it must not be derivable from
    the thing it observed, because a re-read of unchanged data is still two
    events. Deriving it from the content (or from a timestamp, which two
    processes can share) would let two executions collapse onto one raw path,
    which is exactly the history loss this field exists to prevent.

    Uniqueness is the sole requirement, so it is drawn from the OS CSPRNG rather
    than computed. The cost is that a run is not reproducible by re-derivation:
    to re-run an extraction you run it again and get a new run id, which is the
    correct behaviour, not a limitation.
    """
    return str(uuid.uuid4())


def build_batch(
    pages: Sequence[Page],
    *,
    source: str,
    route: str,
    frequency: str,
    measure: str,
    window_start: str,
    window_end: str,
    extracted_at: str,
    prefix: str = RAW_PREFIX,
) -> RawBatch:
    """Assemble a batch from parsed pages, preserving each payload verbatim."""
    raw_pages = tuple(
        RawPage(
            page_number=page.page_number,
            offset=page.offset,
            payload=canonical_json(page.rows),
            record_count=len(page.rows),
            sha256=sha256_hex(canonical_json(page.rows)),
            warnings=tuple(page.warnings),
        )
        for page in pages
    )
    return RawBatch(
        run_id=new_run_id(),
        source=source,
        route=route,
        frequency=frequency,
        measure=measure,
        window_start=window_start,
        window_end=window_end,
        extracted_at=extracted_at,
        pages=raw_pages,
        source_record_total=pages[0].total if pages else None,
        prefix=prefix,
    )
