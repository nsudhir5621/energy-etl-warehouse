"""Immutable raw storage, and the storage boundary.

This module is the whole storage contract. ``extract.py`` produces a
:class:`~energy_pipeline.raw.RawBatch` and this one writes it; the arrow between
them carries no store type, which is what keeps the extraction layer
independent of where raw objects land.

Three guarantees, one structural and two requested:

* **Structural — unique keys.** Every object lives under
  ``<prefix>/<route slug>/ingestion_date=…/run_id=<uuid4>/``. ``run_id`` is a
  UUID4 drawn per execution, so no two executions can resolve to the same key.
* **Create-only writes.** Unique naming is not enough, because a bug or a
  replayed job can still aim at a key that already exists. Every write must fail
  on a key that is taken, leaving the existing bytes untouched. We never
  delete-and-replace: a delete would itself be a mutation of history, and the
  whole point is that history is append-only.
* **The manifest is the completion marker.** Pages are written first, manifest
  last, and the manifest is create-only like everything else. Its presence is
  therefore what marks a run complete: pages without a manifest is a run that
  died part-way, and that distinction is visible to anyone listing the raw zone
  without querying a database.

**Stores.** :class:`LocalObjectStore` is the store used by every default run; it
needs nothing but a filesystem. :class:`InMemoryObjectStore` is the test fake.
Both satisfy the same two-method protocol, which is what lets the sink's own
logic be tested without a filesystem.

The logical object key ``raw/eia/…`` is resolved by the store itself, under its
root directory; nothing upstream of the store knows where a key lands.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .errors import ObjectExistsError, PartialUploadError, StorageConfigError, StorageError
from .raw import RawBatch, canonical_json

MANIFEST_FILENAME = "manifest.json"

#: Statuses a manifest can carry. Only ever written on success — a failed run
#: writes no manifest at all, so its absence is the failure signal.
STATUS_SUCCEEDED = "succeeded"
STATUS_EMPTY = "empty"


class ObjectStore(Protocol):
    """The two operations this sink needs from a store.

    Deliberately tiny. Everything interesting here — ordering, the manifest, the
    create-only contract, partial-failure behaviour — is *our* logic, so it is
    testable against an in-memory fake without emulating a storage service.
    """

    def put_if_absent(self, path: str, data: bytes) -> None:
        """Write ``data`` at ``path``, failing if the path is already taken."""

    def read(self, path: str) -> bytes:
        """Read an object back, for verification."""


@dataclass(frozen=True, slots=True)
class UploadResult:
    """What one successful write produced."""

    run_id: str
    content_hash: str
    status: str
    manifest_path: str
    object_paths: tuple[str, ...]
    record_count: int


@dataclass(frozen=True, slots=True)
class RawSink:
    """Writes a raw batch into any :class:`ObjectStore`, immutably."""

    store: ObjectStore
    #: Injected so the manifest timestamp is deterministic under test.
    clock: Any = None

    def write(self, batch: RawBatch) -> UploadResult:
        """Persist every page, then the manifest. Raises on any failure.

        The manifest is the authoritative audit record — nothing is repeated
        on each object, so there is one source of truth, not two that could
        disagree.
        """
        uploaded_at = _timestamp(self.clock)
        objects = batch.objects()
        manifest_path = f"{batch.namespace}/{MANIFEST_FILENAME}"
        written: list[str] = []

        # Pages first, manifest last. The manifest's existence is therefore the
        # completion marker, and a run that dies half-way leaves pages with no
        # manifest — which anyone listing the raw zone can see.
        current = ""
        try:
            for _page, obj in zip(batch.pages, objects, strict=True):
                current = obj.path
                self._put(self.store, obj.path, obj.payload.encode("utf-8"))
                written.append(obj.path)

            manifest = self._manifest(batch, uploaded_at=uploaded_at)
            current = manifest_path
            self._put(self.store, manifest_path, canonical_json(manifest).encode("utf-8"))
        except StorageError as exc:
            # The objects already written are left exactly where they are. Raw
            # history is append-only, so the recovery is a fresh run under a new
            # run id — never a rollback, which would itself mutate stored history.
            raise PartialUploadError(
                f"raw upload failed at {current} after {len(written)} object(s) "
                f"were written: {exc}",
                written_paths=written,
                failed_path=current,
            ) from exc

        return UploadResult(
            run_id=batch.run_id,
            content_hash=batch.content_hash,
            status=manifest["status"],
            manifest_path=manifest_path,
            object_paths=tuple(written),
            record_count=batch.record_count,
        )

    @staticmethod
    def _manifest(batch: RawBatch, *, uploaded_at: str) -> dict[str, Any]:
        """What exactly did this extraction produce?

        Everything here is already computed by the batch -- the manifest restates
        it with the two things only the sink knows: when it was written, and
        whether there was anything to write. No field is padded in for appearance.
        """
        return {
            **batch.manifest(),
            # An empty run is a real, successful outcome (no new period published
            # yet, or a window genuinely containing no rows). It gets its own
            # status so it is never confused with a run that never happened.
            # Keyed on rows, not pages: one empty page is still zero data.
            "status": STATUS_SUCCEEDED if batch.record_count else STATUS_EMPTY,
            "uploaded_at": uploaded_at,
        }

    @staticmethod
    def _put(store: ObjectStore, path: str, data: bytes) -> None:
        try:
            store.put_if_absent(path, data)
        except StorageError:
            raise
        except Exception as exc:  # a store SDK raises its own exception types
            raise StorageError(f"failed to write {path}: {exc}") from exc


class LocalObjectStore:
    """:class:`ObjectStore` on a local filesystem -- the store a default run uses.

    An object-store-*style* implementation over a filesystem. It gives the raw
    zone the two properties that make it trustworthy -- create-only writes and
    byte-exact payloads -- using the filesystem's own guarantees.

    **Create-only is done with ``O_CREAT | O_EXCL``**, an atomic
    open-or-fail at the OS level. The obvious alternative,
    ``if not path.exists(): write(...)``, has a race between the check and the
    write that another process -- or a second pipeline run -- can slip through, and
    a race in an append-only raw zone is exactly the failure this contract exists
    to prevent.
    """

    def __init__(self, root: str | Path) -> None:
        if not str(root).strip():
            raise StorageConfigError("raw storage root is not configured (set RAW_ROOT)")
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def resolve(self, path: str) -> Path:
        """Map a logical object key to a file path, refusing to escape the root.

        The keys this project writes are ours, but the store is a boundary and a
        traversal here would read or clobber arbitrary files. Checked rather than
        assumed, and checked on the *resolved* path so a symlink cannot slip past.
        """
        candidate = (self._root / path).resolve()
        root = self._root.resolve()
        if candidate != root and root not in candidate.parents:
            raise StorageError(f"object key escapes the raw root: {path!r}")
        return candidate

    def put_if_absent(self, path: str, data: bytes) -> None:
        target = self.resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Atomic create-or-fail. O_EXCL is what makes a collision a collision
            # rather than a race that silently overwrites history.
            handle = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError as exc:
            raise ObjectExistsError(
                f"raw object already exists and was not overwritten: {path}"
            ) from exc
        except OSError as exc:
            raise StorageError(f"failed to create {path}: {exc}") from exc

        try:
            with os.fdopen(handle, "wb") as file:
                file.write(data)
        except OSError as exc:
            # A half-written object is debris from this attempt, not history. It
            # is removed so the retry does not collide with a corrupt file and so
            # "this page exists" keeps meaning "this page is complete" — an
            # all-or-nothing guarantee. This is the only delete in this
            # module, and it can only ever remove a file this call created
            # moments earlier.
            target.unlink(missing_ok=True)
            raise StorageError(f"failed to write {path}: {exc}") from exc

    def read(self, path: str) -> bytes:
        try:
            return self.resolve(path).read_bytes()
        except FileNotFoundError as exc:
            raise StorageError(f"raw object not found: {path}") from exc
        except OSError as exc:
            raise StorageError(f"failed to read {path}: {exc}") from exc

    def load_json(self, path: str) -> Any:
        """Read an object back as JSON. Verification, not part of the sink path."""
        return json.loads(self.read(path).decode("utf-8"))


@dataclass
class InMemoryObjectStore:
    """A fake that obeys the create-only contract, for tests.

    Not an emulation of anything — it is just a dict that refuses to overwrite,
    which is the single behaviour the sink depends on. Anything richer would be
    testing the store rather than our logic.
    """

    objects: dict[str, bytes] = field(default_factory=dict)
    #: Paths that should raise, to simulate a failure part-way through a run.
    fail_on: set[str] = field(default_factory=set)

    def put_if_absent(self, path: str, data: bytes) -> None:
        if path in self.fail_on:
            raise StorageError(f"simulated transport failure on {path}")
        if path in self.objects:
            raise ObjectExistsError(f"raw object already exists and was not overwritten: {path}")
        self.objects[path] = data

    def read(self, path: str) -> bytes:
        if path not in self.objects:
            raise StorageError(f"raw object not found: {path}")
        return self.objects[path]

    def load_json(self, path: str) -> Any:
        return json.loads(self.read(path).decode("utf-8"))


def _timestamp(clock: Any) -> str:
    if clock is not None:
        return clock()
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
