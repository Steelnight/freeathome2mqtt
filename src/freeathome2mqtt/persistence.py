"""``entities.json``: atomic writes, versioning (docs/02 §8; docs/07 §4.1; docs/11 WP8).

Runtime state the bridge itself owns -- per-entity aliases and options -- lives here, separate from
the user-owned, read-only ``config.yaml`` (docs/07 §1). `EntitiesStore` is written only through the
bridge API's mutation path (`mqtt/bridge_api.py`, WP9); this module only has to be correct about
*how* that state round-trips to disk, not *when* it changes.

``discovery.json`` (docs/07 §4.2) is implemented here too, as `DiscoveryStore`: a topic -> payload
hash map that lets a restart with an unchanged installation publish zero discovery messages
(docs/05 §5), and lets the bridge retract discovery topics it published in a previous run but no
longer would (P-35's cross-restart case -- the in-memory old-model-vs-new-model diff in
`supervisor._diff_and_apply` only catches removals that happen *while this process is running*).

The configuration snapshot cache docs/05 §5 originally proposed is **not** here, and will not be:
WP17 measured what it would save and dropped it (see docs/05 §5, which now carries the numbers).
Its useful half -- not republishing unchanged discovery -- is `DiscoveryStore` above.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

CURRENT_VERSION = 1


class PersistenceError(Exception):
    """``entities.json`` exists but is not valid JSON, or names an unsupported future version."""


@dataclass(slots=True)
class EntityRecord:
    """One ``entities.<id>`` entry (docs/07 §4.1): an alias, per-entity options, and when the
    entity was first seen -- never pruned on its own (a device merely off the bus keeps its
    alias), only on an explicit `EntitiesStore.remove`.
    """

    alias: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None


def _migrate(raw: dict[str, Any]) -> dict[str, Any]:
    """Upgrade `raw` to `CURRENT_VERSION`, one bump at a time.

    Only version 1 has ever existed, so there is nothing to migrate *from* yet -- this function is
    the seam the next version bump hangs its migration step on, per docs/07 §4.1's "a migration
    function per bump", not a no-op to delete once a real migration is needed.
    """
    version = raw.get("version", 0)
    if not isinstance(version, int) or version > CURRENT_VERSION:
        raise PersistenceError(
            f"entities.json version {version!r} is newer than supported ({CURRENT_VERSION})"
        )
    return raw


class EntitiesStore:
    """Loads/saves ``entities.json``; the only thing that ever writes it (docs/02 §6)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self.entities: dict[str, EntityRecord] = {}

    def load(self) -> None:
        """Populate `entities` from disk. A missing file is a fresh install, not an error."""
        if not self._path.exists():
            return
        try:
            raw = orjson.loads(self._path.read_bytes())
        except orjson.JSONDecodeError as exc:
            raise PersistenceError(f"{self._path} is not valid JSON: {exc}") from exc
        raw = _migrate(raw)
        entities = raw.get("entities", {})
        if not isinstance(entities, dict):
            raise PersistenceError(f"{self._path}: 'entities' is not an object")
        self.entities = {
            entity_id: EntityRecord(
                alias=record.get("alias"),
                options=dict(record.get("options", {})),
                first_seen=record.get("first_seen"),
            )
            for entity_id, record in entities.items()
        }

    async def save(self) -> None:
        """Atomic snapshot write (docs/02 §8 shutdown step 6)."""
        payload = {
            "version": CURRENT_VERSION,
            "entities": {
                entity_id: {
                    "alias": record.alias,
                    "options": record.options,
                    "first_seen": record.first_seen,
                }
                for entity_id, record in self.entities.items()
            },
        }
        await atomic_write(self._path, orjson.dumps(payload))

    def alias_for(self, entity_id: str) -> str | None:
        record = self.entities.get(entity_id)
        return record.alias if record is not None else None

    def options_for(self, entity_id: str) -> dict[str, Any]:
        record = self.entities.get(entity_id)
        return dict(record.options) if record is not None else {}

    def set_alias(self, entity_id: str, alias: str, *, first_seen: str | None = None) -> None:
        record = self.entities.setdefault(entity_id, EntityRecord(first_seen=first_seen))
        record.alias = alias

    def set_options(self, entity_id: str, options: dict[str, Any]) -> None:
        """Merge `options` into whatever is already persisted (docs/04 §5's `entity/options`
        is a set of overrides, not a wholesale replacement -- setting `debounce_ms` must not
        erase a previously-set `enabled`).
        """
        record = self.entities.setdefault(entity_id, EntityRecord())
        record.options.update(options)

    def remove(self, entity_id: str) -> None:
        """Explicit prune only (docs/07 §4.1) -- never called automatically on a topology diff."""
        self.entities.pop(entity_id, None)


def _hash_payload(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class DiscoveryStore:
    """Loads/saves ``discovery.json`` (docs/07 §4.2): the only record of what Home Assistant
    discovery topics this bridge has published, across restarts.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self.hashes: dict[str, str] = {}

    def load(self) -> None:
        """Populate `hashes` from disk. A missing file is a fresh install, not an error."""
        if not self._path.exists():
            return
        try:
            raw = orjson.loads(self._path.read_bytes())
        except orjson.JSONDecodeError as exc:
            raise PersistenceError(f"{self._path} is not valid JSON: {exc}") from exc
        raw = _migrate(raw)
        topics = raw.get("topics", {})
        if not isinstance(topics, dict):
            raise PersistenceError(f"{self._path}: 'topics' is not an object")
        self.hashes = dict(topics)

    async def save(self) -> None:
        payload = {"version": CURRENT_VERSION, "topics": self.hashes}
        await atomic_write(self._path, orjson.dumps(payload))

    def is_changed(self, topic: str, payload: bytes) -> bool:
        return self.hashes.get(topic) != _hash_payload(payload)

    def mark(self, topic: str, payload: bytes) -> None:
        self.hashes[topic] = _hash_payload(payload)

    def remove(self, topic: str) -> None:
        self.hashes.pop(topic, None)


async def atomic_write(path: Path, data: bytes) -> None:
    """Temp file in the same directory, ``fsync``, ``os.replace`` (docs/02 §8) -- via the executor
    so a slow disk never blocks the loop.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _atomic_write_sync, path, data)


def _atomic_write_sync(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise
