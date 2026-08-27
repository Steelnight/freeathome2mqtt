"""``entities.json``: atomic writes, versioning (docs/02 §8; docs/07 §4.1; docs/11 WP8).

Runtime state the bridge itself owns -- per-entity aliases and options -- lives here, separate from
the user-owned, read-only ``config.yaml`` (docs/07 §1). `EntitiesStore` is written only through the
bridge API's mutation path (`mqtt/bridge_api.py`, WP9); this module only has to be correct about
*how* that state round-trips to disk, not *when* it changes.

Only ``entities.json`` is implemented here. `discovery.json` (docs/07 §4.2, last-published-payload
hashes) and the configuration snapshot cache (docs/05 §5) are deferred to the work packages that
actually need them -- WP10's discovery change-detection and WP9's startup-optimisation cache --
rather than built speculatively ahead of a caller.
"""

from __future__ import annotations

import asyncio
import contextlib
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
