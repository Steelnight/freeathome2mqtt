"""Tests for persistence.py: entities.json load/save, versioning, atomic writes (docs/02 §8;
docs/07 §4.1; docs/11 WP8).
"""

from __future__ import annotations

import os
from pathlib import Path

import orjson
import pytest

from freeathome2mqtt.persistence import (
    CURRENT_VERSION,
    DiscoveryStore,
    EntitiesStore,
    PersistenceError,
    atomic_write,
)


def test_load_on_a_missing_file_leaves_entities_empty(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    store.load()
    assert store.entities == {}


async def test_save_then_load_round_trips_alias_options_and_first_seen(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    store = EntitiesStore(path)
    store.set_alias("ABB7F500E17A_ch0003", "kitchen_ceiling", first_seen="2026-01-14T09:12:00Z")
    store.entities["ABB7F500E17A_ch0003"].options = {"optimistic": False, "debounce_ms": 100}
    await store.save()

    reloaded = EntitiesStore(path)
    reloaded.load()
    record = reloaded.entities["ABB7F500E17A_ch0003"]
    assert record.alias == "kitchen_ceiling"
    assert record.options == {"optimistic": False, "debounce_ms": 100}
    assert record.first_seen == "2026-01-14T09:12:00Z"


async def test_save_writes_the_current_version(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    store = EntitiesStore(path)
    await store.save()
    raw = orjson.loads(path.read_bytes())
    assert raw["version"] == CURRENT_VERSION


def test_load_rejects_malformed_json(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    path.write_bytes(b"{not json")
    store = EntitiesStore(path)
    with pytest.raises(PersistenceError):
        store.load()


def test_load_rejects_a_future_version(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    path.write_bytes(orjson.dumps({"version": CURRENT_VERSION + 1, "entities": {}}))
    store = EntitiesStore(path)
    with pytest.raises(PersistenceError):
        store.load()


def test_load_rejects_entities_that_is_not_an_object(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    path.write_bytes(orjson.dumps({"version": CURRENT_VERSION, "entities": []}))
    store = EntitiesStore(path)
    with pytest.raises(PersistenceError):
        store.load()


def test_alias_for_and_options_for_default_when_unknown(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    assert store.alias_for("unknown") is None
    assert store.options_for("unknown") == {}


def test_set_options_merges_into_existing_options(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    store.set_options("ABB_ch0001", {"optimistic": False})
    store.set_options("ABB_ch0001", {"debounce_ms": 100})
    assert store.options_for("ABB_ch0001") == {"optimistic": False, "debounce_ms": 100}


def test_set_options_overwrites_a_previously_set_key(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    store.set_options("ABB_ch0001", {"enabled": True})
    store.set_options("ABB_ch0001", {"enabled": False})
    assert store.options_for("ABB_ch0001") == {"enabled": False}


def test_set_options_creates_the_entity_record_if_unknown(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    store.set_options("ABB_ch0002", {"enabled": False})
    assert "ABB_ch0002" in store.entities
    assert store.alias_for("ABB_ch0002") is None


def test_remove_prunes_an_entity(tmp_path: Path) -> None:
    store = EntitiesStore(tmp_path / "entities.json")
    store.set_alias("ABB_ch0001", "hallway")
    store.remove("ABB_ch0001")
    assert "ABB_ch0001" not in store.entities


def test_unknown_entity_ids_are_retained_across_a_reload(tmp_path: Path) -> None:
    # docs/07 §4.1: a device temporarily off the bus must not lose its alias -- nothing besides
    # an explicit remove() prunes an entry, so a fresh load of an untouched file keeps it too.
    path = tmp_path / "entities.json"
    path.write_bytes(
        orjson.dumps(
            {
                "version": CURRENT_VERSION,
                "entities": {"ABB_ch9999": {"alias": "spare_room", "options": {}}},
            }
        )
    )
    store = EntitiesStore(path)
    store.load()
    assert store.alias_for("ABB_ch9999") == "spare_room"


# ------------------------------------------------------------------------------- DiscoveryStore


def test_discovery_store_load_on_a_missing_file_leaves_hashes_empty(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    store.load()
    assert store.hashes == {}


def test_discovery_store_is_changed_true_for_an_unseen_topic(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    assert store.is_changed("homeassistant/switch/x/config", b'{"a":1}') is True


def test_discovery_store_mark_then_is_changed_is_false_for_the_same_payload(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    store.mark("homeassistant/switch/x/config", b'{"a":1}')
    assert store.is_changed("homeassistant/switch/x/config", b'{"a":1}') is False


def test_discovery_store_is_changed_true_for_a_different_payload(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    store.mark("homeassistant/switch/x/config", b'{"a":1}')
    assert store.is_changed("homeassistant/switch/x/config", b'{"a":2}') is True


def test_discovery_store_remove_forgets_a_topic(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    store.mark("homeassistant/switch/x/config", b'{"a":1}')
    store.remove("homeassistant/switch/x/config")
    assert store.is_changed("homeassistant/switch/x/config", b'{"a":1}') is True


async def test_discovery_store_save_then_load_round_trips_hashes(tmp_path: Path) -> None:
    path = tmp_path / "discovery.json"
    store = DiscoveryStore(path)
    store.mark("homeassistant/switch/x/config", b'{"a":1}')
    await store.save()

    reloaded = DiscoveryStore(path)
    reloaded.load()
    assert reloaded.is_changed("homeassistant/switch/x/config", b'{"a":1}') is False


def test_discovery_store_load_rejects_non_object_topics(tmp_path: Path) -> None:
    path = tmp_path / "discovery.json"
    path.write_bytes(orjson.dumps({"version": CURRENT_VERSION, "topics": []}))
    store = DiscoveryStore(path)
    with pytest.raises(PersistenceError, match="'topics' is not an object"):
        store.load()


async def test_atomic_write_creates_the_file_with_exact_content(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "file.json"
    await atomic_write(path, b'{"a": 1}')
    assert path.read_bytes() == b'{"a": 1}'


async def test_atomic_write_leaves_no_temp_file_behind(tmp_path: Path) -> None:
    path = tmp_path / "file.json"
    await atomic_write(path, b"data")
    leftovers = [p for p in tmp_path.iterdir() if p.name != "file.json"]  # noqa: ASYNC240
    assert leftovers == []


async def test_atomic_write_overwrites_an_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "file.json"
    await atomic_write(path, b"first")
    await atomic_write(path, b"second")
    assert path.read_bytes() == b"second"


async def test_atomic_write_cleans_up_its_temp_file_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "sub" / "file.json"

    def _flaky_replace(_src: str, _dst: str) -> None:
        raise OSError("simulated failure")

    monkeypatch.setattr(os, "replace", _flaky_replace)
    with pytest.raises(OSError, match="simulated failure"):
        await atomic_write(target, b"data")

    assert list(target.parent.iterdir()) == []
