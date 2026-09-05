"""The clean-interpreter half of `bench_memory` (docs/05 §1 P9; §6; docs/12 WP13).

Run as a child process by `test_bench_memory.py` so the number it reports is the bridge's own
footprint, not pytest's: this process imports exactly what the container ships (the real runtime
dependencies) and builds exactly what docs/05 §6's table enumerates -- `Entity` objects, the
`ingress` dict, `values` as `list[list]`, and pre-serialised discovery payloads. It prints one
JSON object to stdout and exits.

Deliberately not a broker/WebSocket run: the socket half of the pipeline is what the in-process
trend test exercises, and adding an embedded broker here would put *its* memory back into the
number this exists to isolate.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# The child is executed by path, so `src/` is not importable unless the parent's environment
# already made it so; a plain `sys.path` insert keeps the child runnable standalone too.
_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import orjson  # noqa: E402

from freeathome2mqtt.bus.state import StateStore  # noqa: E402
from freeathome2mqtt.model.codecs import build_codec  # noqa: E402
from freeathome2mqtt.model.entity import AttrKind, Binding, Entity  # noqa: E402

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"


def _rss_kib() -> int:
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return -1


def _entity(idx: int, attrs_per_entity: int) -> Entity:
    names = tuple(f"attr{i}" for i in range(attrs_per_entity))
    return Entity(
        idx=idx,
        id=f"{SERIAL}_ch{idx:04d}",
        profile="test_profile",
        name=f"Entity {idx}",
        area="Area",
        device_serial=SERIAL,
        channel_id=f"ch{idx:04d}",
        attr_names=names,
        attr_kinds=tuple(AttrKind.STATE for _ in names),
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=f"{BASE}/test{idx}/availability",
        optimistic=False,
        discovery=(
            (
                f"homeassistant/sensor/{SERIAL}_ch{idx:04d}/config",
                orjson.dumps(
                    {
                        "unique_id": f"{SERIAL}_ch{idx:04d}",
                        "name": f"Entity {idx}",
                        "state_topic": f"{BASE}/test{idx}",
                        "availability_topic": f"{BASE}/test{idx}/availability",
                        "device": {"identifiers": [SERIAL], "name": "Test device"},
                    }
                ),
            ),
        ),
    )


def main() -> int:
    entity_count = int(sys.argv[1])
    attrs_per_entity = int(sys.argv[2])

    # Import the rest of the real runtime surface before the baseline reading, so the number
    # below isolates the *model's* growth rather than counting library import cost as model cost.
    import aiohttp  # noqa: F401, PLC0415
    import aiomqtt  # noqa: F401, PLC0415

    baseline_kib = _rss_kib()

    entities = [_entity(i, attrs_per_entity) for i in range(entity_count)]
    int_codec = build_codec("int")
    ingress: dict[str, Binding] = {
        f"{SERIAL}/ch{entity_idx:04d}/odp{attr_idx:04d}": Binding(
            entity_idx=entity_idx,
            attr_idx=attr_idx,
            decode=int_codec.decode,
            kind=AttrKind.STATE,
            attr_bit=1 << attr_idx,
        )
        for entity_idx in range(entity_count)
        for attr_idx in range(attrs_per_entity)
    }
    state = StateStore(entities)
    for entity_idx in range(entity_count):
        for attr_idx in range(attrs_per_entity):
            state.seed(entity_idx, attr_idx, entity_idx * attrs_per_entity + attr_idx)

    print(
        json.dumps(
            {
                "entities": len(entities),
                "bindings": len(ingress),
                "rss_kib": _rss_kib(),
                "rss_kib_baseline": baseline_kib,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
