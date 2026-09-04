"""bench_compile: compile() meets budget P7 and scales near-linearly (docs/05 §8; docs/11 WP3)."""

from __future__ import annotations

import time
from typing import Any

import pytest

from freeathome2mqtt.model.compiler import CompileOptions, compile
from freeathome2mqtt.model.profiles import ProfileRegistry, build_registry, parse_profile

pytestmark = pytest.mark.bench

# docs/05 §1 P7: config parse + compile <= 400ms end-to-end. compile() alone (this benchmark's
# scope -- parsing is a separate, prior step) must fit comfortably inside that with headroom for
# the parse this benchmark does not measure.
_P7_BUDGET_SECONDS = 0.4

_SWITCH_PROFILE = {
    "id": "switch_actuator",
    "functions": ["FID_SWITCH_ACTUATOR"],
    "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    "commands": {
        "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01", "optimistic": "state"}
    },
}

_ROOMS = ("0C", "0D", "0E", "0F")


def _registry() -> ProfileRegistry:
    return build_registry([parse_profile(_SWITCH_PROFILE, source="<bench>")])


def _make_config(num_channels: int) -> dict[str, Any]:
    """One device, one channel each, spread across a few rooms -- close to how a real installation
    of this size actually looks, not a single degenerate room.
    """
    devices = {}
    for i in range(num_channels):
        room = _ROOMS[i % len(_ROOMS)]
        devices[f"DEV{i:06d}"] = {
            "displayName": f"Device {i}",
            "interface": "TP",
            "floor": "01",
            "room": room,
            "channels": {
                "ch0000": {
                    "displayName": f"Channel {i}",
                    "functionID": "7",
                    "floor": "01",
                    "room": room,
                    "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                    "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                }
            },
        }
    return {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {r: {"name": r} for r in _ROOMS}}}},
        "devices": devices,
    }


def test_bench_compile_1000_channels_meets_p7_budget(benchmark: Any) -> None:
    config = _make_config(1000)
    registry = _registry()
    model = benchmark(compile, config, registry, CompileOptions())
    assert len(model.entities) == 1000
    assert benchmark.stats["mean"] < _P7_BUDGET_SECONDS


def test_bench_compile_scales_near_linearly() -> None:
    registry = _registry()
    config_1000 = _make_config(1000)
    config_2500 = _make_config(2500)

    # Minimum of a few reps, not a single sample: scheduling noise on a shared runner affects the
    # mean far more than the floor, and it is the floor that reflects the algorithm's real cost.
    duration_1000 = min(_time_once(config_1000, registry) for _ in range(5))
    duration_2500 = min(_time_once(config_2500, registry) for _ in range(5))

    # 2.5x the channels; a generous 6x ceiling only fails genuinely super-linear behaviour (e.g. an
    # accidental O(n^2) scan), not ordinary noise.
    assert duration_2500 < duration_1000 * 6.0


def _time_once(config: dict[str, Any], registry: ProfileRegistry) -> float:
    start = time.perf_counter()
    compile(config, registry, CompileOptions())
    return time.perf_counter() - start
