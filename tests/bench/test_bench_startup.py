"""bench_startup: cold start (process -> `bridge/state: online`) meets budget P6, 1000 channels
(docs/05 §1; docs/11 WP10).

Runs the real pipeline end to end -- fake SysAP, a real in-process broker, `Supervisor` -- the
same style `bench_resync` (WP8) already established for a full-stack timing budget, extended here
to also cover the WP10 addition to cold start: building and publishing Home Assistant discovery
and the `bridge/devices` inventory for every entity, sequentially (P-49), before `bridge/state`
flips to `online`.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import pytest

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.supervisor import Supervisor, SupervisorConfig

from . import _record

pytestmark = pytest.mark.bench

PROFILES_DIR = (
    Path(__file__).resolve().parent.parent.parent / "src" / "freeathome2mqtt" / "profiles"
)
REGISTRY = load_profile_registry(PROFILES_DIR)

BASE = "freeathome2mqtt"
_ROOMS = ("0C", "0D", "0E", "0F")
_P6_BUDGET_SECONDS = 3.0


def _configuration(num_channels: int) -> dict[str, Any]:
    """One device, one switch_actuator channel each, spread across a few rooms -- close to how a
    real installation of this size actually looks, not a single degenerate room (matching
    bench_compile's own device-generation convention).
    """
    devices: dict[str, Any] = {}
    for i in range(num_channels):
        room = _ROOMS[i % len(_ROOMS)]
        devices[f"ABB7F5{i:06X}"] = {
            "displayName": f"Device {i}",
            "deviceId": "910C",
            "interface": "TP",
            "floor": "01",
            "room": room,
            "unresponsive": False,
            "defect": False,
            "channels": {
                "ch0000": {
                    "displayName": f"Channel {i}",
                    "functionID": "7",  # FID_SWITCH_ACTUATOR
                    "floor": "01",
                    "room": room,
                    "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                    "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                }
            },
        }
    return {
        "sysapName": "Bench House",
        "sysap": {},
        "users": {},
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {r: {"name": r} for r in _ROOMS}}}},
        "devices": devices,
    }


async def _wait_until(predicate: Any, *, timeout_seconds: float = 20.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.005)


async def test_bench_startup_1000_channels_meets_p6_budget(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration(1000))
        config = SupervisorConfig(
            sysap_base_url=str(http_client.make_url("")).rstrip("/"),
            sysap_username="installer",
            sysap_password="secret",
            mqtt_host="127.0.0.1",
            mqtt_port=broker.port,
            base_topic=BASE,
            data_dir=tmp_path,
            config_refresh_interval_s=0,
        )
        supervisor = Supervisor(config=config, profiles=REGISTRY, http_session=http_client.session)
        start = time.monotonic()
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            elapsed = time.monotonic() - start
            _record.record("tests/bench/test_bench_startup.py::cold_start_seconds", elapsed)
            assert supervisor._model is not None
            assert len(supervisor._model.entities) == 1000
            assert elapsed <= _P6_BUDGET_SECONDS, (
                f"cold start took {elapsed:.2f}s (budget {_P6_BUDGET_SECONDS}s)"
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=10.0)
