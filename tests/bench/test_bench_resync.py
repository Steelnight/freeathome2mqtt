"""bench_resync: resync after a WS outage is fast and costs exactly one config request (docs/05
§1 P8; §8; docs/11 WP8).

The budget is about recovery cost, not outage duration -- docs/06 §4's diff-and-publish-deltas
mechanism does the same fixed amount of work (one `GET /api/rest/configuration`, one diff, one
publish pass) regardless of how long the WS was down. A short simulated outage stands in for
docs/05 §8's literal "60 s WS outage", the same deviation WP6's `bench_ingest` already documents
for its own traffic window.

**The clock starts at the drop, not at the reconnect (changed in WP13).** It used to start only
after a poll loop had *observed* `reconnect_count` increase, by which point the resync -- fired
from the same `on_connected` hook that increments that counter -- had usually already finished:
the measurement came out at 10-15 microseconds against a 1.5 s budget, so the timing assertion
was passing without measuring anything, and the test's real content was its request-count
assertion. Timing the whole outage-to-correct-state recovery instead gives a well-defined
interval with a defined start, at the cost of including this test's own configured reconnect
backoff (`link_backoff_initial`, 20 ms here) in the number. That is the honest trade: a budget
whose start point is "whenever the poller happened to wake up" is not a budget.
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

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_P8_BUDGET_SECONDS = 1.5


def _configuration(devices: dict[str, Any]) -> dict[str, Any]:
    return {
        "sysapName": "Test House",
        "sysap": {},
        "users": {},
        "floorplan": {"floors": {}},
        "devices": devices,
    }


def _switch_device(*, state: str) -> dict[str, Any]:
    return {
        "displayName": "Switch",
        "interface": "TP",
        "unresponsive": False,
        "unresponsiveCounter": 0,
        "defect": False,
        "channels": {
            "ch0000": {
                "displayName": "Switch",
                "functionID": "7",  # FID_SWITCH_ACTUATOR
                "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                "outputs": {"odp0000": {"pairingID": 256, "value": state}},
                "floor": "01",
                "room": "01",
            }
        },
        "floor": "01",
        "room": "01",
    }


async def _wait_until(predicate: Any, *, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.005)


async def test_bench_resync_after_ws_outage_meets_p8_budget(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(state="0")}))
        config = SupervisorConfig(
            sysap_base_url=str(http_client.make_url("")).rstrip("/"),
            sysap_username="installer",
            sysap_password="secret",
            mqtt_host="127.0.0.1",
            mqtt_port=broker.port,
            base_topic=BASE,
            data_dir=tmp_path,
            config_refresh_interval_s=0,
            link_backoff_initial=0.02,
            link_backoff_cap=0.1,
        )
        supervisor = Supervisor(config=config, profiles=REGISTRY, http_session=http_client.session)
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._ws is not None and supervisor._model is not None
            config_path = "/fhapi/v1/api/rest/configuration"
            requests_before = fake.request_count(config_path)
            reconnects_before = supervisor._ws.reconnect_count

            # The outage: the WS drops; a change happens on the SysAP while it is disconnected --
            # exactly the scenario a resync has to catch up on once the link comes back.
            start = time.monotonic()
            await fake.drop_websocket()
            fake.set_datapoint(SERIAL, "ch0000", "odp0000", "1")

            await _wait_until(
                lambda: (
                    supervisor._ws is not None
                    and supervisor._ws.reconnect_count > reconnects_before
                ),
                timeout_seconds=10.0,
            )

            idx = supervisor._model.by_id[f"{SERIAL}_ch0000"]
            await _wait_until(
                lambda: supervisor._state is not None and supervisor._state.values[idx][0] is True,
                timeout_seconds=_P8_BUDGET_SECONDS + 5.0,
            )
            elapsed = time.monotonic() - start
            _record.record("tests/bench/test_bench_resync.py::resync_seconds", elapsed)

            assert elapsed <= _P8_BUDGET_SECONDS
            assert fake.request_count(config_path) == requests_before + 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)
