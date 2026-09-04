"""The soak test (docs/10 §8; docs/11 WP12): nightly, 24 h, against the fake SysAP with a chaos
script -- random WS drops/hangs, broker restarts, `502` bursts, config changes, and continuous
synthetic traffic with periodic bursts. `@pytest.mark.soak` -- excluded from the fast suite
entirely (`pyproject.toml`'s own marker docstring), run only via `pytest -m soak`.

Duration is `FAH2MQTT_SOAK_DURATION_S` (default 20 s here so the harness itself is exercised
locally/in a PR without waiting a day); `.github/workflows/soak.yml`'s nightly cron sets it to the
literal 86400 docs/10 §8 specifies. `_run_chaos_phase` runs for that many seconds of **wall-clock**
time, pacing each chaos cycle to `cycle_budget` so the traffic generator keeps running between
events -- the traffic *between* the chaos is what a soak is actually measuring, since RSS growth
and unbounded-collection bugs are time-dependent.

This was not always true, and the bug is worth recording because the test looked like it passed
the whole time: the loop used to advance a nominal `elapsed` counter by `duration_s / 10` per
cycle instead of reading the clock, which made the iteration count exactly ten for *any* duration
and the whole phase return in under a second. The nightly "24 hour" soak was really a ~2 second
one. `test_soak_survives_chaos_with_correct_final_state` now asserts the phase consumed at least
90% of the duration it was given, so the env var cannot silently go back to being decorative.

Chaos *frequency* still scales with the run rather than being a literal wall-clock interval --
the same kind of documented timing deviation `bench_ingest` (WP6) and `bench_resync` (WP8) use for
their own budgets -- but `_MAX_CYCLE_BUDGET_S` caps the spacing at 5 minutes so a long run gets
proportionally more chaos instead of the same ten events spread thinner. At the nightly 86400 s
that is ~288 cycles: a WS drop or hang every 5 minutes and a config change every 20, against
docs/10 §8's "every 5-30 minutes" and "every 30 minutes".
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from pathlib import Path
from typing import Any

import pytest

from fakes.fake_broker import free_port, running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.supervisor import Supervisor, SupervisorConfig

pytestmark = pytest.mark.soak

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY = load_profile_registry(PROFILES_DIR)

SERIAL_PREFIX = "ABB7F500"
BASE = "freeathome2mqtt"
_CHANNEL_COUNT = 20
_DEFAULT_DURATION_S = 20.0
_TRAFFIC_HZ = 20.0  # scaled down from docs/10 §8's 50 dp/s -- shape matters more than rate here
_BURST_SIZE = 100  # scaled down from the spec's 500-frame bursts, same reasoning
_RSS_GROWTH_CEILING = 0.10  # docs/10 §8: < 10% over the run
_CHAOS_CYCLES = 10
"""How many chaos cycles a short run gets, so the local default keeps its seconds-long shape."""

_MIN_CYCLE_BUDGET_S = 0.2
"""Floor, so a very short duration still leaves each cycle room to actually complete."""

_MAX_CYCLE_BUDGET_S = 300.0
"""Ceiling, so a long run gets *more* chaos rather than the same ten events spread thinner.

At the nightly 86400 s this pins one cycle every 5 minutes -- the fast end of docs/10 §8's
"random WS drops and hangs every 5-30 minutes" -- which puts `_run_chaos_cycle`'s `cycle % 4`
config change at every 20 minutes, close to that section's "every 30 minutes". Without the cap,
`duration_s / _CHAOS_CYCLES` would make a 24 h run one chaos event every 2.4 hours.
"""


def _duration_s() -> float:
    raw = os.environ.get("FAH2MQTT_SOAK_DURATION_S")
    return float(raw) if raw else _DEFAULT_DURATION_S


def _serial(n: int) -> str:
    return f"{SERIAL_PREFIX}{n:04X}"


def _switch_channel(*, state: str = "0") -> dict[str, Any]:
    return {
        "displayName": "Switch",
        "functionID": "7",  # FID_SWITCH_ACTUATOR
        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 256, "value": state}},
        "floor": "01",
        "room": "01",
    }


def _device(n: int) -> dict[str, Any]:
    return {
        "displayName": f"Switch {n}",
        "interface": "TP",
        "unresponsive": False,
        "defect": False,
        "channels": {"ch0000": _switch_channel()},
        "floor": "01",
        "room": "01",
    }


def _configuration(count: int) -> dict[str, Any]:
    return {
        "sysapName": "Soak Test House",
        "sysap": {},
        "users": {},
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"01": {"name": "Room"}}}}},
        "devices": {_serial(n): _device(n) for n in range(count)},
    }


def _soak_config(tmp_path: Path, mqtt_port: int, http_client: Any) -> SupervisorConfig:
    return SupervisorConfig(
        sysap_base_url=str(http_client.make_url("")).rstrip("/"),
        sysap_username="installer",
        sysap_password="secret",
        mqtt_host="127.0.0.1",
        mqtt_port=mqtt_port,
        base_topic=BASE,
        data_dir=tmp_path,
        config_refresh_interval_s=0,
        grace_seconds=0.5,
        link_backoff_initial=0.02,
        link_backoff_cap=0.5,
        reload_debounce_s=0.05,
        reload_min_interval_s=0.1,
        command_debounce_s=0.02,
        reconcile_delay_s=0.2,
    )


async def _wait_until(predicate, *, timeout_seconds: float = 10.0, interval: float = 0.02) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


def _current_rss_kb() -> int | None:
    """Current (not peak) RSS in KiB via `/proc/self/status` -- Linux only, matching this
    project's own primary-deployment-target assumption (docs/00 §5: a Raspberry Pi). Returns
    `None` where unavailable so the assertion can skip cleanly rather than fail spuriously on an
    unsupported platform.
    """
    try:
        with Path("/proc/self/status").open(encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


class _ErrorCounter(logging.Handler):
    """Counts ERROR-and-above log records emitted anywhere under the `freeathome2mqtt` logger
    tree during the chaos run -- docs/10 §8's "zero unhandled exceptions" assertion. Handled
    failures (backoff, reconcile, codec errors) all log at WARNING or below (docs/06 §9); an
    ERROR here means something genuinely fell through.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def _traffic_task(fake: FakeSysAp, *, hz: float, stop: asyncio.Event) -> None:
    """Continuous synthetic datapoint changes at ~`hz`, cycling through every seeded channel."""
    interval = 1.0 / hz
    n = 0
    while not stop.is_set():
        serial = _serial(n % _CHANNEL_COUNT)
        value = "1" if n % 2 == 0 else "0"
        # push_datapoint_change (not push_ws_frame) so the fake's own stored configuration --
        # this test's ground truth -- stays in sync with what it broadcasts (docs/10 §8's final
        # state comparison needs that to hold).
        await fake.push_datapoint_change(serial, "ch0000", "odp0000", value)
        n += 1
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)


async def _traffic_burst(fake: FakeSysAp, *, size: int) -> None:
    datapoints: dict[str, str] = {}
    for i in range(size):
        serial = _serial(i % _CHANNEL_COUNT)
        value = "1" if i % 2 == 0 else "0"
        fake.set_datapoint(serial, "ch0000", "odp0000", value)
        datapoints[f"{serial}/ch0000/odp0000"] = value
    await fake.push_ws_frame({"datapoints": datapoints})


async def _run_chaos_cycle(
    *,
    fake: FakeSysAp,
    supervisor: Supervisor,
    cycle: int,
) -> None:
    """One round of docs/10 §8's chaos script, scaled down to run in seconds rather than hours."""
    await _traffic_burst(fake, size=_BURST_SIZE)

    if cycle % 2 == 0:
        await fake.drop_websocket()
        await _wait_until(
            lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 1
        )
    else:
        fake.hang_websocket()
        await asyncio.sleep(0.05)
        fake.restore_websocket()

    if cycle % 3 == 0:
        fake.set_error("/fhapi/v1/api/rest/datapoint", 502, times=3)

    if cycle % 4 == 0:
        # A config change (docs/10 §8): rename a device, then signal the topology frame that
        # triggers a debounced resync (docs/06 §4).
        config = _configuration(_CHANNEL_COUNT)
        config["devices"][_serial(0)]["displayName"] = f"Renamed {cycle}"
        fake.set_configuration(config)
        await fake.push_ws_frame({"devices": [_serial(0)]})

    await asyncio.sleep(0.05)


async def _run_chaos_phase(*, fake: FakeSysAp, supervisor: Supervisor, duration_s: float) -> None:
    """Continuous traffic plus the chaos cycle loop, for `duration_s` of *wall-clock* time
    (see the module docstring), then a clean stop of the traffic generator.
    """
    stop_traffic = asyncio.Event()
    traffic_task = asyncio.create_task(_traffic_task(fake, hz=_TRAFFIC_HZ, stop=stop_traffic))

    cycle_budget = min(max(duration_s / _CHAOS_CYCLES, _MIN_CYCLE_BUDGET_S), _MAX_CYCLE_BUDGET_S)
    deadline = time.monotonic() + duration_s
    cycle = 0
    while time.monotonic() < deadline:
        cycle_started = time.monotonic()
        await _run_chaos_cycle(fake=fake, supervisor=supervisor, cycle=cycle)
        cycle += 1
        # Hold the rest of this cycle's budget rather than starting the next one immediately.
        # The chaos operations themselves take milliseconds; the *soak* is the traffic flowing
        # between them, and pacing here is what makes the run last the duration it was asked for
        # instead of finishing as fast as the cycles happen to complete. Never overshoot the
        # deadline, so a long final budget cannot stretch the run past `duration_s`.
        idle = min(cycle_budget - (time.monotonic() - cycle_started), deadline - time.monotonic())
        if idle > 0:
            await asyncio.sleep(idle)

    stop_traffic.set()
    await asyncio.wait_for(traffic_task, timeout=5.0)
    fake.clear_error("/fhapi/v1/api/rest/datapoint")


async def _assert_final_state_matches_ground_truth(
    *, fake: FakeSysAp, supervisor: Supervisor
) -> None:
    """A final clean resync so any in-flight chaos (a WS drop mid-cycle, a debounced config
    change not yet applied) has settled, then compares every entity's state against the fake's
    own stored configuration.
    """
    await _wait_until(
        lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 1,
        timeout_seconds=15.0,
    )
    await supervisor._resync()
    await asyncio.sleep(0.1)

    assert supervisor._model is not None
    assert supervisor._state is not None
    model, state = supervisor._model, supervisor._state
    for entity_idx, entity in enumerate(model.entities):
        serial, channel_id = entity.device_serial, entity.channel_id
        ground_truth = fake._configuration["devices"][serial]["channels"][channel_id]
        raw = ground_truth["outputs"]["odp0000"]["value"]
        expected = raw == "1"
        assert state.values[entity_idx][0] == expected, (
            f"{entity.id}: expected {expected} from ground truth, got {state.values[entity_idx][0]}"
        )

    assert supervisor.metrics.task_restarts < 5, (
        "task_restarts approaching the P-29 escalation threshold -- something is "
        "crash-looping under chaos, not just recovering from it"
    )


async def test_soak_survives_chaos_with_correct_final_state(tmp_path: Path) -> None:
    duration_s = _duration_s()
    mqtt_port = free_port()

    error_counter = _ErrorCounter()
    root_logger = logging.getLogger("freeathome2mqtt")
    root_logger.addHandler(error_counter)

    rss_before = _current_rss_kb()

    async with (
        running_fake_broker(port=mqtt_port) as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration(_CHANNEL_COUNT))
        supervisor = Supervisor(
            config=_soak_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done, timeout_seconds=30.0)
            phase_started = time.monotonic()
            await _run_chaos_phase(fake=fake, supervisor=supervisor, duration_s=duration_s)
            phase_elapsed = time.monotonic() - phase_started
            await _assert_final_state_matches_ground_truth(fake=fake, supervisor=supervisor)
        finally:
            root_logger.removeHandler(error_counter)
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=10.0)

    # A soak that does not actually soak proves nothing: leaks, unbounded collections and slow
    # degradation are all time-dependent. `_run_chaos_phase` used to advance a *nominal* counter
    # by `duration_s / 10` per cycle instead of consulting the clock, so it always ran exactly ten
    # cycles and returned in seconds no matter what `FAH2MQTT_SOAK_DURATION_S` said -- the nightly
    # workflow's 86400 included. This pins the wall-clock contract the env var name promises.
    assert phase_elapsed >= duration_s * 0.9, (
        f"chaos phase ran for {phase_elapsed:.1f}s but was asked for {duration_s:.1f}s -- "
        f"the soak is not soaking (FAH2MQTT_SOAK_DURATION_S is being ignored)"
    )

    assert error_counter.records == [], (
        f"{len(error_counter.records)} unhandled ERROR-level log record(s) during the soak run: "
        f"{[r.getMessage() for r in error_counter.records[:5]]}"
    )

    rss_after = _current_rss_kb()
    if rss_before is not None and rss_after is not None and rss_before > 0:
        growth = (rss_after - rss_before) / rss_before
        assert growth < _RSS_GROWTH_CEILING, (
            f"RSS grew {growth:.1%} over the run (before={rss_before}KiB, after={rss_after}KiB), "
            f"need < {_RSS_GROWTH_CEILING:.0%} (docs/10 §8, catches P-51 and unbounded collections)"
        )
