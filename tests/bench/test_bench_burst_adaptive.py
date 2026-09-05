"""bench_burst_adaptive: does adaptive coalescing actually buy anything? (docs/05 §4.1; docs/12
WP17).

docs/05 §4.1 describes adaptive coalescing and says "implement it only after P1-P4 pass without
it, and keep it behind a flag". P1-P4 do pass, so the precondition is met -- but a documented
optional optimisation is not the same as a *useful* one, and docs/12 WP17 committed up front to
deleting the knob if the measurement did not support it.

The scenario is the one the feature exists for: a scene that arrives as a *sequence* of frames
over a couple of hundred milliseconds (which is how a real free@home scene reports -- the bus
drives channels progressively, it does not deliver 500 datapoints in one frame), repeatedly
touching the same entities. Plain 20 ms coalescing flushes many times through such a burst and
republishes entities that changed again; a window that widens under load flushes fewer times.

If the counts come out equal, the honest conclusion is that the feature buys nothing here and the
knob should go -- which is why this asserts a real improvement rather than merely "it still
works".
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.entity import AttrKind, Entity

from . import _record

pytestmark = pytest.mark.bench

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_ENTITIES = 40
_BURST_FRAMES = 20
_FRAME_INTERVAL_S = 0.01  # 20 frames over ~200 ms, a realistic scene ramp
_BASE_COALESCE_MS = 20


class _CountingMqtt:
    """Counts publishes. The measurement here is a *count*, so a real broker would add socket
    noise without changing what is being compared.
    """

    def __init__(self) -> None:
        self.publishes = 0

    async def publish(self, topic: str, payload: bytes, **_kwargs: object) -> None:
        self.publishes += 1


def _entities(count: int) -> list[Entity]:
    return [
        Entity(
            idx=i,
            id=f"{SERIAL}_ch{i:04d}",
            profile="test_profile",
            name="Test",
            area=None,
            device_serial=SERIAL,
            channel_id=f"ch{i:04d}",
            attr_names=("state",),
            attr_kinds=(AttrKind.STATE,),
            state_topic=f"{BASE}/test{i}",
            set_topic=f"{BASE}/test{i}/set",
            get_topic=f"{BASE}/test{i}/get",
            availability_topic=None,
            optimistic=False,
            discovery=(),
        )
        for i in range(count)
    ]


async def _publishes_during_scene_ramp(*, adaptive: bool) -> int:
    entities = _entities(_ENTITIES)
    state = StateStore(entities)
    mqtt = _CountingMqtt()
    publisher = Publisher(
        entities=entities,
        state=state,
        mqtt=mqtt,
        coalesce_ms=_BASE_COALESCE_MS,
        coalesce_adaptive=adaptive,
        publish_last_changed=False,
    )
    task = asyncio.create_task(publisher.run())
    try:
        for frame in range(_BURST_FRAMES):
            for idx in range(_ENTITIES):
                state.apply(idx, 0, f"{frame}-{idx}")
            await asyncio.sleep(_FRAME_INTERVAL_S)
        # Let the final window close and drain.
        await asyncio.sleep(0.5)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return mqtt.publishes


async def test_bench_adaptive_coalescing_reduces_publishes_under_a_scene_ramp() -> None:
    fixed = await _publishes_during_scene_ramp(adaptive=False)
    adaptive = await _publishes_during_scene_ramp(adaptive=True)
    _record.record("tests/bench/test_bench_burst_adaptive.py::fixed_publishes", fixed)
    _record.record("tests/bench/test_bench_burst_adaptive.py::adaptive_publishes", adaptive)

    assert fixed > 0 and adaptive > 0
    assert adaptive < fixed, (
        f"adaptive coalescing published {adaptive} messages against fixed coalescing's {fixed}: "
        "it is not buying the reduction docs/05 §4.1 claims for it, and the knob should be "
        "deleted rather than shipped inert (docs/12 §7.1)"
    )
