"""bench_command_debounce: a slider drag collapses to a handful of SysAP writes (docs/05 §1 P5;
§4.2; §8; docs/11 WP7).

60 `/set` messages over 2 s (30 Hz, a realistic slider drag) on one `continuous: true` command,
through the real pipeline (fake SysAP, a real broker, `MqttClient`, `RestClient`,
`CommandDispatcher`) -- per docs/10 §7, not a microbenchmark of the debounce data structure alone.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.commands import CommandDispatcher
from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.codecs import build_codec
from freeathome2mqtt.model.entity import AttrKind, EgressBinding, Entity
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.sysap.rest import RestClient

pytestmark = pytest.mark.bench

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_UUID = "00000000-0000-0000-0000-000000000000"
_MESSAGE_COUNT = 60
_DRAG_DURATION_S = 2.0
_P5_BUDGET_WRITES = 6


def _dimmer_entity() -> Entity:
    return Entity(
        idx=0,
        id=f"{SERIAL}_ch0000",
        profile="dimming_actuator",
        name="Dimmer",
        area=None,
        device_serial=SERIAL,
        channel_id="ch0000",
        attr_names=("brightness",),
        attr_kinds=(AttrKind.STATE,),
        state_topic=f"{BASE}/dimmer",
        set_topic=f"{BASE}/dimmer/set",
        get_topic=f"{BASE}/dimmer/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
    )


def _brightness_egress() -> EgressBinding:
    percent_int = build_codec("percent_int")
    return EgressBinding(
        entity_idx=0,
        rest_path=f"{SERIAL}.ch0000.idp0011",
        encode=percent_int.encode,
        continuous=True,
        optimistic_attr=0,
        validate=lambda v: max(0, min(100, v)),
        confirm=True,
    )


async def _wait_until(predicate, *, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.005)


@contextlib.asynccontextmanager
async def _environment() -> AsyncIterator[tuple[Any, aiomqtt.Client]]:
    entities = [_dimmer_entity()]
    egress = {(0, "brightness"): _brightness_egress()}
    ingress: dict[str, Any] = {}
    state = StateStore(entities)
    by_topic = {"dimmer": 0}

    dispatcher_holder: list[CommandDispatcher] = []

    def _forward(message: aiomqtt.Message) -> None:
        dispatcher_holder[0].on_message(message)

    async with running_fake_broker() as broker:
        mqtt_client = MqttClient(
            host="127.0.0.1",
            port=broker.port,
            base_topic=BASE,
            sysap_serial=SERIAL,
            backoff_initial=0.02,
            backoff_cap=0.2,
            on_message=_forward,
        )
        mqtt_task = asyncio.create_task(mqtt_client.run())
        await _wait_until(lambda: mqtt_client.reconnect_count >= 1)

        async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
            fake.set_configuration(
                {
                    "devices": {
                        SERIAL: {
                            "channels": {
                                "ch0000": {
                                    "inputs": {"idp0011": {"value": "0"}},
                                    "outputs": {"odp0011": {"value": "0"}},
                                }
                            }
                        }
                    }
                }
            )
            rest = RestClient(
                base_url=str(http_client.make_url("")).rstrip("/"),
                username="installer",
                password="secret",
                session=http_client.session,
            )
            await rest.get_configuration()

            rate_limiter = RateLimiter()
            reconciler = Reconciler(
                state=state, rest=rest, ingress_table=ingress, rate_limiter=rate_limiter
            )
            dispatcher = CommandDispatcher(
                entities=entities,
                egress=egress,
                by_topic=by_topic,
                state=state,
                rest=rest,
                mqtt=mqtt_client,
                reconciler=reconciler,
                rate_limiter=rate_limiter,
                base_topic=BASE,
            )
            dispatcher_holder.append(dispatcher)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
                try:
                    yield fake, outsider
                finally:
                    await mqtt_client.stop()
                    await asyncio.wait_for(mqtt_task, timeout=5.0)


async def test_bench_command_debounce_60_sets_over_2s_meets_p5_budget() -> None:
    path = f"/fhapi/v1/api/rest/datapoint/{_UUID}/{SERIAL}.ch0000.idp0011"
    interval = _DRAG_DURATION_S / _MESSAGE_COUNT

    async with _environment() as (fake, outsider):
        for i in range(_MESSAGE_COUNT):
            position = round(i * 100 / (_MESSAGE_COUNT - 1))
            await outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": position}))
            await asyncio.sleep(interval)

        # The trailing edge's own window must still close and send the final value.
        await asyncio.sleep(0.2)

        assert fake.request_count(path) <= _P5_BUDGET_WRITES
