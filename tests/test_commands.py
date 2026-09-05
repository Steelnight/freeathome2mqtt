"""Tests for bus/commands.py: object/attribute/scalar /set forms, validate-then-clamp, debounce,
optimistic writes, reconciliation wiring, and rate-limited /get (docs/02 §5; docs/04 §3; docs/11
WP7).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from typing import Any

import aiomqtt
import orjson

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.commands import CommandDispatcher
from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.codecs import build_codec
from freeathome2mqtt.model.entity import AttrKind, Binding, EgressBinding, Entity
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.sysap.rest import RestClient

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"


def _identity(value: Any) -> Any:
    return value


def _range_validator(low: float, high: float):
    return lambda value: max(low, min(high, value))


def _entity(
    idx: int, attr_names: tuple[str, ...], slug: str, *, transform: str | None = None
) -> Entity:
    return Entity(
        idx=idx,
        id=f"{SERIAL}_ch{idx:04d}",
        profile="test_profile",
        name="Test",
        area=None,
        device_serial=SERIAL,
        channel_id=f"ch{idx:04d}",
        attr_names=attr_names,
        attr_kinds=tuple(AttrKind.STATE for _ in attr_names),
        state_topic=f"{BASE}/{slug}",
        set_topic=f"{BASE}/{slug}/set",
        get_topic=f"{BASE}/{slug}/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
        transform=transform,
    )


def _binding(entity_idx: int, attr_idx: int, dp: str, codec_name: str) -> Binding:
    codec = build_codec(codec_name)
    return Binding(
        entity_idx=entity_idx,
        attr_idx=attr_idx,
        decode=codec.decode,
        kind=AttrKind.STATE,
        attr_bit=1 << attr_idx,
    )


def _egress(
    entity_idx: int,
    channel_id: str,
    dp: str,
    codec_name: str,
    *,
    optimistic_attr: int | None,
    continuous: bool = False,
    value_range: tuple[float, float] | None = None,
    confirm: bool = True,
) -> EgressBinding:
    codec = build_codec(codec_name)
    validate = _identity if value_range is None else _range_validator(*value_range)
    return EgressBinding(
        entity_idx=entity_idx,
        rest_path=f"{SERIAL}.{channel_id}.{dp}",
        encode=codec.encode,
        continuous=continuous,
        optimistic_attr=optimistic_attr,
        validate=validate,
        confirm=confirm,
    )


def _enum_binding(
    entity_idx: int, attr_idx: int, dp: str, decode_values: dict[str, str]
) -> Binding:
    codec = build_codec("enum", decode_values=decode_values, encode_values={})
    return Binding(
        entity_idx=entity_idx,
        attr_idx=attr_idx,
        decode=codec.decode,
        kind=AttrKind.STATE,
        attr_bit=1 << attr_idx,
    )


def _enum_egress(
    entity_idx: int,
    channel_id: str,
    dp: str,
    encode_values: dict[str, str],
    *,
    optimistic_attr: int | None,
) -> EgressBinding:
    codec = build_codec("enum", decode_values={}, encode_values=encode_values)
    return EgressBinding(
        entity_idx=entity_idx,
        rest_path=f"{SERIAL}.{channel_id}.{dp}",
        encode=codec.encode,
        continuous=False,
        optimistic_attr=optimistic_attr,
        validate=_identity,
        confirm=True,
    )


def _channel(inputs: dict[str, str], outputs: dict[str, str]) -> dict[str, Any]:
    return {
        "inputs": {dp: {"value": v} for dp, v in inputs.items()},
        "outputs": {dp: {"value": v} for dp, v in outputs.items()},
    }


def _fixture() -> tuple[list[Entity], dict, dict]:
    """switch (ch0000): one discrete bool01 command. dimmer (ch0001): discrete state +
    continuous brightness, declared in that order. noconfirm (ch0002): confirm:false command.
    nocommands (ch0003): no egress entries at all. noopt (ch0004): a command with no optimistic
    tracking (optimistic_attr=None). rtc (ch0005): room_temperature_controller-transformed,
    on_off/eco/mode all directly settable too. cover (ch0006): cover_with_slats-transformed,
    position/slat_position/stop all directly settable too (docs/03 §7).
    """
    entities = [
        _entity(0, ("state",), "switch"),
        _entity(1, ("state", "brightness"), "dimmer"),
        _entity(2, ("state",), "noconfirm"),
        _entity(3, ("state",), "nocommands"),
        _entity(4, (), "noopt"),
        _entity(5, ("on_off", "eco", "mode"), "rtc", transform="room_temperature_controller"),
        _entity(6, ("position", "slat_position"), "cover", transform="cover_with_slats"),
    ]
    egress = {
        (0, "state"): _egress(0, "ch0000", "idp0000", "bool01", optimistic_attr=0),
        (1, "state"): _egress(1, "ch0001", "idp0010", "bool01", optimistic_attr=0),
        (1, "brightness"): _egress(
            1,
            "ch0001",
            "idp0011",
            "percent_int",
            optimistic_attr=1,
            continuous=True,
            value_range=(0, 100),
        ),
        (2, "state"): _egress(2, "ch0002", "idp0020", "bool01", optimistic_attr=0, confirm=False),
        (4, "trigger"): _egress(4, "ch0004", "idp0040", "bool01", optimistic_attr=None),
        (5, "on_off"): _egress(5, "ch0005", "idp0050", "bool01", optimistic_attr=0),
        (5, "eco"): _egress(5, "ch0005", "idp0051", "bool01", optimistic_attr=1),
        (5, "mode"): _enum_egress(
            5, "ch0005", "idp0052", {"heating": "0", "cooling": "1"}, optimistic_attr=2
        ),
        (6, "position"): _egress(
            6, "ch0006", "idp0060", "percent_int", optimistic_attr=0, continuous=True
        ),
        (6, "slat_position"): _egress(
            6, "ch0006", "idp0061", "percent_int", optimistic_attr=1, continuous=True
        ),
        (6, "stop"): _egress(6, "ch0006", "idp0062", "bool01", optimistic_attr=None),
    }
    ingress = {
        f"{SERIAL}/ch0000/odp0000": _binding(0, 0, "odp0000", "bool01"),
        f"{SERIAL}/ch0001/odp0010": _binding(1, 0, "odp0010", "bool01"),
        f"{SERIAL}/ch0001/odp0011": _binding(1, 1, "odp0011", "percent_int"),
        f"{SERIAL}/ch0002/odp0020": _binding(2, 0, "odp0020", "bool01"),
        f"{SERIAL}/ch0005/odp0050": _binding(5, 0, "odp0050", "bool01"),
        f"{SERIAL}/ch0005/odp0051": _binding(5, 1, "odp0051", "bool01"),
        f"{SERIAL}/ch0005/odp0052": _enum_binding(
            5, 2, "odp0052", {"0": "heating", "1": "cooling"}
        ),
        f"{SERIAL}/ch0006/odp0060": _binding(6, 0, "odp0060", "percent_int"),
        f"{SERIAL}/ch0006/odp0061": _binding(6, 1, "odp0061", "percent_int"),
    }
    return entities, egress, ingress


def _fake_configuration() -> dict[str, Any]:
    return {
        "devices": {
            SERIAL: {
                "channels": {
                    "ch0000": _channel({"idp0000": "0"}, {"odp0000": "0"}),
                    "ch0001": _channel(
                        {"idp0010": "0", "idp0011": "0"}, {"odp0010": "0", "odp0011": "0"}
                    ),
                    "ch0002": _channel({"idp0020": "0"}, {"odp0020": "0"}),
                    "ch0004": _channel({"idp0040": "0"}, {}),
                    "ch0005": _channel(
                        {"idp0050": "0", "idp0051": "0", "idp0052": "0"},
                        {"odp0050": "0", "odp0051": "0", "odp0052": "0"},
                    ),
                    "ch0006": _channel(
                        {"idp0060": "0", "idp0061": "0", "idp0062": "0"},
                        {"odp0060": "0", "odp0061": "0"},
                    ),
                }
            }
        }
    }


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


class _Environment:
    """Wires a full real stack: broker, bridge MqttClient, fake SysAP, RestClient,
    CommandDispatcher.
    """

    def __init__(
        self,
        *,
        broker: Any,
        fake: FakeSysAp,
        rest: Any,
        mqtt_client: MqttClient,
        outsider: aiomqtt.Client,
        state: StateStore,
        dispatcher: CommandDispatcher,
        entities: list[Entity],
    ) -> None:
        self.broker = broker
        self.fake = fake
        self.rest = rest
        self.mqtt_client = mqtt_client
        self.outsider = outsider
        self.state = state
        self.dispatcher = dispatcher
        self.entities = entities
        self.responses: list[tuple[str, dict[str, Any]]] = []

    async def collect_responses(self, *, count: int, timeout_seconds: float = 2.0) -> None:
        async with asyncio.timeout(timeout_seconds):
            async for message in self.outsider.messages:
                self.responses.append((str(message.topic), orjson.loads(message.payload)))
                if len(self.responses) >= count:
                    return


@contextlib.asynccontextmanager
async def _environment(
    *,
    debounce_s: float = 0.05,
    default_optimistic: bool = True,
    optimistic_overrides: dict[int, bool] | None = None,
    debounce_overrides: dict[int, float] | None = None,
    metrics: Metrics | None = None,
    on_topology_changed: Callable[[], None] | None = None,
) -> AsyncIterator[_Environment]:
    entities, egress, ingress = _fixture()
    state = StateStore(entities)
    by_topic = {e.state_topic.rsplit("/", 1)[-1]: e.idx for e in entities}

    # CommandDispatcher needs the MqttClient instance to publish responses, and MqttClient needs
    # an on_message callback at construction time -- resolved with a small forwarding closure
    # rather than reaching into MqttClient's private state after the fact.
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
            fake.set_configuration(_fake_configuration())
            rest = RestClient(
                base_url=str(http_client.make_url("")).rstrip("/"),
                username="installer",
                password="secret",
                session=http_client.session,
            )
            await rest.get_configuration()

            rate_limiter = RateLimiter()
            reconciler = Reconciler(
                state=state,
                rest=rest,
                ingress_table=ingress,
                rate_limiter=rate_limiter,
                delay_s=0.05,
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
                debounce_s=debounce_s,
                default_optimistic=default_optimistic,
                optimistic_overrides=optimistic_overrides or {},
                debounce_overrides=debounce_overrides or {},
                metrics=metrics,
                on_topology_changed=on_topology_changed,
            )
            dispatcher_holder.append(dispatcher)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
                await outsider.subscribe(f"{BASE}/bridge/response/#")
                env = _Environment(
                    broker=broker,
                    fake=fake,
                    rest=rest,
                    mqtt_client=mqtt_client,
                    outsider=outsider,
                    state=state,
                    dispatcher=dispatcher,
                    entities=entities,
                )
                try:
                    yield env
                finally:
                    await mqtt_client.stop()
                    await asyncio.wait_for(mqtt_task, timeout=5.0)


_UUID = "00000000-0000-0000-0000-000000000000"


def _dp_path(address: str) -> str:
    return f"/fhapi/v1/api/rest/datapoint/{_UUID}/{address}"


def _input_value(fake: FakeSysAp, channel: str, dp: str) -> str:
    # No public getter exists on the fake; reaching into its configuration is test-only
    # introspection of the double, not production code.
    value: str = fake._configuration["devices"][SERIAL]["channels"][channel]["inputs"][dp]["value"]
    return value


async def test_set_object_form_optimistic_write_and_rest_dispatch() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.state.values[0][0] is True)

        assert env.state.unconfirmed[0] == 0b1
        assert env.state.dirty == {0}
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0000.idp0000")) >= 1)
        assert _input_value(env.fake, "ch0000", "idp0000") == "1"


async def test_set_attribute_form() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/dimmer/set/brightness", b"75")
        await _wait_until(lambda: env.state.values[1][1] == 75)
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0001.idp0011")) >= 1)
        assert _input_value(env.fake, "ch0001", "idp0011") == "75"


async def test_set_scalar_shorthand_true_maps_to_the_primary_command() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/set", b"true")
        await _wait_until(lambda: env.state.values[0][0] is True)


async def test_set_scalar_shorthand_on_off_strings() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/set", b"ON")
        await _wait_until(lambda: env.state.values[0][0] is True)

        await env.outsider.publish(f"{BASE}/switch/set", b"OFF")
        await _wait_until(lambda: env.state.values[0][0] is False)


async def test_set_object_form_applies_attributes_in_profile_declaration_order() -> None:
    # docs/04 §3.1: JSON key order must not matter -- "state" is declared before "brightness".
    async with _environment() as env:
        await env.outsider.publish(
            f"{BASE}/dimmer/set", orjson.dumps({"brightness": 40, "state": True})
        )
        await _wait_until(lambda: env.state.values[1][1] == 40)
        assert env.state.values[1][0] is True


async def test_set_rejects_an_unknown_command() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"nope": True}))
        await env.collect_responses(count=1)

        topic, payload = env.responses[0]
        assert topic == f"{BASE}/bridge/response/set"
        assert payload["status"] == "error"
        assert payload["id"] == "ABB7F500E17A_ch0000"
        assert env.state.dirty == set()  # never touched


async def test_set_clamps_an_out_of_range_value() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 150}))
        await _wait_until(lambda: env.state.values[1][1] == 100)  # clamped, not rejected


async def test_set_rejects_a_structurally_wrong_value() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": "bright"}))
        await env.collect_responses(count=1)

        assert env.responses[0][1]["status"] == "error"
        assert env.state.values[1][1] is None  # never applied


async def test_set_echoes_the_transaction_on_success() -> None:
    async with _environment() as env:
        await env.outsider.publish(
            f"{BASE}/switch/set", orjson.dumps({"state": True, "transaction": "abc123"})
        )
        await env.collect_responses(count=1)

        topic, payload = env.responses[0]
        assert topic == f"{BASE}/bridge/response/set"
        assert payload == {"status": "ok", "id": "ABB7F500E17A_ch0000", "transaction": "abc123"}


async def test_set_echoes_the_transaction_on_error() -> None:
    async with _environment() as env:
        await env.outsider.publish(
            f"{BASE}/switch/set", orjson.dumps({"nope": True, "transaction": "xyz"})
        )
        await env.collect_responses(count=1)

        assert env.responses[0][1]["transaction"] == "xyz"


async def test_set_without_a_transaction_gets_no_success_response() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.state.values[0][0] is True)
        await asyncio.sleep(0.1)  # give a wrongly-always-acking implementation time to respond

        assert env.responses == []


async def test_set_no_optimistic_suppresses_the_optimistic_write() -> None:
    async with _environment() as env:
        await env.outsider.publish(
            f"{BASE}/switch/set", orjson.dumps({"state": True, "no_optimistic": True})
        )
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0000.idp0000")) >= 1)

        assert env.state.values[0][0] is None  # the REST write still happened, just not the guess
        assert env.state.unconfirmed[0] == 0


async def test_continuous_command_debounces_leading_and_trailing_edge() -> None:
    # docs/05 §4.2: leading edge sends immediately; further /set inside the window only update
    # `pending`; the window's close sends the final value if it differs from what was last sent.
    async with _environment(debounce_s=0.1) as env:
        path = _dp_path(f"{SERIAL}.ch0001.idp0011")

        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 10}))
        await _wait_until(lambda: env.fake.request_count(path) >= 1)
        assert _input_value(env.fake, "ch0001", "idp0011") == "10"  # leading edge

        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 20}))
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 30}))
        assert env.fake.request_count(path) == 1  # still just the leading edge -- not yet sent

        await _wait_until(lambda: env.fake.request_count(path) >= 2, timeout_seconds=2.0)
        assert _input_value(env.fake, "ch0001", "idp0011") == "30"  # trailing edge: final value
        assert env.fake.request_count(path) == 2  # 20 was collapsed into pending, never sent


async def test_per_entity_optimistic_override_false_suppresses_the_optimistic_write() -> None:
    # entity/options {"optimistic": false} (docs/04 §5, docs/07 §4.1) must force optimism off
    # even though this particular message never set no_optimistic itself.
    async with _environment(optimistic_overrides={0: False}) as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0000.idp0000")) >= 1)

        assert env.state.values[0][0] is None  # the REST write still happened, just not the guess
        assert env.state.unconfirmed[0] == 0


async def test_default_optimistic_false_suppresses_optimism_with_no_per_entity_override() -> None:
    # performance.optimistic: false (docs/07 §2) is the installation-wide fallback, distinct
    # from an entity/options per-entity override -- it must take effect on its own.
    async with _environment(default_optimistic=False) as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0000.idp0000")) >= 1)

        assert env.state.values[0][0] is None  # the REST write still happened, just not the guess
        assert env.state.unconfirmed[0] == 0


async def test_per_entity_optimistic_override_only_affects_its_own_entity() -> None:
    async with _environment(optimistic_overrides={0: False}) as env:
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.state.values[1][0] is True)  # unaffected -- different entity


async def test_per_entity_debounce_override_widens_the_window() -> None:
    # entity/options {"debounce_ms": ...} overrides the dispatcher-wide default just for this
    # entity's own commands.
    async with _environment(debounce_s=0.05, debounce_overrides={1: 0.4}) as env:
        path = _dp_path(f"{SERIAL}.ch0001.idp0011")

        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 10}))
        await _wait_until(lambda: env.fake.request_count(path) >= 1)  # leading edge

        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 20}))
        await asyncio.sleep(0.15)  # comfortably past the dispatcher-wide 0.05s default
        assert env.fake.request_count(path) == 1  # the override window is still open

        await _wait_until(lambda: env.fake.request_count(path) >= 2, timeout_seconds=2.0)
        assert _input_value(env.fake, "ch0001", "idp0011") == "20"


async def test_stop_accepting_ignores_further_messages() -> None:
    # docs/08 §10 step 1: shutdown stops routing new commands, without touching MQTT itself.
    async with _environment() as env:
        path = _dp_path(f"{SERIAL}.ch0000.idp0000")
        env.dispatcher.stop_accepting()
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await asyncio.sleep(0.1)
        assert env.fake.request_count(path) == 0


async def test_flush_pending_sends_the_settled_value_of_an_open_debounce_window() -> None:
    # docs/08 §10 step 2: a value the user just set must not be silently dropped at shutdown.
    async with _environment(debounce_s=5.0) as env:
        path = _dp_path(f"{SERIAL}.ch0001.idp0011")
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 10}))
        await _wait_until(lambda: env.fake.request_count(path) >= 1)  # leading edge
        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 42}))
        await asyncio.sleep(0.05)  # let the dispatcher's background task collapse it into pending
        assert env.fake.request_count(path) == 1  # collapsed into `pending`, window still open

        await env.dispatcher.flush_pending(deadline_s=2.0)

        assert env.fake.request_count(path) == 2
        assert _input_value(env.fake, "ch0001", "idp0011") == "42"


async def test_flush_pending_is_a_noop_with_nothing_pending() -> None:
    async with _environment() as env:
        await env.dispatcher.flush_pending(deadline_s=2.0)  # must not raise or hang


async def test_discrete_command_is_never_debounced() -> None:
    # A window long enough to catch a wrongly-debounced discrete command.
    async with _environment(debounce_s=1.0) as env:
        path = _dp_path(f"{SERIAL}.ch0000.idp0000")

        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": False}))
        await _wait_until(lambda: env.fake.request_count(path) >= 2)


async def test_command_failure_rolls_back_via_immediate_reconciliation() -> None:
    # F12: no retry; error to bridge/response; reconcile immediately (not after the 3s timer) so
    # the optimistic lie is corrected within one round trip.
    async with _environment() as env:
        env.fake.set_error(_dp_path(f"{SERIAL}.ch0000.idp0000"), 400)

        # The optimistic guess (True) and its correction (False) can both land before a poll
        # ever observes the guess -- 400 raises with no retry, so the whole chain (optimistic
        # write -> failed PUT -> immediate reconcile) can complete within microseconds. Only the
        # settled outcome is asserted below, not the transient intermediate state.
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))

        await env.collect_responses(count=1)
        assert env.responses[0][0] == f"{BASE}/bridge/response/set"
        assert env.responses[0][1]["status"] == "error"

        # The write never actually reached the SysAP, so odp0000 is still "0" -- reconciliation
        # reads that back and rolls the optimistic guess back to False.
        await _wait_until(lambda: env.state.values[0][0] is False, timeout_seconds=2.0)
        assert env.state.unconfirmed[0] == 0


async def test_get_refreshes_a_single_named_attribute() -> None:
    async with _environment() as env:
        env.fake.set_datapoint(SERIAL, "ch0000", "odp0000", "1")

        await env.outsider.publish(f"{BASE}/switch/get", orjson.dumps({"attribute": "state"}))

        await _wait_until(lambda: env.state.values[0][0] is True)


async def test_get_with_an_empty_payload_refreshes_every_state_attribute() -> None:
    async with _environment() as env:
        env.fake.set_datapoint(SERIAL, "ch0001", "odp0010", "1")
        env.fake.set_datapoint(SERIAL, "ch0001", "odp0011", "42")

        await env.outsider.publish(f"{BASE}/dimmer/get", b"{}")

        await _wait_until(lambda: env.state.values[1] == [True, 42])


async def test_get_rejects_an_unknown_attribute() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/switch/get", orjson.dumps({"attribute": "nope"}))
        await env.collect_responses(count=1)

        topic, payload = env.responses[0]
        assert topic == f"{BASE}/bridge/response/get"
        assert payload["status"] == "error"


async def test_get_storm_is_rate_limited() -> None:
    # P-52: a loop hammering /get for one entity must not amplify into repeated SysAP reads.
    async with _environment() as env:
        path = _dp_path(f"{SERIAL}.ch0000.odp0000")

        await env.outsider.publish(f"{BASE}/switch/get", b"{}")
        await _wait_until(lambda: env.fake.request_count(path) >= 1)

        await env.outsider.publish(f"{BASE}/switch/get", b"{}")
        await env.collect_responses(count=1)

        topic, payload = env.responses[0]
        assert topic == f"{BASE}/bridge/response/get"
        assert payload["status"] == "error"
        assert payload["error"] == "rate_limited"
        assert env.fake.request_count(path) == 1  # the second /get never reached the SysAP


async def test_unconfirmed_command_is_reconciled() -> None:
    # ADR-012/P-53: with no WS echo ever arriving in this harness, only the reconciliation timer
    # (armed at 0.05s by _environment()) can clear the unconfirmed mark.
    async with _environment() as env:
        env.fake.set_datapoint(SERIAL, "ch0000", "odp0000", "1")  # what the SysAP will "confirm"

        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.state.unconfirmed[0] & 0b1)  # the optimistic mark is set

        await _wait_until(lambda: env.state.unconfirmed[0] == 0, timeout_seconds=2.0)
        assert env.state.values[0][0] is True  # confirmed correct by the read, not by an echo


async def test_no_reconcile_when_confirm_false() -> None:
    # P-19: not every channel type echoes; confirm:false must never arm a reconcile timer.
    async with _environment() as env:
        path = _dp_path(f"{SERIAL}.ch0002.odp0020")

        await env.outsider.publish(f"{BASE}/noconfirm/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: env.state.values[2][0] is True)  # the optimistic write happens

        await asyncio.sleep(0.2)  # comfortably past the 0.05s reconcile delay used in this harness
        assert env.fake.request_count(path) == 0


async def test_on_message_ignores_a_bridge_request_topic() -> None:
    # "bridge" is not a known entity slug -- the bridge API itself is WP9's mqtt/bridge_api.py.
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/bridge/request/reload", b"{}")
        await asyncio.sleep(0.1)
        assert env.state.dirty == set()


def test_parse_topic_rejects_topics_outside_the_base_and_malformed_shapes() -> None:
    entities, egress, _ingress = _fixture()
    state = StateStore(entities)
    by_topic = {e.state_topic.rsplit("/", 1)[-1]: e.idx for e in entities}
    dispatcher = CommandDispatcher(
        entities=entities,
        egress=egress,
        by_topic=by_topic,
        state=state,
        rest=None,  # type: ignore[arg-type] -- _parse_topic touches none of these
        mqtt=None,  # type: ignore[arg-type]
        reconciler=None,  # type: ignore[arg-type]
        rate_limiter=None,  # type: ignore[arg-type]
        base_topic=BASE,
    )

    assert dispatcher._parse_topic("other/switch/set") is None
    assert dispatcher._parse_topic(f"{BASE}/switch/set/extra/segments") is None


async def test_set_scalar_shorthand_with_no_known_commands_is_a_noop() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/nocommands/set", b"true")
        await asyncio.sleep(0.1)
        assert env.state.dirty == set()


async def test_continuous_command_with_no_follow_up_sends_exactly_once() -> None:
    async with _environment(debounce_s=0.05) as env:
        path = _dp_path(f"{SERIAL}.ch0001.idp0011")

        await env.outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 10}))
        await _wait_until(lambda: env.fake.request_count(path) >= 1)
        await asyncio.sleep(0.15)  # past the window close, with no follow-up message

        assert env.fake.request_count(path) == 1  # no redundant resend of the same value


async def test_transformed_hvac_mode_command_writes_multiple_datapoints() -> None:
    # docs/03 §7: room_temperature_controller's "hvac_mode" is not a real profile command --
    # setting it to "eco" must write BOTH on_off and eco (RoomTemperatureControllerTransform).
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/rtc/set", orjson.dumps({"hvac_mode": "eco"}))
        await _wait_until(lambda: env.state.values[5][0] is True and env.state.values[5][1] is True)

        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0005.idp0050")) >= 1)
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0005.idp0051")) >= 1)
        assert _input_value(env.fake, "ch0005", "idp0050") == "1"
        assert _input_value(env.fake, "ch0005", "idp0051") == "1"


async def test_transformed_hvac_mode_command_responds_ok_once() -> None:
    async with _environment() as env:
        await env.outsider.publish(
            f"{BASE}/rtc/set", orjson.dumps({"hvac_mode": "heating", "transaction": "t1"})
        )
        await env.collect_responses(count=1)
        assert env.responses == [
            (
                f"{BASE}/bridge/response/set",
                {"status": "ok", "id": f"{SERIAL}_ch0005", "transaction": "t1"},
            )
        ]


async def test_transformed_entity_real_commands_still_settable_directly() -> None:
    # room_temperature_controller's transform only claims "hvac_mode" -- on_off/eco/mode are
    # still ordinary, directly-settable profile commands (docs/03 §7's own worked example).
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/rtc/set", orjson.dumps({"eco": True}))
        await _wait_until(lambda: env.state.values[5][1] is True)
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0005.idp0051")) >= 1)


async def test_transformed_entity_unknown_command_still_errors() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/rtc/set", orjson.dumps({"nope": True}))
        await env.collect_responses(count=1)
        assert env.responses[0][1]["status"] == "error"
        assert "nope" in env.responses[0][1]["error"]
        assert env.state.dirty == set()


async def test_cover_with_slats_position_fully_open_also_resets_slat_position() -> None:
    # docs/03 §7: opening fully leaves the slat angle meaningless -- CoverWithSlatsTransform
    # resets it to 0 as a side effect of a full-open position command.
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/cover/set", orjson.dumps({"position": 100}))
        await _wait_until(lambda: env.state.values[6][0] == 100 and env.state.values[6][1] == 0)
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0006.idp0060")) >= 1)
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0006.idp0061")) >= 1)
        assert _input_value(env.fake, "ch0006", "idp0060") == "100"
        assert _input_value(env.fake, "ch0006", "idp0061") == "0"


async def test_cover_with_slats_position_not_fully_open_leaves_slat_alone() -> None:
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/cover/set", orjson.dumps({"position": 50}))
        await _wait_until(lambda: env.state.values[6][0] == 50)
        await asyncio.sleep(0.1)
        assert env.fake.request_count(_dp_path(f"{SERIAL}.ch0006.idp0061")) == 0


async def test_cover_with_slats_stop_command_is_intercepted_but_still_works() -> None:
    # cover_with_slats' transform claims every command, including ones with no real side effect.
    async with _environment() as env:
        await env.outsider.publish(f"{BASE}/cover/set", orjson.dumps({"stop": True}))
        await _wait_until(lambda: env.fake.request_count(_dp_path(f"{SERIAL}.ch0006.idp0062")) >= 1)
        assert _input_value(env.fake, "ch0006", "idp0062") == "1"


async def test_write_failure_without_optimistic_tracking_still_responds() -> None:
    # A command with no `optimistic:` attribute has nothing to roll back -- failure must still
    # report the error, just without attempting reconciliation.
    async with _environment() as env:
        env.fake.set_error(_dp_path(f"{SERIAL}.ch0004.idp0040"), 400)

        await env.outsider.publish(f"{BASE}/noopt/set", orjson.dumps({"trigger": True}))
        await env.collect_responses(count=1)

        assert env.responses[0][1]["status"] == "error"


# ---------------------------------------------------- WP14: the commands / command_errors pair


async def test_accepted_commands_are_counted() -> None:
    """docs/04 §4.2's `commands`: commands the bridge accepted and acted on, counted after
    validation and *before* debouncing, so the figure answers "how much is being asked of the
    bridge?" rather than "how many writes survived the debouncer?" (docs/05 §4.2).
    """
    metrics = Metrics()
    async with _environment(metrics=metrics) as env:
        payload = orjson.dumps({"state": True, "brightness": 40})
        await env.outsider.publish(f"{BASE}/dimmer/set", payload)
        await _wait_until(lambda: metrics.commands >= 2)

    assert metrics.commands == 2
    assert metrics.command_errors == 0


async def test_a_rejected_command_counts_as_an_error_not_as_a_command() -> None:
    metrics = Metrics()
    async with _environment(metrics=metrics) as env:
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"nonexistent": True}))
        await _wait_until(lambda: metrics.command_errors >= 1)

    assert metrics.command_errors == 1
    assert metrics.commands == 0


async def test_an_out_of_range_value_counts_as_an_error() -> None:
    metrics = Metrics()
    async with _environment(metrics=metrics) as env:
        payload = orjson.dumps({"brightness": "not a number"})
        await env.outsider.publish(f"{BASE}/dimmer/set", payload)
        await _wait_until(lambda: metrics.command_errors >= 1)

    assert metrics.command_errors == 1


# --------------------------------------- WP18: a 404 on write means the topology moved under us


async def test_a_404_on_write_requests_a_resync() -> None:
    """docs/06 §4.1's last row: "`404` on a datapoint write -> debounced full resync (topology
    changed under us)". Named as an unwired gap in `supervisor.py`'s docstring until WP18.
    """
    requested = 0

    def _on_topology_changed() -> None:
        nonlocal requested
        requested += 1

    async with _environment(on_topology_changed=_on_topology_changed) as env:
        env.fake.set_error(_dp_path(f"{SERIAL}.ch0000.idp0000"), 404)
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await _wait_until(lambda: requested >= 1)

    assert requested == 1


async def test_a_400_on_write_does_not_request_a_resync() -> None:
    """A rejected *value* says nothing about topology; only `404` means the datapoint is gone.
    Resyncing on every bad command would turn a typo into a config fetch (ADR-007's whole point).
    """
    requested = 0

    def _on_topology_changed() -> None:
        nonlocal requested
        requested += 1

    async with _environment(on_topology_changed=_on_topology_changed) as env:
        env.fake.set_error(_dp_path(f"{SERIAL}.ch0000.idp0000"), 400)
        await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
        await env.collect_responses(count=1)

    assert requested == 0


async def test_a_burst_of_404s_still_only_asks_once_per_write() -> None:
    """The dispatcher only *requests*; debouncing is the Supervisor's `_ReloadDebouncer` (P-55),
    which already collapses a burst into one resync. This pins the contract that the command path
    does not do its own retry or fan-out on top of that.
    """
    requested = 0

    def _on_topology_changed() -> None:
        nonlocal requested
        requested += 1

    async with _environment(debounce_s=0.0, on_topology_changed=_on_topology_changed) as env:
        env.fake.set_error(_dp_path(f"{SERIAL}.ch0000.idp0000"), 404)
        for _ in range(3):
            await env.outsider.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
            await asyncio.sleep(0.05)
        await _wait_until(lambda: requested >= 1)

    assert 1 <= requested <= 3
