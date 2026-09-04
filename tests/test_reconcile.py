"""Tests for bus/reconcile.py: RateLimiter + Reconciler (ADR-012; docs/04 §3.4; docs/11 WP7)."""

from __future__ import annotations

import asyncio

from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.codecs import build_codec
from freeathome2mqtt.model.entity import AttrKind, Binding, Entity
from freeathome2mqtt.sysap.rest import RestClient

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
DATAPOINT_PATH = (
    "/fhapi/v1/api/rest/datapoint/00000000-0000-0000-0000-000000000000/ABB7F500E17A.ch0000.odp0000"
)


def _entity(idx: int, attr_names: tuple[str, ...] = ("state",)) -> Entity:
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
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
    )


def _config(*, entity_idx: int = 0, value: str = "0") -> dict:
    return {
        "devices": {
            SERIAL: {
                "channels": {
                    f"ch{entity_idx:04d}": {"outputs": {"odp0000": {"value": value}}},
                }
            }
        }
    }


def _ingress_table(entity_idx: int = 0) -> dict[str, Binding]:
    bool01 = build_codec("bool01")
    key = f"{SERIAL}/ch{entity_idx:04d}/odp0000"
    binding = Binding(
        entity_idx=entity_idx, attr_idx=0, decode=bool01.decode, kind=AttrKind.STATE, attr_bit=1
    )
    return {key: binding}


class _FrozenClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def test_rate_limiter_allows_the_first_read_and_blocks_within_the_window() -> None:
    clock = _FrozenClock()
    limiter = RateLimiter(min_interval_s=5.0, clock=clock)
    assert limiter.allow(0) is True
    assert limiter.allow(0) is False


def test_rate_limiter_is_independent_per_entity() -> None:
    clock = _FrozenClock()
    limiter = RateLimiter(min_interval_s=5.0, clock=clock)
    assert limiter.allow(0) is True
    assert limiter.allow(1) is True


def test_rate_limiter_allows_again_after_the_window_elapses() -> None:
    clock = _FrozenClock()
    limiter = RateLimiter(min_interval_s=5.0, clock=clock)
    assert limiter.allow(0) is True
    clock.now += 5.0
    assert limiter.allow(0) is True


async def _resolved_rest_client(client) -> RestClient:
    rest = RestClient(
        base_url=str(client.make_url("")).rstrip("/"),
        username="installer",
        password="secret",
        session=client.session,
    )
    await rest.get_configuration()
    return rest


async def test_reconcile_now_reads_and_applies_the_true_value() -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)  # a stale/wrong guess

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="1"))
        rest = await _resolved_rest_client(client)
        reconciler = Reconciler(
            state=state, rest=rest, ingress_table=_ingress_table(), rate_limiter=RateLimiter()
        )

        await reconciler.reconcile_now(0, 0)

    assert state.values[0][0] is True
    assert state.dirty == {0}


async def test_read_one_bypasses_the_rate_limiter() -> None:
    # docs/04 §3.4: /get's "refresh all attributes" budgets once per *request*, not once per
    # attribute -- so the low-level read primitive must not enforce its own per-call limit.
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="1"))
        rest = await _resolved_rest_client(client)
        limiter = RateLimiter()
        limiter.allow(0)  # exhaust the budget up front
        reconciler = Reconciler(
            state=state, rest=rest, ingress_table=_ingress_table(), rate_limiter=limiter
        )

        await reconciler.read_one(0, 0)

    assert state.values[0][0] is True


async def test_reconcile_now_is_a_noop_when_rate_limited() -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="1"))
        rest = await _resolved_rest_client(client)
        limiter = RateLimiter()
        limiter.allow(0)  # consume the budget before the reconciler ever runs
        reconciler = Reconciler(
            state=state, rest=rest, ingress_table=_ingress_table(), rate_limiter=limiter
        )

        await reconciler.reconcile_now(0, 0)

    assert state.values[0][0] is False  # never read -- still the stale guess


async def test_reconcile_now_logs_and_survives_a_failed_read(caplog) -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config())
        # 404, not 500: a 5xx retries with real backoff inside RestClient (already covered by
        # test_rest.py) -- a 404 raises immediately, keeping this test about Reconciler, not retry.
        fake.set_error(DATAPOINT_PATH, 404)
        rest = await _resolved_rest_client(client)
        reconciler = Reconciler(
            state=state, rest=rest, ingress_table=_ingress_table(), rate_limiter=RateLimiter()
        )

        with caplog.at_level("WARNING"):
            await reconciler.reconcile_now(0, 0)  # must not raise

    assert state.values[0][0] is False
    assert "reconciliation" in caplog.text.lower()


async def test_schedule_reconciles_after_the_delay_when_still_unconfirmed() -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)
    state.unconfirmed[0] = 0b1

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="1"))
        rest = await _resolved_rest_client(client)
        reconciler = Reconciler(
            state=state,
            rest=rest,
            ingress_table=_ingress_table(),
            rate_limiter=RateLimiter(),
            delay_s=0.02,
        )

        reconciler.schedule(0, 0)
        await asyncio.sleep(0.1)

    assert state.values[0][0] is True
    assert state.unconfirmed[0] == 0


async def test_schedule_skips_reconciliation_once_confirmed_by_an_echo() -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, True)
    state.unconfirmed[0] = 0b1

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="0"))  # would prove a real read happened
        rest = await _resolved_rest_client(client)
        reconciler = Reconciler(
            state=state,
            rest=rest,
            ingress_table=_ingress_table(),
            rate_limiter=RateLimiter(),
            delay_s=0.02,
        )

        reconciler.schedule(0, 0)
        state.apply(0, 0, True, attr_bit=0b1)  # the echo arrives before the timer fires
        await asyncio.sleep(0.1)

    assert fake.request_count(DATAPOINT_PATH) == 0
    assert state.values[0][0] is True  # unchanged by any (nonexistent) reconciliation read


async def test_schedule_cancels_a_previous_timer_for_the_same_key() -> None:
    entities = [_entity(0)]
    state = StateStore(entities)
    state.seed(0, 0, False)
    state.unconfirmed[0] = 0b1

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        fake.set_configuration(_config(value="1"))
        rest = await _resolved_rest_client(client)
        reconciler = Reconciler(
            state=state,
            rest=rest,
            ingress_table=_ingress_table(),
            rate_limiter=RateLimiter(),
            delay_s=0.05,
        )

        reconciler.schedule(0, 0)
        await asyncio.sleep(0.02)
        reconciler.schedule(0, 0)  # restart the window; the first timer must not fire
        await asyncio.sleep(0.09)  # first timer's original deadline has passed by now

    assert fake.request_count(DATAPOINT_PATH) == 1
