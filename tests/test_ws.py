"""Tests for sysap/ws.py: WsReader -- heartbeat, idle watchdog, reconnect, buffering, UUID
handling (docs/01 §5; docs/02 §7; docs/06 §2; docs/11 WP2).
"""

import asyncio

import pytest

from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.sysap.ws import WsAuthenticationError, WsBufferOverflowError, WsReader

SERIAL = "ABB7F500E17A"


def _reader_for(client, *, on_frame=None, **kwargs) -> WsReader:
    url = str(client.make_url("/fhapi/v1/api/ws"))
    return WsReader(
        url=url,
        username="installer",
        password="secret",
        session=client.session,
        on_frame=on_frame,
        **kwargs,
    )


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.01) -> None:
    # Generic condition polling for tests: no single event to wait on across call sites.
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


async def test_connects_and_dispatches_frames() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0003/odp0000": "1"}})
            await _wait_until(lambda: len(received) >= 1)
            assert received[0] == {"datapoints": {f"{SERIAL}/ch0003/odp0000": "1"}}
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_non_default_sysap_uuid() -> None:
    custom_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014"
    received = []
    async with running_fake_sysap(FakeSysAp(sysap_uuid=custom_uuid)) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)

            await fake.push_ws_frame({"datapoints": {"a": "1"}}, sysap_uuid=custom_uuid)
            await _wait_until(lambda: len(received) >= 1)
            assert reader.sysap_uuid == custom_uuid

            # A frame keyed by a different UUID must be dropped, not handed to on_frame.
            other_uuid = "11111111-1111-1111-1111-111111111111"
            await fake.push_ws_frame({"datapoints": {"b": "2"}}, sysap_uuid=other_uuid)
            await asyncio.sleep(0.1)
            assert len(received) == 1
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_ws_reconnects_on_silence() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(
            client,
            on_frame=received.append,
            heartbeat=None,
            idle_timeout=0.05,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)

            fake.hang_websocket()
            await _wait_until(lambda: reader.reconnect_count >= 2, timeout_seconds=5.0)

            fake.restore_websocket()
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0003/odp0000": "1"}})
            await _wait_until(lambda: len(received) >= 1)
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_ws_reconnects_after_clean_close() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(
            client, on_frame=received.append, backoff_initial=0.01, backoff_cap=0.02
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)

            await fake.drop_websocket()
            await _wait_until(lambda: reader.reconnect_count >= 2, timeout_seconds=5.0)

            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0003/odp0000": "1"}})
            await _wait_until(lambda: len(received) >= 1)
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_on_connected_fires_before_each_connection_can_dispatch_a_frame() -> None:
    connected_count = 0

    def _on_connected() -> None:
        nonlocal connected_count
        connected_count += 1

    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(
            client,
            on_frame=received.append,
            on_connected=_on_connected,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await _wait_until(lambda: connected_count >= 1)
            assert connected_count == 1

            await fake.drop_websocket()
            await _wait_until(lambda: reader.reconnect_count >= 2, timeout_seconds=5.0)
            await _wait_until(lambda: connected_count >= 2)
            assert connected_count == 2
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_on_connected_can_arm_buffering_before_any_frame_is_dispatched() -> None:
    # The real use case (docs/02 §7): re-arming buffering from inside `on_connected` closes the
    # reconnect-variant of P-22 -- a frame pushed the instant a connection opens must never reach
    # `on_frame` before the caller has had a chance to decide it should be buffered instead.
    received: list[object] = []
    reader_holder: list[WsReader] = []

    def _arm_buffering() -> None:
        reader_holder[0].start_buffering()

    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(
            client,
            on_frame=received.append,
            on_connected=_arm_buffering,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        reader_holder.append(reader)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await _wait_until(lambda: reader.buffered_frame_count >= 1)
            assert received == []
            assert reader.drain_buffer() == [{"datapoints": {"a": "1"}}]
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_buffering_holds_frames_until_drained() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        reader.start_buffering()
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await fake.push_ws_frame({"datapoints": {"b": "2"}})
            await _wait_until(lambda: reader.buffered_frame_count >= 2)

            assert received == []  # nothing dispatched live while buffering

            buffered = reader.drain_buffer()
            assert buffered == [{"datapoints": {"a": "1"}}, {"datapoints": {"b": "2"}}]
            assert reader.buffered_frame_count == 0

            await fake.push_ws_frame({"datapoints": {"c": "3"}})
            await _wait_until(lambda: len(received) >= 1)
            assert received == [{"datapoints": {"c": "3"}}]
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_buffer_overflow_raises() -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, buffer_limit=2)
        reader.start_buffering()
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await fake.push_ws_frame({"datapoints": {"b": "2"}})
            await fake.push_ws_frame({"datapoints": {"c": "3"}})

            with pytest.raises(WsBufferOverflowError):
                await asyncio.wait_for(task, timeout=5.0)
        finally:
            if not task.done():
                await reader.stop()
                await asyncio.wait_for(task, timeout=5.0)


async def test_dispatch_drops_malformed_json_and_keeps_running() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_raw_ws_text("not-json-at-all")
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await _wait_until(lambda: len(received) >= 1)
            assert received == [{"datapoints": {"a": "1"}}]
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_dispatch_drops_non_object_top_level_frame() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_raw_ws_text("[1, 2, 3]")
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await _wait_until(lambda: len(received) >= 1)
            assert received == [{"datapoints": {"a": "1"}}]
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_dispatch_drops_non_object_frame_body() -> None:
    received = []
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=received.append)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_raw_ws_text('{"00000000-0000-0000-0000-000000000000": "not-an-object"}')
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await _wait_until(lambda: len(received) >= 1)
            assert received == [{"datapoints": {"a": "1"}}]
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_dispatch_without_on_frame_callback_does_not_raise() -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(client, on_frame=None)
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            await fake.push_ws_frame({"datapoints": {"a": "1"}})
            await asyncio.sleep(0.05)
            assert not task.done()
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_connection_failure_is_retried_without_crashing() -> None:
    async with running_fake_sysap(FakeSysAp()) as (_fake, client):
        # Port 1 is a well-known unassigned/refused port -- the connection itself fails.
        reader = WsReader(
            url="http://127.0.0.1:1/fhapi/v1/api/ws",
            username="installer",
            password="secret",
            session=client.session,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.backoff_attempt >= 2, timeout_seconds=5.0)
            assert reader.reconnect_count == 0
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_backoff_attempt_resets_after_a_successful_reconnect() -> None:
    # Regression: attempt used to reset only when _connect_and_dispatch() returned normally,
    # which never happens (its receive loop only ever exits by raising) -- dead code that left
    # backoff climbing forever after the first disconnect. It must reset on every reconnect.
    async with running_fake_sysap(FakeSysAp()) as (fake, client):
        reader = _reader_for(
            client,
            heartbeat=None,
            idle_timeout=0.05,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.reconnect_count >= 1)
            assert reader.backoff_attempt == 0

            fake.hang_websocket()
            await _wait_until(lambda: reader.reconnect_count >= 3, timeout_seconds=5.0)
            fake.restore_websocket()

            assert reader.backoff_attempt == 0
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_run_raises_ws_authentication_error_immediately_on_401() -> None:
    # docs/06 §3: "Auth failure (401/403) -> Immediately. Do not retry." -- unlike an ordinary
    # connection failure, this must not enter the backoff/retry loop at all.
    fake = FakeSysAp()
    fake.require_username("jid-user")
    async with running_fake_sysap(fake) as (_fake, client):
        reader = _reader_for(client, backoff_initial=0.01, backoff_cap=0.02)
        with pytest.raises(WsAuthenticationError):
            await asyncio.wait_for(reader.run(), timeout=5.0)
        assert reader.reconnect_count == 0
        assert reader.backoff_attempt == 0


async def test_run_still_retries_ordinary_connection_failures_after_a_401_fix() -> None:
    # Confirms the 401 fast-path doesn't disturb the ordinary retry-forever behaviour for
    # ordinary failures once credentials are no longer the problem.
    async with running_fake_sysap(FakeSysAp()) as (_fake, client):
        reader = WsReader(
            url="http://127.0.0.1:1/fhapi/v1/api/ws",
            username="installer",
            password="secret",
            session=client.session,
            backoff_initial=0.01,
            backoff_cap=0.02,
        )
        task = asyncio.create_task(reader.run())
        try:
            await _wait_until(lambda: reader.backoff_attempt >= 2, timeout_seconds=5.0)
            assert reader.reconnect_count == 0
        finally:
            await reader.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_connect_once_succeeds_silently_with_valid_credentials() -> None:
    async with running_fake_sysap(FakeSysAp()) as (_fake, client):
        reader = _reader_for(client)
        await reader.connect_once()  # must not raise
        assert reader.reconnect_count == 0  # never entered the receive loop


async def test_connect_once_raises_ws_authentication_error_on_401() -> None:
    fake = FakeSysAp()
    fake.require_username("jid-user")
    async with running_fake_sysap(fake) as (_fake, client):
        reader = _reader_for(client)
        with pytest.raises(WsAuthenticationError):
            await reader.connect_once()


async def test_connect_once_swallows_non_auth_connection_failures() -> None:
    # A booting SysAP that isn't reachable yet is `run()`'s retry-forever job, not this one-shot
    # startup probe's -- it must not raise for anything other than a 401/403.
    async with running_fake_sysap(FakeSysAp()) as (_fake, client):
        reader = WsReader(
            url="http://127.0.0.1:1/fhapi/v1/api/ws",
            username="installer",
            password="secret",
            session=client.session,
        )
        await reader.connect_once()  # must not raise


async def test_stop_ends_the_run_loop() -> None:
    async with running_fake_sysap(FakeSysAp()) as (_fake, client):
        reader = _reader_for(client)
        task = asyncio.create_task(reader.run())
        await _wait_until(lambda: reader.reconnect_count >= 1)
        await reader.stop()
        await asyncio.wait_for(task, timeout=5.0)
        assert task.done()
        assert task.exception() is None
