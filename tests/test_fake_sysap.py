"""Tests for the fake SysAP test double itself (docs/10 §3.1) -- it must be trustworthy before
RestClient/WsReader tests can lean on it (docs/10 §2: prefer a fake over mocking the transport).
"""

import asyncio

import pytest
from aiohttp import WSMsgType

from fakes.fake_sysap import FakeSysAp, running_fake_sysap

SERIAL = "ABB7F500E17A"

SAMPLE_CONFIG = {
    "sysapName": "Test House",
    "sysap": {},
    "users": {},
    "floorplan": {"floors": {}},
    "devices": {
        SERIAL: {
            "displayName": "Ceiling Light",
            "channels": {
                "ch0003": {
                    "displayName": "Switch",
                    "functionID": "7",
                    "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                    "outputs": {"odp0000": {"pairingID": 256, "value": "0"}},
                }
            },
        }
    },
}


async def test_configuration_is_served_wrapped_by_sysap_uuid() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        body = await resp.json()
        assert set(body) == {fake.sysap_uuid}
        assert body[fake.sysap_uuid]["devices"][SERIAL]["displayName"] == "Ceiling Light"


async def test_set_datapoint_mutates_stored_configuration() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_datapoint(SERIAL, "ch0003", "odp0000", "1")
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        body = await resp.json()
        channel = body[fake.sysap_uuid]["devices"][SERIAL]["channels"]["ch0003"]
        assert channel["outputs"]["odp0000"]["value"] == "1"


async def test_fake_does_not_mutate_the_callers_configuration_dict() -> None:
    # A previous bug: FakeSysAp stored the passed-in dict by reference, so set_datapoint on one
    # fake silently corrupted every other test sharing the same fixture dict.
    fake = FakeSysAp(configuration=SAMPLE_CONFIG)
    fake.set_datapoint(SERIAL, "ch0003", "odp0000", "1")
    assert (
        SAMPLE_CONFIG["devices"][SERIAL]["channels"]["ch0003"]["outputs"]["odp0000"]["value"] == "0"
    )


async def test_set_datapoint_rejects_unknown_datapoint() -> None:
    fake = FakeSysAp(configuration=SAMPLE_CONFIG)
    with pytest.raises(KeyError):
        fake.set_datapoint(SERIAL, "ch0003", "odp9999", "1")


async def test_set_error_forces_status_until_cleared() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_error("/fhapi/v1/api/rest/configuration", 502)
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        assert resp.status == 502

        fake.clear_error("/fhapi/v1/api/rest/configuration")
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        assert resp.status == 200


async def test_set_error_with_times_auto_clears() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_error("/fhapi/v1/api/rest/configuration", 502, times=2)

        first = await client.get("/fhapi/v1/api/rest/configuration")
        second = await client.get("/fhapi/v1/api/rest/configuration")
        third = await client.get("/fhapi/v1/api/rest/configuration")

        assert [first.status, second.status, third.status] == [502, 502, 200]


async def test_max_concurrent_forces_502_above_threshold() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_latency(50)
        fake.set_max_concurrent(2)

        responses = await asyncio.gather(
            *(client.get("/fhapi/v1/api/rest/configuration") for _ in range(4))
        )
        statuses = sorted(r.status for r in responses)
        assert statuses == [200, 200, 502, 502]
        assert fake.peak_concurrency() == 4


async def test_request_count_tracks_per_path() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        await client.get("/fhapi/v1/api/rest/configuration")
        await client.get("/fhapi/v1/api/rest/configuration")
        assert fake.request_count("/fhapi/v1/api/rest/configuration") == 2


async def test_set_raw_response_returns_body_verbatim() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_raw_response("/fhapi/v1/api/rest/configuration", {"weird": True})
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        body = await resp.json()
        assert body == {"weird": True}

        fake.clear_raw_response("/fhapi/v1/api/rest/configuration")
        resp = await client.get("/fhapi/v1/api/rest/configuration")
        body = await resp.json()
        assert set(body) == {fake.sysap_uuid}


async def test_get_datapoint_roundtrip() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        path = f"/fhapi/v1/api/rest/datapoint/{fake.sysap_uuid}/{SERIAL}.ch0003.odp0000"
        resp = await client.get(path)
        body = await resp.json()
        assert body[fake.sysap_uuid]["values"] == ["0"]


async def test_put_datapoint_updates_value_and_reports_result() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        path = f"/fhapi/v1/api/rest/datapoint/{fake.sysap_uuid}/{SERIAL}.ch0003.idp0000"
        resp = await client.put(path, data=b"1")
        body = await resp.json()
        assert body[fake.sysap_uuid]["result"] == "OK"
        config_resp = await client.get("/fhapi/v1/api/rest/configuration")
        config_body = await config_resp.json()
        channel = config_body[fake.sysap_uuid]["devices"][SERIAL]["channels"]["ch0003"]
        assert channel["inputs"]["idp0000"]["value"] == "1"


async def test_put_datapoint_write_result_is_configurable() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_write_result("FAILED")
        path = f"/fhapi/v1/api/rest/datapoint/{fake.sysap_uuid}/{SERIAL}.ch0003.idp0000"
        resp = await client.put(path, data=b"1")
        body = await resp.json()
        assert body[fake.sysap_uuid]["result"] == "FAILED"


async def test_ws_push_frame_is_received_by_client() -> None:
    async with (
        running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client),
        client.ws_connect("/fhapi/v1/api/ws") as ws,
    ):
        await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0003/odp0000": "1"}})
        msg = await ws.receive(timeout=5)
        assert msg.type == WSMsgType.TEXT
        assert SERIAL in msg.data


async def test_push_raw_ws_text_is_received_verbatim() -> None:
    async with (
        running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client),
        client.ws_connect("/fhapi/v1/api/ws") as ws,
    ):
        await fake.push_raw_ws_text("not-json-at-all")
        msg = await ws.receive(timeout=5)
        assert msg.type == WSMsgType.TEXT
        assert msg.data == "not-json-at-all"


async def test_drop_websocket_closes_the_connection() -> None:
    async with (
        running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client),
        client.ws_connect("/fhapi/v1/api/ws") as ws,
    ):
        await fake.drop_websocket()
        msg = await ws.receive(timeout=5)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING)


async def test_hang_websocket_stops_pong_responses() -> None:
    async with (
        running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client),
        client.ws_connect("/fhapi/v1/api/ws", autoping=False) as ws,
    ):
        fake.hang_websocket()
        await ws.ping()
        with pytest.raises(TimeoutError):
            await ws.receive(timeout=0.2)

        fake.restore_websocket()
        await ws.ping()
        msg = await ws.receive(timeout=5)
        assert msg.type == WSMsgType.PONG
