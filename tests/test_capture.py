"""Tests for tools/capture.py: pseudonymisation of a captured SysAP session (docs/10 §3.3)."""

from pathlib import Path

import orjson

from freeathome2mqtt.tools.capture import (
    capture,
    pseudonymise,
    pseudonymise_with_map,
    pseudonymise_ws_frame,
    write_capture,
)

PASSWORD = "hunter2-sentinel-password"

REAL_CONFIG_SAMPLE = {
    "00000000-0000-0000-0000-000000000000": {
        "sysapName": "Familie Müller",
        "sysap": {},
        "users": {"installer": {"name": "installer", "password_hint": PASSWORD}},
        "floorplan": {
            "floors": {
                "01": {
                    "name": "Erdgeschoss",
                    "rooms": {"0C": {"name": "Küche"}},
                },
                "02": {"name": "Obergeschoss", "rooms": None},  # P-14
            }
        },
        "devices": {
            "ABB7F500E17A": {
                "displayName": "Küche Decke",
                "deviceId": "910C",
                "interface": "TP",
                "floor": "01",
                "room": "0C",
                "channels": {
                    "ch0003": {
                        "displayName": "Deckenlicht",
                        "functionID": "7",
                        "floor": "01",
                        "room": "0C",
                        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0000": {"pairingID": 256, "value": "0"}},
                    }
                },
            }
        },
    }
}


def test_capture_contains_no_identifying_data() -> None:
    out = pseudonymise(REAL_CONFIG_SAMPLE)
    blob = orjson.dumps(out).decode()
    for secret in ("installer", PASSWORD, "ABB7F500E17A", "Küche", "Familie Müller"):
        assert secret not in blob


def test_pseudonymise_drops_users_entirely() -> None:
    out = pseudonymise(REAL_CONFIG_SAMPLE)
    body = out["00000000-0000-0000-0000-000000000000"]
    assert "users" not in body


def test_pseudonymise_replaces_sysap_name() -> None:
    out = pseudonymise(REAL_CONFIG_SAMPLE)
    body = out["00000000-0000-0000-0000-000000000000"]
    assert body["sysapName"] != "Familie Müller"


def test_pseudonymise_remaps_serials_consistently() -> None:
    result = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    assert "ABB7F500E17A" in result.serial_map
    synthetic = result.serial_map["ABB7F500E17A"]
    devices = result.config["00000000-0000-0000-0000-000000000000"]["devices"]
    assert synthetic in devices
    assert "ABB7F500E17A" not in devices


def test_pseudonymise_replaces_display_names() -> None:
    result = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    synthetic = result.serial_map["ABB7F500E17A"]
    device = result.config["00000000-0000-0000-0000-000000000000"]["devices"][synthetic]
    assert device["displayName"] != "Küche Decke"
    channel = device["channels"]["ch0003"]
    assert channel["displayName"] != "Deckenlicht"


def test_pseudonymise_replaces_room_and_floor_names() -> None:
    out = pseudonymise(REAL_CONFIG_SAMPLE)
    floors = out["00000000-0000-0000-0000-000000000000"]["floorplan"]["floors"]
    assert floors["01"]["name"] != "Erdgeschoss"
    assert floors["01"]["rooms"]["0C"]["name"] != "Küche"


def test_pseudonymise_handles_null_rooms() -> None:
    # P-14: a floor's `rooms` can be None, not just absent -- must not raise.
    out = pseudonymise(REAL_CONFIG_SAMPLE)
    floors = out["00000000-0000-0000-0000-000000000000"]["floorplan"]["floors"]
    assert floors["02"]["rooms"] is None


def test_pseudonymise_tolerates_malformed_shapes() -> None:
    # nasty.json-style quirks: non-dict body, non-dict floor/room/device/channel entries.
    config = {
        "00000000-0000-0000-0000-000000000000": {
            "sysapName": "Home",
            "floorplan": {"floors": {"01": "not-a-dict", "02": {"rooms": {"0A": "not-a-dict"}}}},
            "devices": {
                "AAA": "not-a-dict",
                "BBB": {"displayName": "B", "channels": {"ch0000": "not-a-dict"}},
            },
        },
        "11111111-1111-1111-1111-111111111111": "not-a-dict-body",
    }
    out = pseudonymise(config)
    assert out["11111111-1111-1111-1111-111111111111"] == "not-a-dict-body"
    devices = out["00000000-0000-0000-0000-000000000000"]["devices"]
    assert "AAA" not in devices  # remapped even though its value is malformed
    assert any(value == "not-a-dict" for value in devices.values())


def test_pseudonymise_is_deterministic() -> None:
    first = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    second = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    assert first.config == second.config
    assert first.serial_map == second.serial_map


def test_pseudonymise_ws_frame_remaps_datapoint_serial() -> None:
    result = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    synthetic = result.serial_map["ABB7F500E17A"]
    frame = {
        "00000000-0000-0000-0000-000000000000": {"datapoints": {"ABB7F500E17A/ch0003/odp0000": "1"}}
    }
    out = pseudonymise_ws_frame(frame, result.serial_map)
    datapoints = out["00000000-0000-0000-0000-000000000000"]["datapoints"]
    assert f"{synthetic}/ch0003/odp0000" in datapoints
    assert "ABB7F500E17A/ch0003/odp0000" not in datapoints


def test_pseudonymise_ws_frame_remaps_devices_added_removed() -> None:
    result = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    synthetic = result.serial_map["ABB7F500E17A"]
    frame = {
        "00000000-0000-0000-0000-000000000000": {
            "devicesAdded": ["ABB7F500E17A"],
            "devicesRemoved": [],
        }
    }
    out = pseudonymise_ws_frame(frame, result.serial_map)
    body = out["00000000-0000-0000-0000-000000000000"]
    assert body["devicesAdded"] == [synthetic]


def test_pseudonymise_ws_frame_remaps_scenes_and_devices_and_tolerates_non_dict_body() -> None:
    result = pseudonymise_with_map(REAL_CONFIG_SAMPLE)
    synthetic = result.serial_map["ABB7F500E17A"]
    frame = {
        "00000000-0000-0000-0000-000000000000": {
            "scenesTriggered": {"ABB7F500E17A": {"channels": {}}},
            "devices": {"ABB7F500E17A": {"displayName": "Küche Decke"}},
        },
        "11111111-1111-1111-1111-111111111111": "not-a-dict-body",
    }
    out = pseudonymise_ws_frame(frame, result.serial_map)
    body = out["00000000-0000-0000-0000-000000000000"]
    assert synthetic in body["scenesTriggered"]
    assert synthetic in body["devices"]
    assert out["11111111-1111-1111-1111-111111111111"] == "not-a-dict-body"


def test_write_capture_round_trips(tmp_path: Path) -> None:
    output_path = tmp_path / "capture.json"
    ws_frames = [
        {
            "00000000-0000-0000-0000-000000000000": {
                "datapoints": {"ABB7F500E17A/ch0003/odp0000": "1"}
            }
        }
    ]
    write_capture(output_path, REAL_CONFIG_SAMPLE, ws_frames)

    blob = output_path.read_bytes()
    assert b"ABB7F500E17A" not in blob
    assert b"Familie M" not in blob

    written = orjson.loads(blob)
    assert "config" in written
    assert "ws_frames" in written
    assert len(written["ws_frames"]) == 1


async def test_capture_orchestrates_fetch_and_write(tmp_path: Path) -> None:
    output_path = tmp_path / "session.json"
    calls = []

    async def fetch_config() -> dict[str, object]:
        calls.append("config")
        return REAL_CONFIG_SAMPLE

    async def collect_ws_frames() -> list[dict[str, object]]:
        calls.append("ws")
        return []

    await capture(
        fetch_config=fetch_config, collect_ws_frames=collect_ws_frames, output_path=output_path
    )

    assert calls == ["config", "ws"]
    assert output_path.exists()
