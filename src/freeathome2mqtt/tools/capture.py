"""Record a live SysAP session into a pseudonymised test fixture (docs/10 §3.3; docs/11 WP1).

Pseudonymisation is consistent across one capture: serials are remapped to a stable synthetic
namespace, display names and room/floor names are replaced with generated ones, and credentials
are never written (the ``users`` section is dropped entirely, since nothing downstream needs it).
This is what turns "my blinds don't work" into a regression fixture users can trust to attach.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson


@dataclass(frozen=True, slots=True)
class PseudonymisedCapture:
    """A pseudonymised configuration plus the serial remap used to produce it."""

    config: dict[str, Any]
    serial_map: Mapping[str, str] = field(default_factory=dict)


def pseudonymise(config: dict[str, Any]) -> dict[str, Any]:
    """Pseudonymise a captured ``/api/rest/configuration`` response (docs/10 §3.3).

    Convenience wrapper that discards the serial map; use `pseudonymise_with_map` directly when a
    WS session captured in the same run also needs pseudonymising, so serials remap consistently.
    """
    return pseudonymise_with_map(config).config


def _device_serial_map(devices: dict[str, Any]) -> dict[str, str]:
    return {serial: f"CAP{index:04d}" for index, serial in enumerate(sorted(devices), start=1)}


def _pseudonymise_rooms(rooms: dict[str, Any]) -> dict[str, Any]:
    new_rooms: dict[str, Any] = {}
    for index, (room_id, room) in enumerate(sorted(rooms.items()), start=1):
        if isinstance(room, dict) and "name" in room:
            new_rooms[room_id] = {**room, "name": f"Room {index}"}
        else:
            new_rooms[room_id] = room
    return new_rooms


def _pseudonymise_floorplan(floorplan: dict[str, Any]) -> dict[str, Any]:
    floors = floorplan.get("floors")
    if not isinstance(floors, dict):
        return dict(floorplan)
    new_floors: dict[str, Any] = {}
    for index, (floor_id, floor) in enumerate(sorted(floors.items()), start=1):
        if not isinstance(floor, dict):
            new_floors[floor_id] = floor
            continue
        new_floor = dict(floor)
        if "name" in new_floor:
            new_floor["name"] = f"Floor {index}"
        rooms = floor.get("rooms")
        # `rooms` may be None, not just absent (P-14) -- nothing to replace in that case.
        if isinstance(rooms, dict):
            new_floor["rooms"] = _pseudonymise_rooms(rooms)
        new_floors[floor_id] = new_floor
    return {**floorplan, "floors": new_floors}


def _pseudonymise_channels(channels: dict[str, Any], device_label: str) -> dict[str, Any]:
    new_channels: dict[str, Any] = {}
    for index, (channel_id, channel) in enumerate(sorted(channels.items()), start=1):
        if not isinstance(channel, dict):
            new_channels[channel_id] = channel
            continue
        new_channel = dict(channel)
        if "displayName" in new_channel:
            new_channel["displayName"] = f"{device_label} Channel {index}"
        new_channels[channel_id] = new_channel
    return new_channels


def _pseudonymise_devices(devices: dict[str, Any], serial_map: Mapping[str, str]) -> dict[str, Any]:
    new_devices: dict[str, Any] = {}
    for serial, device in devices.items():
        synthetic = serial_map.get(serial, serial)
        if not isinstance(device, dict):
            new_devices[synthetic] = device
            continue
        new_device = dict(device)
        if "displayName" in new_device:
            new_device["displayName"] = f"Device {synthetic}"
        channels = device.get("channels")
        if isinstance(channels, dict):
            new_device["channels"] = _pseudonymise_channels(channels, synthetic)
        new_devices[synthetic] = new_device
    return new_devices


def pseudonymise_with_map(config: dict[str, Any]) -> PseudonymisedCapture:
    """Pseudonymise a configuration response, also returning the serial remap it used."""
    result: dict[str, Any] = {}
    serial_map: dict[str, str] = {}
    for sysap_uuid, body in config.items():
        if not isinstance(body, dict):
            result[sysap_uuid] = body
            continue

        devices = body.get("devices")
        if isinstance(devices, dict):
            serial_map.update(_device_serial_map(devices))

        new_body = dict(body)
        new_body.pop("users", None)  # credentials never written
        if "sysapName" in new_body:
            new_body["sysapName"] = "Captured Installation"

        floorplan = body.get("floorplan")
        if isinstance(floorplan, dict):
            new_body["floorplan"] = _pseudonymise_floorplan(floorplan)
        if isinstance(devices, dict):
            new_body["devices"] = _pseudonymise_devices(devices, serial_map)

        result[sysap_uuid] = new_body

    return PseudonymisedCapture(config=result, serial_map=serial_map)


def _remap_datapoint_key(key: str, serial_map: Mapping[str, str]) -> str:
    serial, separator, rest = key.partition("/")
    if not separator:
        return key
    return f"{serial_map.get(serial, serial)}/{rest}"


def pseudonymise_ws_frame(frame: dict[str, Any], serial_map: Mapping[str, str]) -> dict[str, Any]:
    """Pseudonymise a captured WS frame using the serial map from `pseudonymise_with_map`."""
    result: dict[str, Any] = {}
    for sysap_uuid, body in frame.items():
        if not isinstance(body, dict):
            result[sysap_uuid] = body
            continue
        new_body = dict(body)

        datapoints = body.get("datapoints")
        if isinstance(datapoints, dict):
            new_body["datapoints"] = {
                _remap_datapoint_key(key, serial_map): value for key, value in datapoints.items()
            }

        for list_key in ("devicesAdded", "devicesRemoved"):
            serials = body.get(list_key)
            if isinstance(serials, list):
                new_body[list_key] = [
                    serial_map.get(serial, serial) if isinstance(serial, str) else serial
                    for serial in serials
                ]

        scenes = body.get("scenesTriggered")
        if isinstance(scenes, dict):
            new_body["scenesTriggered"] = {
                serial_map.get(scene_serial, scene_serial): scene
                for scene_serial, scene in scenes.items()
            }

        devices = body.get("devices")
        if isinstance(devices, dict):
            new_body["devices"] = _pseudonymise_devices(devices, serial_map)

        result[sysap_uuid] = new_body
    return result


def write_capture(path: Path, config: dict[str, Any], ws_frames: Sequence[dict[str, Any]]) -> None:
    """Pseudonymise `config` and `ws_frames` together and write them as one fixture to `path`."""
    pseudonymised = pseudonymise_with_map(config)
    payload = {
        "config": pseudonymised.config,
        "ws_frames": [
            pseudonymise_ws_frame(frame, pseudonymised.serial_map) for frame in ws_frames
        ],
    }
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))


async def capture(
    *,
    fetch_config: Callable[[], Awaitable[dict[str, Any]]],
    collect_ws_frames: Callable[[], Awaitable[Sequence[dict[str, Any]]]],
    output_path: Path,
) -> None:
    """Record a configuration snapshot plus a WS session, pseudonymised, to `output_path`.

    Takes the fetchers as callables rather than concrete SysAP clients because ``sysap/rest.py``
    and ``sysap/ws.py`` are still stubs (docs/11 WP2); wiring in real clients there needs no
    change here.
    """
    config = await fetch_config()
    ws_frames = await collect_ws_frames()
    write_capture(output_path, config, ws_frames)
