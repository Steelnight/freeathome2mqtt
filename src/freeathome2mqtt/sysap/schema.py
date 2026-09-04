"""``TypedDict``\\ s for the configuration and WS frame JSON (docs/01 §4, §5.1). No validation.

These describe the *shape* the SysAP is expected to send, for mypy and for readers — they are not
enforced at runtime (that is ``model/profiles.py``'s job, for profiles, at load time only). Every
field is optional (``total=False``) because real installations omit keys the docs don't promise.
"""

from __future__ import annotations

from typing import TypedDict


class InOutPut(TypedDict, total=False):
    """One input (``idpXXXX``) or output (``odpXXXX``) datapoint (docs/01 §4.1)."""

    pairingID: int
    value: str


class Channel(TypedDict, total=False):
    """One channel on a device (docs/01 §4)."""

    displayName: str | None
    functionID: str
    floor: str
    room: str
    type: str
    inputs: dict[str, InOutPut]
    outputs: dict[str, InOutPut]
    parameters: dict[str, str]


class Device(TypedDict, total=False):
    """One physical (or virtual, docs/01 §4.5) device (docs/01 §4)."""

    displayName: str
    deviceId: str
    articleNumber: str
    interface: str | None
    nativeId: str
    floor: str
    room: str
    unresponsive: bool
    unresponsiveCounter: int
    defect: bool
    parameters: dict[str, str]
    channels: dict[str, Channel]


class Room(TypedDict, total=False):
    """One room within a floor (docs/01 §4.4)."""

    name: str


class Floor(TypedDict, total=False):
    """One floor. ``rooms`` may be ``None``, not just absent (P-14)."""

    name: str
    rooms: dict[str, Room] | None


class Floorplan(TypedDict, total=False):
    """The installation's floors and rooms (docs/01 §4.4)."""

    floors: dict[str, Floor]


class Configuration(TypedDict, total=False):
    """The body of ``GET /api/rest/configuration``, once unwrapped from its SysAP-UUID key."""

    sysapName: str
    sysap: dict[str, object]
    users: dict[str, object]  # shape unspecified by docs/01 §4; unused by the compile path
    floorplan: Floorplan
    devices: dict[str, Device]


ConfigurationResponse = dict[str, Configuration]
"""``{sysap_uuid: Configuration}`` — every REST response is wrapped this way (docs/01 §3)."""


class SceneChannelOutput(TypedDict, total=False):
    """One output value carried by a ``scenesTriggered`` frame (docs/01 §5.1)."""

    value: str
    pairingID: int


class SceneChannel(TypedDict, total=False):
    """One channel's outputs within a triggered scene."""

    outputs: dict[str, SceneChannelOutput]


class SceneTrigger(TypedDict, total=False):
    """One entry of ``scenesTriggered``, keyed by the triggering device's serial."""

    channels: dict[str, SceneChannel]


class WsFrameBody(TypedDict, total=False):
    """The body of one WebSocket frame, once unwrapped from its SysAP-UUID key (docs/01 §5.1)."""

    datapoints: dict[str, str]
    devices: dict[str, Device]
    devicesAdded: list[str]
    devicesRemoved: list[str]
    scenesTriggered: dict[str, SceneTrigger]
    parameters: dict[str, str]


WsFrame = dict[str, WsFrameBody]
"""``{sysap_uuid: WsFrameBody}`` — a single frame may carry several keys at once (docs/01 §5.1)."""


def parse_function_id(raw: str | None) -> int | None:
    """Parse a channel's ``functionID`` hex string (docs/01 §4.3).

    Returns ``None`` for a missing or unparseable value so the caller can skip the channel and
    count it, rather than raise or silently misinterpret it (P-06): ``int(v)`` instead of
    ``int(v, 16)`` maps hex ``"12"`` (0x12, a dimming actuator) onto decimal 12 with no error.
    """
    if not raw:
        return None
    try:
        return int(raw, 16)
    except ValueError:
        return None
