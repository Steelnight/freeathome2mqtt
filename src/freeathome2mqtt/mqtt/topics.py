"""The sole place a topic string is constructed (docs/02 §2; docs/11 WP5).

Every other module builds a topic by calling one of these functions, never by formatting a string
itself -- this is what prevents the class of bug where one code path publishes to
`.../availability` and another subscribes to `.../available` (docs/02 §2).
"""

from __future__ import annotations


class WildcardTopicError(ValueError):
    """A topic meant for publishing contains an MQTT wildcard character (P-38).

    Slugification restricts topic segments to ``[a-z0-9_]``, so this should be unreachable in
    practice; `assert_publishable` is the belt to that suspenders, checked right before publish.
    """


def entity_state_topic(base_topic: str, slug: str) -> str:
    return f"{base_topic}/{slug}"


def entity_set_topic(state_topic: str) -> str:
    return f"{state_topic}/set"


def entity_set_attribute_topic(state_topic: str, attribute: str) -> str:
    return f"{state_topic}/set/{attribute}"


def entity_get_topic(state_topic: str) -> str:
    return f"{state_topic}/get"


def entity_availability_topic(state_topic: str) -> str:
    return f"{state_topic}/availability"


def entity_event_topic(state_topic: str) -> str:
    return f"{state_topic}/event"


def bridge_state_topic(base_topic: str) -> str:
    return f"{base_topic}/bridge/state"


def bridge_info_topic(base_topic: str) -> str:
    return f"{base_topic}/bridge/info"


def bridge_devices_topic(base_topic: str) -> str:
    return f"{base_topic}/bridge/devices"


def bridge_devices_part_topic(base_topic: str, part: int) -> str:
    """docs/04 §4.3, P-41: one indexed part of a `bridge/devices` payload too large for
    `mqtt.maximum_packet_size` -- `bridge_devices_topic` itself becomes an index message
    listing these instead of the flat inventory.
    """
    return f"{base_topic}/bridge/devices/{part}"


def bridge_event_topic(base_topic: str) -> str:
    return f"{base_topic}/bridge/event"


def bridge_logging_topic(base_topic: str) -> str:
    return f"{base_topic}/bridge/logging"


def bridge_request_topic(base_topic: str, command: str) -> str:
    return f"{base_topic}/bridge/request/{command}"


def bridge_response_topic(base_topic: str, command: str) -> str:
    return f"{base_topic}/bridge/response/{command}"


def command_subscriptions(base_topic: str) -> tuple[str, ...]:
    """The narrow subscription set (ADR-006) -- exactly these, never `<base_topic>/#`."""
    return (
        f"{base_topic}/+/set",
        f"{base_topic}/+/set/+",
        f"{base_topic}/+/get",
        f"{base_topic}/bridge/request/#",
    )


def ha_birth_topic(discovery_topic: str) -> str:
    return f"{discovery_topic}/status"


def ha_discovery_config_topic(
    discovery_topic: str, component: str, node_id: str, object_id: str
) -> str:
    """docs/04 §6.1: `<node_id>` is the entity id (stable across renames), `<object_id>` seeds
    Home Assistant's initial `entity_id`.
    """
    return f"{discovery_topic}/{component}/{node_id}/{object_id}/config"


def assert_publishable(topic: str) -> None:
    """Raise if `topic` contains an MQTT wildcard character (P-38)."""
    if "+" in topic or "#" in topic:
        raise WildcardTopicError(f"refusing to publish to wildcard topic: {topic!r}")
