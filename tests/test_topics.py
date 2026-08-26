"""Tests for mqtt/topics.py: the sole source of topic strings (docs/02 §2; docs/04 §1; docs/11
WP5).
"""

import pytest

from freeathome2mqtt.mqtt.topics import (
    WildcardTopicError,
    assert_publishable,
    bridge_devices_topic,
    bridge_event_topic,
    bridge_logging_topic,
    bridge_request_topic,
    bridge_response_topic,
    bridge_state_topic,
    command_subscriptions,
    entity_availability_topic,
    entity_event_topic,
    entity_get_topic,
    entity_set_attribute_topic,
    entity_set_topic,
    entity_state_topic,
    ha_birth_topic,
)

BASE = "freeathome2mqtt"


def test_entity_state_topic() -> None:
    assert entity_state_topic(BASE, "kueche_deckenlicht") == "freeathome2mqtt/kueche_deckenlicht"


def test_entity_subtopics_are_built_from_the_state_topic() -> None:
    state_topic = entity_state_topic(BASE, "kueche_deckenlicht")
    assert entity_set_topic(state_topic) == "freeathome2mqtt/kueche_deckenlicht/set"
    assert entity_get_topic(state_topic) == "freeathome2mqtt/kueche_deckenlicht/get"
    assert (
        entity_availability_topic(state_topic) == "freeathome2mqtt/kueche_deckenlicht/availability"
    )
    assert entity_event_topic(state_topic) == "freeathome2mqtt/kueche_deckenlicht/event"
    assert entity_set_attribute_topic(state_topic, "brightness") == (
        "freeathome2mqtt/kueche_deckenlicht/set/brightness"
    )


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        (bridge_state_topic, "freeathome2mqtt/bridge/state"),
        (bridge_devices_topic, "freeathome2mqtt/bridge/devices"),
        (bridge_event_topic, "freeathome2mqtt/bridge/event"),
        (bridge_logging_topic, "freeathome2mqtt/bridge/logging"),
    ],
)
def test_bridge_topics(func, expected) -> None:
    assert func(BASE) == expected


def test_bridge_request_response_topics() -> None:
    assert bridge_request_topic(BASE, "reload") == "freeathome2mqtt/bridge/request/reload"
    assert bridge_response_topic(BASE, "reload") == "freeathome2mqtt/bridge/response/reload"


def test_command_subscriptions_are_exactly_the_adr006_set() -> None:
    # docs/00 ADR-006 / docs/04 §1: narrow subscriptions only, never <base>/#.
    assert command_subscriptions(BASE) == (
        "freeathome2mqtt/+/set",
        "freeathome2mqtt/+/set/+",
        "freeathome2mqtt/+/get",
        "freeathome2mqtt/bridge/request/#",
    )


def test_ha_birth_topic() -> None:
    assert ha_birth_topic("homeassistant") == "homeassistant/status"


def test_assert_publishable_accepts_a_normal_topic() -> None:
    assert_publishable("freeathome2mqtt/kueche_deckenlicht")  # must not raise


@pytest.mark.parametrize(
    "topic", ["freeathome2mqtt/+/set", "freeathome2mqtt/bridge/#", "a+b", "a#"]
)
def test_assert_publishable_rejects_wildcards(topic) -> None:
    # P-38: a carelessly-slugged name must never produce a publishable wildcard topic.
    with pytest.raises(WildcardTopicError):
        assert_publishable(topic)
