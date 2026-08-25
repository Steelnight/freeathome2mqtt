"""Tests for model/entity.py: the dataclass shapes (docs/03 §2, §3.2, §3.3; docs/11 WP3)."""

from __future__ import annotations

import dataclasses

import pytest

from freeathome2mqtt.model.entity import (
    AttributeSpec,
    AttrKind,
    Binding,
    CommandSpec,
    EgressBinding,
    Entity,
)
from freeathome2mqtt.sysap.codes import Pairing

# ------------------------------------------------------------------------------- AttrKind


def test_attr_kind_state_and_event_are_distinct_ints() -> None:
    assert isinstance(AttrKind.STATE, int)
    assert isinstance(AttrKind.EVENT, int)
    assert AttrKind.STATE != AttrKind.EVENT


# ------------------------------------------------------------------------------- Entity


def _make_entity(**overrides: object) -> Entity:
    # The docs/03 §8 worked example, field for field.
    fields: dict[str, object] = {
        "idx": 17,
        "id": "ABB7F500E17A_ch0003",
        "profile": "dimming_actuator",
        "name": "Deckenlicht",
        "area": "Küche",
        "device_serial": "ABB7F500E17A",
        "channel_id": "ch0003",
        "attr_names": ("state", "brightness"),
        "attr_kinds": (AttrKind.STATE, AttrKind.STATE),
        "state_topic": "freeathome2mqtt/kueche_deckenlicht",
        "set_topic": "freeathome2mqtt/kueche_deckenlicht/set",
        "get_topic": "freeathome2mqtt/kueche_deckenlicht/get",
        "availability_topic": None,
        "optimistic": True,
        "discovery": (),
    }
    fields.update(overrides)
    return Entity(**fields)  # type: ignore[arg-type]


def test_entity_holds_the_worked_example_fields() -> None:
    entity = _make_entity()
    assert entity.idx == 17
    assert entity.id == "ABB7F500E17A_ch0003"
    assert entity.attr_names == ("state", "brightness")
    assert entity.attr_kinds == (AttrKind.STATE, AttrKind.STATE)


def test_entity_is_mutable() -> None:
    # docs/03 §2 declares Entity without frozen=True, unlike Binding/EgressBinding below --
    # mirrored here exactly, since the doc's runtime-representation snippet is normative.
    entity = _make_entity()
    entity.name = "Renamed"
    assert entity.name == "Renamed"


def test_entity_has_no_dict_slots_only() -> None:
    entity = _make_entity()
    with pytest.raises(AttributeError):
        entity.not_a_field = "x"  # type: ignore[attr-defined]


# ------------------------------------------------------------------------------- Binding


def _decode_bool01(raw: str) -> bool | None:
    return None if raw == "" else raw == "1"


def test_binding_holds_the_worked_example_fields() -> None:
    binding = Binding(
        entity_idx=17, attr_idx=0, decode=_decode_bool01, kind=AttrKind.STATE, attr_bit=0b01
    )
    assert binding.entity_idx == 17
    assert binding.attr_bit == 0b01
    assert binding.decode("1") is True


def test_binding_is_frozen() -> None:
    binding = Binding(
        entity_idx=0, attr_idx=0, decode=_decode_bool01, kind=AttrKind.STATE, attr_bit=1
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        binding.entity_idx = 1  # type: ignore[misc]


def test_binding_has_no_dict_slots_only() -> None:
    binding = Binding(
        entity_idx=0, attr_idx=0, decode=_decode_bool01, kind=AttrKind.STATE, attr_bit=1
    )
    with pytest.raises(AttributeError):
        binding.not_a_field = "x"  # type: ignore[attr-defined]


# ------------------------------------------------------------------------------- EgressBinding


def _encode_bool01(value: object) -> str:
    return "1" if value else "0"


def _validate_passthrough(value: object) -> object:
    return value


def test_egress_binding_holds_the_worked_example_fields() -> None:
    binding = EgressBinding(
        entity_idx=17,
        rest_path="ABB7F500E17A.ch0003.idp0000",
        encode=_encode_bool01,
        continuous=False,
        optimistic_attr=0,
        validate=_validate_passthrough,
    )
    assert binding.rest_path == "ABB7F500E17A.ch0003.idp0000"
    assert binding.encode(True) == "1"
    assert binding.optimistic_attr == 0


def test_egress_binding_optimistic_attr_may_be_none() -> None:
    binding = EgressBinding(
        entity_idx=0,
        rest_path="x",
        encode=_encode_bool01,
        continuous=False,
        optimistic_attr=None,
        validate=_validate_passthrough,
    )
    assert binding.optimistic_attr is None


def test_egress_binding_is_frozen() -> None:
    binding = EgressBinding(
        entity_idx=0,
        rest_path="x",
        encode=_encode_bool01,
        continuous=False,
        optimistic_attr=None,
        validate=_validate_passthrough,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        binding.continuous = True  # type: ignore[misc]


def test_egress_binding_has_no_dict_slots_only() -> None:
    binding = EgressBinding(
        entity_idx=0,
        rest_path="x",
        encode=_encode_bool01,
        continuous=False,
        optimistic_attr=None,
        validate=_validate_passthrough,
    )
    with pytest.raises(AttributeError):
        binding.not_a_field = "x"  # type: ignore[attr-defined]


# ------------------------------------------------------------------------------- AttributeSpec


def test_attribute_spec_required_fields_and_defaults() -> None:
    spec = AttributeSpec(name="state", pairing=Pairing.AL_INFO_ON_OFF, codec="bool01")
    assert spec.name == "state"
    assert spec.pairing is Pairing.AL_INFO_ON_OFF
    assert spec.codec == "bool01"
    assert spec.kind == AttrKind.STATE
    assert spec.unit is None
    assert spec.values is None
    assert spec.default is None
    assert spec.entity_category is None
    assert spec.precision is None
    assert spec.codec_params == {}


def test_attribute_spec_accepts_every_documented_field() -> None:
    # docs/03 §3.2 worked example: AL_INFO_FORCE, asymmetric to the command's own map (P-03).
    spec = AttributeSpec(
        name="forced_position",
        pairing=Pairing.AL_INFO_FORCE,
        codec="enum",
        kind=AttrKind.STATE,
        unit=None,
        values={"4": "forced_on", "5": "forced_off"},
        default="unknown",
        entity_category="diagnostic",
        precision=None,
        codec_params={"default": "unknown"},
    )
    assert spec.values == {"4": "forced_on", "5": "forced_off"}
    assert spec.entity_category == "diagnostic"
    assert spec.codec_params == {"default": "unknown"}


def test_attribute_spec_event_kind() -> None:
    spec = AttributeSpec(
        name="pressed", pairing=Pairing.AL_INFO_ON_OFF, codec="trigger", kind=AttrKind.EVENT
    )
    assert spec.kind == AttrKind.EVENT


def test_attribute_spec_is_frozen() -> None:
    spec = AttributeSpec(name="state", pairing=Pairing.AL_INFO_ON_OFF, codec="bool01")
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.codec = "int"  # type: ignore[misc]


def test_attribute_spec_codec_params_defaults_are_not_shared() -> None:
    # CLAUDE.md rule 6: no mutable default shared across instances.
    first = AttributeSpec(name="a", pairing=Pairing.AL_INFO_ON_OFF, codec="bool01")
    second = AttributeSpec(name="b", pairing=Pairing.AL_INFO_ON_OFF, codec="bool01")
    assert first.codec_params is not second.codec_params


# ------------------------------------------------------------------------------- CommandSpec


def test_command_spec_required_fields_and_defaults() -> None:
    spec = CommandSpec(name="state", pairing=Pairing.AL_SWITCH_ON_OFF, codec="bool01")
    assert spec.name == "state"
    assert spec.pairing is Pairing.AL_SWITCH_ON_OFF
    assert spec.codec == "bool01"
    assert spec.values is None
    assert spec.range is None
    assert spec.continuous is False
    assert spec.optimistic is None
    assert spec.confirm is True
    assert spec.codec_params == {}


def test_command_spec_accepts_every_documented_field() -> None:
    # docs/03 §3 worked example: brightness command, clamped range, debounced, optimistic.
    spec = CommandSpec(
        name="brightness",
        pairing=Pairing.AL_ABSOLUTE_SET_VALUE_CONTROL,
        codec="percent_int",
        values=None,
        range=(1.0, 100.0),
        continuous=True,
        optimistic="brightness",
        confirm=True,
        codec_params={},
    )
    assert spec.range == (1.0, 100.0)
    assert spec.continuous is True
    assert spec.optimistic == "brightness"


def test_command_spec_enum_values_map_is_the_inverse_direction_of_the_attributes_map() -> None:
    # P-03: AL_FORCED writes 2/3 while AL_INFO_FORCE reads 4/5 -- independent maps, never derived
    # from one another.
    spec = CommandSpec(
        name="forced_position",
        pairing=Pairing.AL_FORCED,
        codec="enum",
        values={"deactivated": "0", "forced_on": "3", "forced_off": "2"},
    )
    assert spec.values == {"deactivated": "0", "forced_on": "3", "forced_off": "2"}


def test_command_spec_is_frozen() -> None:
    spec = CommandSpec(name="state", pairing=Pairing.AL_SWITCH_ON_OFF, codec="bool01")
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.confirm = False  # type: ignore[misc]


def test_command_spec_codec_params_defaults_are_not_shared() -> None:
    first = CommandSpec(name="a", pairing=Pairing.AL_SWITCH_ON_OFF, codec="bool01")
    second = CommandSpec(name="b", pairing=Pairing.AL_SWITCH_ON_OFF, codec="bool01")
    assert first.codec_params is not second.codec_params
