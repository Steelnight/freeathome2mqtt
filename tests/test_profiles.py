"""Tests for model/profiles.py: profile loading, schema validation, merge order (docs/03 §3;
docs/11 WP3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from freeathome2mqtt.model.entity import AttrKind
from freeathome2mqtt.model.profiles import (
    ProfileError,
    ProfileRegistry,
    build_registry,
    load_profile_directory,
    load_profile_file,
    load_profile_registry,
    parse_profile,
)
from freeathome2mqtt.sysap.codes import Function, Pairing, Parameter

# ------------------------------------------------------------------------------- fixtures


def _switch_profile(**overrides: Any) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": "switch_actuator",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {
            "state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"},
        },
        "commands": {
            "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01"},
        },
    }
    profile.update(overrides)
    return profile


def _dimming_profile(**overrides: Any) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": "dimming_actuator",
        "description": "Dimmable light actuator",
        "functions": ["FID_DIMMING_ACTUATOR"],
        "requires": {"outputs": ["AL_INFO_ON_OFF", "AL_INFO_ACTUAL_DIMMING_VALUE"]},
        "attributes": {
            "state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"},
            "brightness": {
                "pairing": "AL_INFO_ACTUAL_DIMMING_VALUE",
                "codec": "percent_int",
                "unit": "%",
            },
            "forced_position": {
                "pairing": "AL_INFO_FORCE",
                "codec": "enum",
                "values": {"0": "deactivated", "4": "forced_on", "5": "forced_off"},
                "default": "unknown",
                "entity_category": "diagnostic",
            },
        },
        "commands": {
            "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01", "optimistic": "state"},
            "brightness": {
                "pairing": "AL_ABSOLUTE_SET_VALUE_CONTROL",
                "codec": "percent_int",
                "range": [1, 100],
                "continuous": True,
                "optimistic": "brightness",
            },
            "forced_position": {
                "pairing": "AL_FORCED",
                "codec": "enum",
                "values": {"deactivated": "0", "forced_on": "3", "forced_off": "2"},
                "optimistic": "forced_position",
            },
        },
        "parameters": {"min_brightness": "PID_DIMMING_ACTUATOR_MIN_BRIGHTNESS"},
    }
    profile.update(overrides)
    return profile


# ------------------------------------------------------------------------------- parse_profile


def test_parse_profile_builds_a_profile_from_a_minimal_valid_dict() -> None:
    profile = parse_profile(_switch_profile(), source="<test>")
    assert profile.id == "switch_actuator"
    assert profile.functions == (Function.FID_SWITCH_ACTUATOR,)
    assert profile.attributes["state"].pairing is Pairing.AL_INFO_ON_OFF
    assert profile.attributes["state"].codec == "bool01"
    assert profile.commands["state"].pairing is Pairing.AL_SWITCH_ON_OFF


def test_parse_profile_resolves_requires_and_parameters() -> None:
    profile = parse_profile(_dimming_profile(), source="<test>")
    assert profile.requires_outputs == (
        Pairing.AL_INFO_ON_OFF,
        Pairing.AL_INFO_ACTUAL_DIMMING_VALUE,
    )
    assert profile.requires_inputs == ()
    assert profile.parameters == {"min_brightness": Parameter.PID_DIMMING_ACTUATOR_MIN_BRIGHTNESS}


def test_parse_profile_defaults() -> None:
    profile = parse_profile(_switch_profile(), source="<test>")
    assert profile.description is None
    assert profile.availability == "device"
    assert profile.transform is None
    assert profile.homeassistant == {}
    assert profile.attributes["state"].kind == AttrKind.STATE
    assert profile.commands["state"].continuous is False
    assert profile.commands["state"].confirm is True
    assert profile.commands["state"].optimistic is None


def test_parse_profile_attribute_event_kind() -> None:
    raw = _switch_profile()
    raw["attributes"]["pressed"] = {"pairing": "AL_INFO_FORCE", "codec": "trigger", "kind": "event"}
    profile = parse_profile(raw, source="<test>")
    assert profile.attributes["pressed"].kind == AttrKind.EVENT


def test_parse_profile_command_values_map_is_independent_of_the_attribute_map() -> None:
    # P-03: AL_FORCED writes 2/3 but AL_INFO_FORCE reads 4/5 -- independent maps.
    profile = parse_profile(_dimming_profile(), source="<test>")
    assert profile.attributes["forced_position"].values == {
        "0": "deactivated",
        "4": "forced_on",
        "5": "forced_off",
    }
    assert profile.commands["forced_position"].values == {
        "deactivated": "0",
        "forced_on": "3",
        "forced_off": "2",
    }


def test_parse_profile_command_range_becomes_a_float_tuple() -> None:
    profile = parse_profile(_dimming_profile(), source="<test>")
    assert profile.commands["brightness"].range == (1.0, 100.0)


def test_parse_profile_extra_attribute_keys_become_codec_params() -> None:
    raw = _switch_profile()
    raw["attributes"]["temperature"] = {
        "pairing": "AL_INFO_FORCE",
        "codec": "scaled",
        "factor": 0.1,
        "offset": -40,
    }
    profile = parse_profile(raw, source="<test>")
    assert profile.attributes["temperature"].codec_params == {"factor": 0.1, "offset": -40}
    # The fixed schema keys must never leak into codec_params.
    assert "pairing" not in profile.attributes["temperature"].codec_params
    assert "codec" not in profile.attributes["temperature"].codec_params


def test_parse_profile_extra_command_keys_become_codec_params() -> None:
    raw = _switch_profile()
    raw["commands"]["setpoint"] = {
        "pairing": "AL_SWITCH_ON_OFF",
        "codec": "scaled",
        "factor": 0.1,
        "offset": -40,
    }
    profile = parse_profile(raw, source="<test>")
    assert profile.commands["setpoint"].codec_params == {"factor": 0.1, "offset": -40}


def test_parse_profile_codec_params_is_a_fresh_dict_per_attribute() -> None:
    profile = parse_profile(_dimming_profile(), source="<test>")
    assert (
        profile.attributes["state"].codec_params
        is not profile.attributes["brightness"].codec_params
    )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda raw: raw.pop("id"), "id"),
        (lambda raw: raw.pop("functions"), "functions"),
        (lambda raw: raw.pop("attributes"), "attributes"),
        (lambda raw: raw["attributes"]["state"].pop("codec"), "codec"),
        (lambda raw: raw.__setitem__("id", "Not-A-Slug"), "id"),
        (lambda raw: raw.__setitem__("unexpected_top_level_key", 1), None),
        (lambda raw: raw["attributes"].clear(), "attributes"),
    ],
)
def test_parse_profile_rejects_schema_violations(mutate: Any, match: str | None) -> None:
    raw = _switch_profile()
    mutate(raw)
    with pytest.raises(ProfileError):
        parse_profile(raw, source="<test>")


def test_parse_profile_rejects_unknown_function_name() -> None:
    raw = _switch_profile(functions=["FID_NOT_A_REAL_FUNCTION"])
    with pytest.raises(ProfileError, match="unknown function"):
        parse_profile(raw, source="<test>")


def test_parse_profile_rejects_unknown_pairing_name() -> None:
    raw = _switch_profile()
    raw["attributes"]["state"]["pairing"] = "AL_NOT_A_REAL_PAIRING"
    with pytest.raises(ProfileError, match="unknown pairing"):
        parse_profile(raw, source="<test>")


def test_parse_profile_rejects_unknown_pairing_name_in_requires() -> None:
    raw = _switch_profile(requires={"outputs": ["AL_NOT_A_REAL_PAIRING"]})
    with pytest.raises(ProfileError, match="unknown pairing"):
        parse_profile(raw, source="<test>")


def test_parse_profile_rejects_unknown_parameter_name() -> None:
    raw = _switch_profile(parameters={"foo": "PID_NOT_A_REAL_PARAMETER"})
    with pytest.raises(ProfileError, match="unknown parameter"):
        parse_profile(raw, source="<test>")


def test_parse_profile_rejects_unknown_codec_name() -> None:
    raw = _switch_profile()
    raw["attributes"]["state"]["codec"] = "not_a_real_codec"
    with pytest.raises(ProfileError, match="unknown codec"):
        parse_profile(raw, source="<test>")


def test_parse_profile_error_message_includes_the_source() -> None:
    raw = _switch_profile(functions=["FID_NOT_A_REAL_FUNCTION"])
    with pytest.raises(ProfileError, match=r"lighting\.yaml"):
        parse_profile(raw, source="lighting.yaml")


# ------------------------------------------------------------------------------- load_profile_file


def test_load_profile_file_reads_a_yaml_list(tmp_path: Path) -> None:
    path = tmp_path / "switch.yaml"
    path.write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes:\n"
        "    state: {pairing: AL_INFO_ON_OFF, codec: bool01}\n"
    )
    profiles = load_profile_file(path)
    assert [p.id for p in profiles] == ["switch_actuator"]


def test_load_profile_file_reads_multiple_profiles_in_file_order(tmp_path: Path) -> None:
    path = tmp_path / "many.yaml"
    path.write_text(
        "- id: bbb\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
        "- id: aaa\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    profiles = load_profile_file(path)
    assert [p.id for p in profiles] == ["bbb", "aaa"]


def test_load_profile_file_rejects_non_list_yaml(tmp_path: Path) -> None:
    path = tmp_path / "not_a_list.yaml"
    path.write_text("id: switch_actuator\n")
    with pytest.raises(ProfileError, match="list"):
        load_profile_file(path)


def test_load_profile_file_rejects_empty_yaml(tmp_path: Path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("")
    with pytest.raises(ProfileError, match="list"):
        load_profile_file(path)


def test_load_profile_file_rejects_invalid_yaml_syntax(tmp_path: Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("- id: [this is not valid: yaml\n")
    with pytest.raises(ProfileError, match="YAML"):
        load_profile_file(path)


def test_load_profile_file_error_includes_the_path(tmp_path: Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("id: not-a-list\n")
    with pytest.raises(ProfileError, match=r"broken\.yaml"):
        load_profile_file(path)


# ----------------------------------------------------------------------- load_profile_directory


def test_load_profile_directory_reads_all_yaml_files_sorted_by_filename(tmp_path: Path) -> None:
    (tmp_path / "b_covers.yaml").write_text(
        "- id: bbb\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    (tmp_path / "a_lighting.yaml").write_text(
        "- id: aaa\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    profiles = load_profile_directory(tmp_path)
    assert [p.id for p in profiles] == ["aaa", "bbb"]


def test_load_profile_directory_accepts_yml_extension_too(tmp_path: Path) -> None:
    (tmp_path / "switch.yml").write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    profiles = load_profile_directory(tmp_path)
    assert [p.id for p in profiles] == ["switch_actuator"]


def test_load_profile_directory_ignores_non_yaml_files(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("not a profile")
    (tmp_path / "switch.yaml").write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    profiles = load_profile_directory(tmp_path)
    assert [p.id for p in profiles] == ["switch_actuator"]


def test_load_profile_directory_empty_directory_returns_empty_list(tmp_path: Path) -> None:
    assert load_profile_directory(tmp_path) == []


# ------------------------------------------------------------------------------- build_registry


def test_build_registry_merges_user_overrides_over_built_ins() -> None:
    built_in = [parse_profile(_switch_profile(), source="<built-in>")]
    override = [parse_profile(_switch_profile(description="user override"), source="<user>")]
    registry = build_registry(built_in, override)
    assert registry["switch_actuator"].description == "user override"
    assert len(registry) == 1


def test_build_registry_with_no_overrides_keeps_the_built_ins() -> None:
    built_in = [parse_profile(_switch_profile(), source="<built-in>")]
    registry = build_registry(built_in)
    assert registry["switch_actuator"].description is None


def test_build_registry_rejects_duplicate_id_within_built_ins() -> None:
    built_in = [
        parse_profile(_switch_profile(), source="a.yaml"),
        parse_profile(_switch_profile(), source="b.yaml"),
    ]
    with pytest.raises(ProfileError, match="duplicate"):
        build_registry(built_in)


def test_build_registry_rejects_duplicate_id_within_overrides() -> None:
    built_in = [parse_profile(_switch_profile(), source="<built-in>")]
    overrides = [
        parse_profile(_switch_profile(description="one"), source="a.yaml"),
        parse_profile(_switch_profile(description="two"), source="b.yaml"),
    ]
    with pytest.raises(ProfileError, match="duplicate"):
        build_registry(built_in, overrides)


# ------------------------------------------------------------------------------- ProfileRegistry


def test_profile_registry_for_function_returns_claiming_profiles_in_id_order() -> None:
    switch = parse_profile(_switch_profile(id="zzz_switch"), source="<test>")
    dimming = parse_profile(
        _dimming_profile(id="aaa_dimming", functions=["FID_SWITCH_ACTUATOR"]), source="<test>"
    )
    registry = build_registry([switch, dimming])
    assert [p.id for p in registry.for_function(Function.FID_SWITCH_ACTUATOR)] == [
        "aaa_dimming",
        "zzz_switch",
    ]


def test_profile_registry_for_function_unknown_function_returns_empty_tuple() -> None:
    registry = build_registry([parse_profile(_switch_profile(), source="<test>")])
    assert registry.for_function(Function.FID_DIMMING_ACTUATOR) == ()


def test_profile_registry_getitem_len_and_iter() -> None:
    profile = parse_profile(_switch_profile(), source="<test>")
    registry = build_registry([profile])
    assert registry["switch_actuator"] is profile
    assert len(registry) == 1
    assert list(registry) == [profile]


def test_profile_registry_getitem_missing_id_raises_key_error() -> None:
    registry = build_registry([parse_profile(_switch_profile(), source="<test>")])
    with pytest.raises(KeyError):
        registry["does_not_exist"]


def test_profile_registry_is_iterable_type() -> None:
    assert isinstance(build_registry([]), ProfileRegistry)


# --------------------------------------------------------------------------- load_profile_registry


def test_load_profile_registry_end_to_end(tmp_path: Path) -> None:
    built_in_dir = tmp_path / "built_in"
    built_in_dir.mkdir()
    (built_in_dir / "switch.yaml").write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    user_dir = tmp_path / "user"
    user_dir.mkdir()
    (user_dir / "override.yaml").write_text(
        "- id: switch_actuator\n"
        "  description: user override\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    registry = load_profile_registry(built_in_dir, user_dir)
    assert registry["switch_actuator"].description == "user override"


def test_load_profile_registry_user_dir_missing_is_treated_as_no_overrides(tmp_path: Path) -> None:
    built_in_dir = tmp_path / "built_in"
    built_in_dir.mkdir()
    (built_in_dir / "switch.yaml").write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    registry = load_profile_registry(built_in_dir, tmp_path / "does_not_exist")
    assert registry["switch_actuator"].description is None


def test_load_profile_registry_user_dir_none_is_treated_as_no_overrides(tmp_path: Path) -> None:
    built_in_dir = tmp_path / "built_in"
    built_in_dir.mkdir()
    (built_in_dir / "switch.yaml").write_text(
        "- id: switch_actuator\n"
        "  functions: [FID_SWITCH_ACTUATOR]\n"
        "  attributes: {state: {pairing: AL_INFO_ON_OFF, codec: bool01}}\n"
    )
    registry = load_profile_registry(built_in_dir)
    assert registry["switch_actuator"].description is None


def test_load_profile_registry_built_in_dir_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(ProfileError, match="built-in"):
        load_profile_registry(tmp_path / "does_not_exist")


def test_load_profile_registry_real_schema_file_is_used(tmp_path: Path) -> None:
    # Regression: parse_profile/load_profile_registry must validate against the actual shipped
    # profiles/_schema.json, not a hand-rolled schema that happens to agree with the tests above.
    built_in_dir = tmp_path / "built_in"
    built_in_dir.mkdir()
    (built_in_dir / "bad.yaml").write_text(
        "- id: 'Not A Valid Slug'\n  functions: []\n  attributes: {}\n"
    )
    with pytest.raises(ProfileError):
        load_profile_registry(built_in_dir)
