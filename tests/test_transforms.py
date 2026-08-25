"""Tests for model/transforms.py: the named transform escape hatch (docs/03 §7; docs/11 WP4)."""

from __future__ import annotations

import pytest

from freeathome2mqtt.model.codecs import CommandError
from freeathome2mqtt.model.transforms import (
    Transform,
    get_transform,
    transform,
)

# ------------------------------------------------------------------------------- registry


def test_get_transform_returns_a_transform_instance() -> None:
    assert isinstance(get_transform("room_temperature_controller"), Transform)
    assert isinstance(get_transform("cover_with_slats"), Transform)


def test_get_transform_unknown_name_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_transform("not_a_real_transform")


def test_get_transform_returns_a_fresh_instance_each_time() -> None:
    # Stateless -- but not a shared singleton either (CLAUDE.md rule 6).
    first = get_transform("cover_with_slats")
    second = get_transform("cover_with_slats")
    assert first is not second


def test_transform_decorator_rejects_a_duplicate_name() -> None:
    @transform("_test_dummy_transform")
    class _Dummy(Transform):
        def derive(self, values: object) -> dict[str, object]:
            return {}

        def command(self, name: str, value: object, values: object) -> list[tuple[str, object]]:
            return []

    with pytest.raises(ValueError, match="duplicate"):

        @transform("_test_dummy_transform")
        class _AlsoDummy(Transform):
            def derive(self, values: object) -> dict[str, object]:
                return {}

            def command(self, name: str, value: object, values: object) -> list[tuple[str, object]]:
                return []


# --------------------------------------------------------------------- room_temperature_controller


def test_room_temperature_controller_derive_off_when_on_off_is_false() -> None:
    rtc = get_transform("room_temperature_controller")
    assert rtc.derive([False, False, "heating"]) == {"hvac_mode": "off"}


def test_room_temperature_controller_derive_off_ignores_eco_and_mode() -> None:
    rtc = get_transform("room_temperature_controller")
    assert rtc.derive([False, True, "cooling"]) == {"hvac_mode": "off"}


def test_room_temperature_controller_derive_eco_when_on_and_eco() -> None:
    rtc = get_transform("room_temperature_controller")
    assert rtc.derive([True, True, "heating"]) == {"hvac_mode": "eco"}


def test_room_temperature_controller_derive_reports_the_raw_mode_when_on_and_not_eco() -> None:
    rtc = get_transform("room_temperature_controller")
    assert rtc.derive([True, False, "heating"]) == {"hvac_mode": "heating"}
    assert rtc.derive([True, False, "cooling"]) == {"hvac_mode": "cooling"}


def test_room_temperature_controller_command_off_writes_only_on_off() -> None:
    rtc = get_transform("room_temperature_controller")
    writes = rtc.command("hvac_mode", "off", [True, False, "heating"])
    assert writes == [("on_off", False)]


def test_room_temperature_controller_command_eco_writes_on_off_and_eco() -> None:
    rtc = get_transform("room_temperature_controller")
    writes = rtc.command("hvac_mode", "eco", [False, False, "heating"])
    assert writes == [("on_off", True), ("eco", True)]


def test_room_temperature_controller_command_heating_writes_three_datapoints() -> None:
    rtc = get_transform("room_temperature_controller")
    writes = rtc.command("hvac_mode", "heating", [False, False, "cooling"])
    assert writes == [("on_off", True), ("eco", False), ("mode", "heating")]


def test_room_temperature_controller_command_cooling_writes_three_datapoints() -> None:
    rtc = get_transform("room_temperature_controller")
    writes = rtc.command("hvac_mode", "cooling", [False, False, "heating"])
    assert writes == [("on_off", True), ("eco", False), ("mode", "cooling")]


def test_room_temperature_controller_command_unknown_value_raises() -> None:
    rtc = get_transform("room_temperature_controller")
    with pytest.raises(CommandError):
        rtc.command("hvac_mode", "auto", [True, False, "heating"])


def test_room_temperature_controller_command_unknown_name_raises() -> None:
    rtc = get_transform("room_temperature_controller")
    with pytest.raises(CommandError):
        rtc.command("not_a_real_command", "off", [True, False, "heating"])


# ------------------------------------------------------------------------------- cover_with_slats


def test_cover_with_slats_derive_reports_slat_position_when_not_fully_open() -> None:
    cover = get_transform("cover_with_slats")
    assert cover.derive([50, 30]) == {"slat_position": 30}


def test_cover_with_slats_derive_reports_none_slat_position_when_fully_open() -> None:
    # docs/03 §7: position and slat angle interact -- a fully open cover has nothing to tilt.
    cover = get_transform("cover_with_slats")
    assert cover.derive([100, 30]) == {"slat_position": None}


def test_cover_with_slats_derive_reports_slat_position_when_fully_closed() -> None:
    cover = get_transform("cover_with_slats")
    assert cover.derive([0, 0]) == {"slat_position": 0}


def test_cover_with_slats_command_stop() -> None:
    cover = get_transform("cover_with_slats")
    assert cover.command("stop", True, [50, 30]) == [("stop", True)]


def test_cover_with_slats_command_position_writes_just_position_when_not_fully_open() -> None:
    cover = get_transform("cover_with_slats")
    assert cover.command("position", 50, [30, 20]) == [("position", 50)]


def test_cover_with_slats_command_position_also_resets_slats_when_fully_open() -> None:
    cover = get_transform("cover_with_slats")
    assert cover.command("position", 100, [30, 20]) == [("position", 100), ("slat_position", 0)]


def test_cover_with_slats_command_unknown_name_raises() -> None:
    cover = get_transform("cover_with_slats")
    with pytest.raises(CommandError):
        cover.command("not_a_real_command", True, [50, 30])
