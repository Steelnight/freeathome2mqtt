"""Tests for addon.py: the Home Assistant add-on's options -> config.yaml translation
(docs/07 §5.3; docs/11 WP12's optional deliverable; docs/12 WP18).

The add-on itself cannot be exercised here -- it needs a Home Assistant Supervisor to install it,
and this environment has none. What *can* be tested is the part where the bugs live: turning the
add-on's `options.json` and the Supervisor's MQTT service credentials into a `config.yaml` this
bridge accepts. Keeping that in Python rather than in a shell script is the whole reason it is
testable at all, and is why `run.sh` is three lines.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from freeathome2mqtt.addon import (
    AddonError,
    build_bridge_config,
    main,
    write_bridge_config,
)
from freeathome2mqtt.settings import load_settings

_OPTIONS = {
    "sysap_host": "https://192.168.1.50",
    "sysap_username": "installer",
    "sysap_password": "secret",
}
_MQTT_SERVICE = {
    "host": "core-mosquitto",
    "port": 1883,
    "username": "addon_user",
    "password": "addon_pass",
    "ssl": False,
}


def test_mqtt_credentials_come_from_the_supervisor_service() -> None:
    """docs/07 §5.3's central promise: "the MQTT service discovery API supplies broker
    credentials automatically", so the user never types them.
    """
    config = build_bridge_config(_OPTIONS, mqtt_service=_MQTT_SERVICE)

    assert config["mqtt"]["server"] == "mqtt://core-mosquitto:1883"
    assert config["mqtt"]["user"] == "addon_user"
    assert config["mqtt"]["password"] == "addon_pass"


def test_an_ssl_mqtt_service_produces_an_mqtts_url() -> None:
    service = {**_MQTT_SERVICE, "ssl": True, "port": 8883}
    config = build_bridge_config(_OPTIONS, mqtt_service=service)
    assert config["mqtt"]["server"] == "mqtts://core-mosquitto:8883"


def test_the_data_dir_is_the_addons_persistent_volume() -> None:
    """docs/07 §5.3: "/data is the add-on's persistent volume"."""
    config = build_bridge_config(_OPTIONS, mqtt_service=_MQTT_SERVICE)
    assert config["advanced"]["data_dir"] == "/data"


def test_optional_options_are_omitted_rather_than_written_as_null() -> None:
    """An absent add-on option must not become an explicit `null` in config.yaml: the schema's
    own defaults are better than a null that overrides them.
    """
    config = build_bridge_config(_OPTIONS, mqtt_service=_MQTT_SERVICE)

    assert "base_topic" not in config["mqtt"]
    assert "verify_ssl" not in config["sysap"]


def test_provided_optional_options_are_passed_through() -> None:
    options = {
        **_OPTIONS,
        "sysap_verify_ssl": False,
        "mqtt_base_topic": "fh2m",
        "log_level": "debug",
        "homeassistant_enabled": False,
    }
    config = build_bridge_config(options, mqtt_service=_MQTT_SERVICE)

    assert config["sysap"]["verify_ssl"] is False
    assert config["mqtt"]["base_topic"] == "fh2m"
    assert config["advanced"]["log_level"] == "debug"
    assert config["homeassistant"]["enabled"] is False


@pytest.mark.parametrize("missing", ["sysap_host", "sysap_username", "sysap_password"])
def test_a_missing_required_option_fails_with_the_option_name(missing: str) -> None:
    """The add-on's own schema should catch this first; if it does not, the message still has to
    name the option the user has to fix, not fail somewhere deeper.
    """
    options = {k: v for k, v in _OPTIONS.items() if k != missing}

    with pytest.raises(AddonError, match=missing):
        build_bridge_config(options, mqtt_service=_MQTT_SERVICE)


def test_a_missing_mqtt_service_fails_with_an_actionable_message() -> None:
    """The add-on declares `services: mqtt:need`, so this means the Mosquitto add-on is not
    running -- which the user can fix, if told.
    """
    with pytest.raises(AddonError, match="MQTT"):
        build_bridge_config(_OPTIONS, mqtt_service=None)


def test_the_generated_config_is_accepted_by_the_real_settings_loader(tmp_path: Path) -> None:
    """The assertion that makes all the others worth having: whatever this produces must load
    cleanly through the same `load_settings` the bridge itself uses, with no special casing.
    """
    options = {**_OPTIONS, "mqtt_base_topic": "fh2m", "log_level": "debug"}
    path = tmp_path / "config.yaml"

    write_bridge_config(options, mqtt_service=_MQTT_SERVICE, path=path)

    settings = load_settings(path, environ={})
    assert settings.sysap.host == "https://192.168.1.50"
    assert settings.mqtt.base_topic == "fh2m"
    assert settings.advanced.log_level == "debug"


def test_the_generated_file_is_yaml_a_human_can_read(tmp_path: Path) -> None:
    """Users do look at it when something is wrong; a JSON blob on one line would be hostile."""
    path = tmp_path / "config.yaml"
    write_bridge_config(_OPTIONS, mqtt_service=_MQTT_SERVICE, path=path)

    text = path.read_text()
    assert "sysap:" in text
    assert yaml.safe_load(text)["sysap"]["username"] == "installer"


def test_options_are_read_from_the_supervisor_paths(tmp_path: Path) -> None:
    """`/data/options.json` and `/run/s6/container_environment` style inputs are read as files, so
    `run.sh` stays a thin wrapper with no parsing of its own.
    """
    options_path = tmp_path / "options.json"
    options_path.write_text(json.dumps(_OPTIONS))
    service_path = tmp_path / "mqtt.json"
    service_path.write_text(json.dumps(_MQTT_SERVICE))
    out_path = tmp_path / "config.yaml"

    exit_code = main(
        [
            "--options",
            str(options_path),
            "--mqtt-service",
            str(service_path),
            "--out",
            str(out_path),
        ]
    )

    assert exit_code == 0
    assert load_settings(out_path, environ={}).mqtt.user == "addon_user"


def test_main_reports_a_missing_options_file_rather_than_raising(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = main(
        ["--options", str(tmp_path / "absent.json"), "--out", str(tmp_path / "c.yaml")]
    )

    assert exit_code == 1
    assert "absent.json" in capsys.readouterr().err
