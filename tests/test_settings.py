"""Tests for settings.py: pydantic config model, env overrides, !env/!secret/!file, semantic
validation, and the Settings -> SupervisorConfig translation (docs/07; docs/11 WP9).
"""

from __future__ import annotations

import inspect
import re
import ssl
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

import freeathome2mqtt.cli
import freeathome2mqtt.settings
from freeathome2mqtt.settings import (
    Settings,
    SettingsError,
    load_settings,
    settings_to_supervisor_config,
)

MINIMAL_CONFIG = """
sysap:
  host: https://192.168.1.50
  username: installer
  password: secret

mqtt:
  server: mqtt://192.168.1.10:1883
"""


def _write(tmp_path: Path, text: str, *, name: str = "config.yaml") -> Path:
    path = tmp_path / name
    path.write_text(text)
    return path


# A throwaway, real self-signed CA cert -- not a secret, just PEM content for the ssl module
# (shared in spirit with test_rest.py's identical fixture; duplicated per CLAUDE.md's "three
# similar lines beats a premature abstraction" -- this is one whole PEM block, not three lines,
# but it's still simplest kept local to each test module rather than factored into a shared
# fixtures module for a single reused constant).
_TEST_CA_PEM = """-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUSxtfQifj6O0QH+FnXi7ZoYk1g6UwDQYJKoZIhvcNAQEL
BQAwIjEgMB4GA1UEAwwXZnJlZWF0aG9tZTJtcXR0LXRlc3QtY2EwHhcNMjYwODIz
MjA1NjQwWhcNMzYwODIwMjA1NjQwWjAiMSAwHgYDVQQDDBdmcmVlYXRob21lMm1x
dHQtdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKkW9Ic0
ekbdSwJrTll6NyfxnjGGl/DiHhavIaqSRSa8ksj4SMB8c52Z61yckDPVzJWgEgT3
Uv5+Um29hua4v3YuOO1N8voGA4AtSeVSRQNi5ltLDDklp6+fTvARGCc+eg/TX8JK
JgSv7c03bkrnqWgjLkOJ0wdu8ERFgvYEvBKrTzmAkhC4DNbTzt2YVuBBdhCDP8ue
g1HVMrCTU4excqcQotouvSKsqaN4zOhsQ8xX/qy6apXqKD0gfX6Fo1TIifdbpM9z
CGEMan5e9YlR06TLO4r+qEdAvEfW21uAzMVO3f5LxrGH+o81PjrDV48Q0X4sHyog
+THxURP0aV3j4IMCAwEAAaNTMFEwHQYDVR0OBBYEFI1pQVZakD38JljCIhhLOJfI
OsxTMB8GA1UdIwQYMBaAFI1pQVZakD38JljCIhhLOJfIOsxTMA8GA1UdEwEB/wQF
MAMBAf8wDQYJKoZIhvcNAQELBQADggEBAKROcIXMqnGlWCGmHzaIHuxoR8jtuMCx
58WNH4IQt9p8lqgdHzZXt8e33do+v9VkIClPof60yVxqXo2K1DY/ZpZ288SmwSNf
o2XXimM6X+kSrJtVEfoHTX7A5GrKqVoa+uiN4SGYIyXgFEvCfGZIAsD2QOan3NnI
rX54+Ss0Cm6Qq4S5xmZ+7TocwQgZgjs9Awsj7Gi9lfiDwcPLBf81o99hlb07gr6s
taEUX3zOBTydv7ZlxX+vMziBd3bKNDnfXyYng0losfGxsW0bRZ10e+g+UoWNaLel
6wFlSoTnvo6DsnmNSyJnvnAMZdknEMfqaBpB8W4yap1Bn5spDfyn6oY=
-----END CERTIFICATE-----
"""


# ---------------------------------------------------------------------------------- basic loading


def test_minimal_config_loads_with_sane_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    assert settings.sysap.host == "https://192.168.1.50"
    assert settings.sysap.username == "installer"
    assert settings.sysap.password == "secret"
    assert settings.mqtt.server == "mqtt://192.168.1.10:1883"
    assert settings.mqtt.base_topic == "freeathome2mqtt"
    assert settings.entities.topic_style == "friendly"
    assert settings.advanced.data_dir == Path("/data")


def test_missing_file_raises_settings_error(tmp_path: Path) -> None:
    with pytest.raises(SettingsError, match="No such file"):
        load_settings(tmp_path / "does-not-exist.yaml", environ={})


def test_invalid_yaml_raises_settings_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "sysap: [unclosed")
    with pytest.raises(SettingsError, match="invalid YAML"):
        load_settings(path, environ={})


def test_non_mapping_top_level_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "- just\n- a\n- list\n")
    with pytest.raises(SettingsError, match="must be a mapping"):
        load_settings(path, environ={})


def test_missing_required_field_raises_a_precise_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path, "sysap:\n  username: installer\n  password: secret\nmqtt:\n  server: mqtt://h:1\n"
    )
    with pytest.raises(SettingsError, match=r"sysap\.host"):
        load_settings(path, environ={})


def test_unknown_top_level_key_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG + "\nbogus_section:\n  x: 1\n")
    with pytest.raises(SettingsError, match="bogus_section"):
        load_settings(path, environ={})


def test_unknown_key_within_a_section_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        MINIMAL_CONFIG + "\nsysap:\n  host: https://h\n  username: u\n  password: p\n  bogus: 1\n",
    )
    with pytest.raises(SettingsError):
        load_settings(path, environ={})


# ------------------------------------------------------------------------------------ env overrides


def test_env_override_wins_over_the_file_value(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(
        path, environ={"FAH2MQTT_SYSAP__PASSWORD": "from-env", "FAH2MQTT_MQTT__KEEPALIVE": "30"}
    )
    assert settings.sysap.password == "from-env"
    assert settings.mqtt.keepalive == 30


def test_env_override_ignores_unrelated_variables(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={"PATH": "/usr/bin", "HOME": "/root"})
    assert settings.sysap.password == "secret"


def test_env_override_into_a_nested_section_that_conflicts_with_a_scalar_raises(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    with pytest.raises(SettingsError, match=r"sysap\.host\.deeper"):
        load_settings(path, environ={"FAH2MQTT_SYSAP__HOST__DEEPER": "x"})


# ------------------------------------------------------------------------ !env/!secret/!file


def test_env_tag_resolves_from_the_environment(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG.replace("password: secret", "password: !env FAH_PASSWORD")
    path = _write(tmp_path, config)
    settings = load_settings(path, environ={"FAH_PASSWORD": "tag-resolved"})
    assert settings.sysap.password == "tag-resolved"


def test_env_tag_raises_when_the_variable_is_unset(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG.replace("password: secret", "password: !env FAH_PASSWORD")
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="!env FAH_PASSWORD is not set"):
        load_settings(path, environ={})


def test_secret_tag_resolves_from_secrets_yaml(tmp_path: Path) -> None:
    (tmp_path / "secrets.yaml").write_text(yaml.dump({"sysap_password": "from-secrets"}))
    config = MINIMAL_CONFIG.replace("password: secret", "password: !secret sysap_password")
    path = _write(tmp_path, config)
    settings = load_settings(path, data_dir=tmp_path, environ={})
    assert settings.sysap.password == "from-secrets"


def test_secret_tag_raises_when_secrets_file_is_missing(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG.replace("password: secret", "password: !secret sysap_password")
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match=r"secrets\.yaml"):
        load_settings(path, data_dir=tmp_path, environ={})


def test_secret_tag_raises_when_key_is_missing(tmp_path: Path) -> None:
    (tmp_path / "secrets.yaml").write_text(yaml.dump({"other_key": "x"}))
    config = MINIMAL_CONFIG.replace("password: secret", "password: !secret sysap_password")
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="sysap_password"):
        load_settings(path, data_dir=tmp_path, environ={})


def test_file_tag_reads_and_strips_trailing_whitespace(tmp_path: Path) -> None:
    secret_file = tmp_path / "mqtt_password.txt"
    secret_file.write_text("file-secret\n")
    config = MINIMAL_CONFIG.replace(
        "server: mqtt://192.168.1.10:1883",
        f"server: mqtt://192.168.1.10:1883\n  password: !file {secret_file}",
    )
    path = _write(tmp_path, config)
    settings = load_settings(path, environ={})
    assert settings.mqtt.password == "file-secret"


def test_file_tag_raises_for_a_missing_file(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG.replace(
        "server: mqtt://192.168.1.10:1883",
        "server: mqtt://192.168.1.10:1883\n  password: !file /no/such/file",
    )
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="!file"):
        load_settings(path, environ={})


# --------------------------------------------------------------------------- semantic validation


def test_discovery_topic_equal_to_base_topic_is_rejected(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG + "\nhomeassistant:\n  discovery_topic: freeathome2mqtt\n"
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="discovery_topic"):
        load_settings(path, environ={})


@pytest.mark.parametrize(
    "bad_topic", ["freeathome2mqtt/", "/freeathome2mqtt", "fah2#mqtt", "fah2+mqtt"]
)
def test_base_topic_with_wildcards_or_leading_trailing_slash_is_rejected(
    tmp_path: Path, bad_topic: str
) -> None:
    config = MINIMAL_CONFIG + f"\nmqtt:\n  server: mqtt://h:1\n  base_topic: {bad_topic!r}\n"
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="base_topic"):
        load_settings(path, environ={})


def test_bare_host_is_normalised_to_https_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = MINIMAL_CONFIG.replace("host: https://192.168.1.50", "host: 192.168.1.50")
    path = _write(tmp_path, config)
    with caplog.at_level("WARNING"):
        settings = load_settings(path, environ={})
    assert settings.sysap.host == "https://192.168.1.50"
    assert any("no scheme" in record.getMessage() for record in caplog.records)


def test_completely_unparseable_host_is_rejected(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG.replace("host: https://192.168.1.50", "host: ''")
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match=r"sysap\.host"):
        load_settings(path, environ={})


def test_unknown_interface_is_rejected(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG + "\nentities:\n  interfaces: [TP, bogus_interface]\n"
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="interfaces"):
        load_settings(path, environ={})


@pytest.mark.parametrize("value", [0, 17])
def test_max_inflight_out_of_range_is_rejected(tmp_path: Path, value: int) -> None:
    config = (
        MINIMAL_CONFIG
        + f"\nsysap:\n  host: https://h\n  username: u\n  password: p\n  max_inflight: {value}\n"
    )
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="max_inflight"):
        load_settings(path, environ={})


@pytest.mark.parametrize("value", [-1, 1001])
def test_coalesce_ms_out_of_range_is_rejected(tmp_path: Path, value: int) -> None:
    config = MINIMAL_CONFIG + f"\nperformance:\n  coalesce_ms: {value}\n"
    path = _write(tmp_path, config)
    with pytest.raises(SettingsError, match="coalesce_ms"):
        load_settings(path, environ={})


def test_valid_glob_patterns_are_accepted(tmp_path: Path) -> None:
    config = MINIMAL_CONFIG + "\nentities:\n  exclude: ['*_backup', 'ABB*_ch0001']\n"
    path = _write(tmp_path, config)
    settings = load_settings(path, environ={})
    assert settings.entities.exclude == ["*_backup", "ABB*_ch0001"]


# --------------------------------------------------------------------- Settings -> SupervisorConfig


async def test_translation_maps_basic_fields(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.sysap_base_url == "https://192.168.1.50"
    assert config.sysap_username == "installer"
    assert config.sysap_password == "secret"
    assert config.mqtt_host == "192.168.1.10"
    assert config.mqtt_port == 1883
    assert config.base_topic == "freeathome2mqtt"
    assert isinstance(config.sysap_ssl, ssl.SSLContext)  # default: verify, no explicit CA file


async def test_translation_disables_ssl_verification_when_configured(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG
        + "\nsysap:\n  host: https://h\n  username: u\n  password: p\n  verify_ssl: false\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.sysap_ssl is False


async def test_translation_uses_ca_file_when_configured(tmp_path: Path) -> None:
    ca_path = tmp_path / "ca.pem"
    ca_path.write_text(_TEST_CA_PEM)
    settings = Settings.model_validate(
        {
            "sysap": {
                "host": "https://h",
                "username": "u",
                "password": "p",
                "ssl_ca_file": str(ca_path),
            },
            "mqtt": {"server": "mqtt://h:1883"},
        }
    )
    config = await settings_to_supervisor_config(settings)
    assert isinstance(config.sysap_ssl, ssl.SSLContext)


async def test_translation_debounce_and_reconcile_ms_to_seconds(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG + "\nperformance:\n  command_debounce_ms: 250\n  reconcile_after_ms: 1500\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.command_debounce_s == pytest.approx(0.25)
    assert config.reconcile_delay_s == pytest.approx(1.5)


async def test_translation_topic_style_friendly_maps_to_slug(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.compile_options.topic_style == "slug"


async def test_translation_topic_style_id_maps_to_id(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG + "\nentities:\n  topic_style: id\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.compile_options.topic_style == "id"


async def test_translation_default_interfaces_matches_compiler_default_exclusions(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.compile_options.excluded_interfaces == frozenset({"hue", "sonos"})


async def test_translation_custom_interfaces_list_changes_exclusions(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG + "\nentities:\n  interfaces: [TP, RF, hue, sonos, smokealarm]\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.compile_options.excluded_interfaces == frozenset()


async def test_translation_mqtts_scheme_defaults_to_8883(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG.replace("server: mqtt://192.168.1.10:1883", "server: mqtts://h")
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.mqtt_port == 8883


async def test_translation_mqtt_ca_produces_an_ssl_context(tmp_path: Path) -> None:
    ca_path = tmp_path / "mqtt-ca.pem"
    ca_path.write_text(_TEST_CA_PEM)
    config_yaml = MINIMAL_CONFIG + f"\nmqtt:\n  server: mqtt://h:1883\n  ca: {ca_path}\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert isinstance(config.mqtt_tls, ssl.SSLContext)


async def test_translation_mqtt_reject_unauthorized_false_disables_verification(
    tmp_path: Path,
) -> None:
    ca_path = tmp_path / "mqtt-ca.pem"
    ca_path.write_text(_TEST_CA_PEM)
    config_yaml = (
        MINIMAL_CONFIG
        + f"\nmqtt:\n  server: mqtt://h:1883\n  ca: {ca_path}\n  reject_unauthorized: false\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert isinstance(config.mqtt_tls, ssl.SSLContext)
    assert config.mqtt_tls.verify_mode == ssl.CERT_NONE
    assert config.mqtt_tls.check_hostname is False


async def test_translation_mqtt_reject_unauthorized_true_keeps_verification(tmp_path: Path) -> None:
    ca_path = tmp_path / "mqtt-ca.pem"
    ca_path.write_text(_TEST_CA_PEM)
    config_yaml = MINIMAL_CONFIG + f"\nmqtt:\n  server: mqtt://h:1883\n  ca: {ca_path}\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert isinstance(config.mqtt_tls, ssl.SSLContext)
    assert config.mqtt_tls.verify_mode == ssl.CERT_REQUIRED


async def test_translation_maps_homeassistant_and_mqtt_packet_size_fields(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG + "\nhomeassistant:\n"
        "  enabled: false\n"
        "  discovery_topic: hass\n"
        "  status_topic: hass/status\n"
        "  republish_delay: 7\n"
        "\nmqtt:\n  server: mqtt://192.168.1.10:1883\n  maximum_packet_size: 2048\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.homeassistant_enabled is False
    assert config.homeassistant_discovery_topic == "hass"
    assert config.homeassistant_status_topic == "hass/status"
    assert config.homeassistant_republish_delay_s == pytest.approx(7.0)
    assert config.mqtt_maximum_packet_size == 2048


async def test_translation_homeassistant_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.homeassistant_enabled is True
    assert config.homeassistant_discovery_topic == "homeassistant"
    assert config.homeassistant_status_topic == "homeassistant/status"
    assert config.homeassistant_republish_delay_s == pytest.approx(5.0)
    assert config.mqtt_maximum_packet_size == 1048576


async def test_translation_maps_mqtt_client_id(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG + "\nmqtt:\n  server: mqtt://192.168.1.10:1883\n  client_id: fixed\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.mqtt_client_id == "fixed"


async def test_translation_mqtt_client_id_defaults_to_none(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.mqtt_client_id is None


async def test_translation_maps_mqtt_qos_and_retain_fields(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG + "\nmqtt:\n  server: mqtt://192.168.1.10:1883\n"
        "  qos_state: 1\n  qos_discovery: 0\n  force_disable_retain: true\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.mqtt_qos_state == 1
    assert config.mqtt_qos_discovery == 0
    assert config.mqtt_force_disable_retain is True


async def test_translation_maps_sysap_request_timeout(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG.replace(
        "password: secret\n", "password: secret\n  request_timeout: 25\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.sysap_request_timeout_s == pytest.approx(25.0)


async def test_translation_maps_performance_optimistic_default(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG + "\nperformance:\n  optimistic: false\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.default_optimistic is False


async def test_translation_maps_availability_enabled_and_per_device(tmp_path: Path) -> None:
    config_yaml = MINIMAL_CONFIG + "\navailability:\n  enabled: false\n  per_device: false\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.availability_enabled is False
    assert config.availability_per_device is False


@pytest.mark.parametrize(
    ("raw_mode_yaml", "expected"),
    [("false", False), ("true", True), ("unsupported_only", "unsupported_only")],
)
async def test_translation_maps_advanced_raw_mode(
    tmp_path: Path, raw_mode_yaml: str, expected: object
) -> None:
    config_yaml = MINIMAL_CONFIG + f"\nadvanced:\n  raw_mode: {raw_mode_yaml}\n"
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.raw_mode == expected


async def test_translation_raw_mode_defaults_to_false(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    settings = load_settings(path, environ={})
    config = await settings_to_supervisor_config(settings)
    assert config.raw_mode is False


async def test_invalid_mqtt_server_url_is_rejected(tmp_path: Path) -> None:
    # The scheme/hostname check only runs in the translator -- Settings itself accepts any
    # string for mqtt.server, since it's `settings_to_supervisor_config` that needs the URL
    # parsed, not `Settings.model_validate` (docs/07 §2.2 doesn't list mqtt.server as a
    # semantic-validation row the way sysap.host is).
    config_yaml = MINIMAL_CONFIG.replace(
        "server: mqtt://192.168.1.10:1883", "server: not-a-valid-server"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    with pytest.raises(SettingsError, match=r"mqtt\.server"):
        await settings_to_supervisor_config(settings)


def test_valid_max_inflight_base_topic_and_coalesce_ms_are_accepted(tmp_path: Path) -> None:
    config_yaml = (
        MINIMAL_CONFIG
        + "\nsysap:\n  host: https://h\n  username: u\n  password: p\n  max_inflight: 8\n"
        + "\nmqtt:\n  server: mqtt://h:1\n  base_topic: my_custom_topic\n"
        + "\nperformance:\n  coalesce_ms: 100\n"
    )
    path = _write(tmp_path, config_yaml)
    settings = load_settings(path, environ={})
    assert settings.sysap.max_inflight == 8
    assert settings.mqtt.base_topic == "my_custom_topic"
    assert settings.performance.coalesce_ms == 100


# ---------------------------------------------------- WP16: knobs that used to be accepted-inert


# A throwaway self-signed client certificate + key, generated for this test alone. Not a secret:
# it exists so `ssl.SSLContext.load_cert_chain` has something real to load, which is the only way
# to prove `mqtt.cert`/`mqtt.key` actually reach a context rather than being accepted and dropped.
_TEST_CLIENT_CERT_PEM = """-----BEGIN CERTIFICATE-----
MIIDLTCCAhWgAwIBAgIUFTdE65Lw6agdKNEVoxTylquZlsQwDQYJKoZIhvcNAQEL
BQAwJjEkMCIGA1UEAwwbZnJlZWF0aG9tZTJtcXR0LXRlc3QtY2xpZW50MB4XDTI2
MDkwNTA3NDEyM1oXDTM2MDkwMjA3NDEyM1owJjEkMCIGA1UEAwwbZnJlZWF0aG9t
ZTJtcXR0LXRlc3QtY2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAnprrg5fpz7l1hRmjxN1VnI73M4zyIY8evXVPwLfpLKgb6fW8a2ikbxYwpm1w
KIGfxOVyBRzfS69A4cyc6Uva0pT83ld+YrSjS1wXLFJQBjbOBHskK3b27DdXlKJr
AkbKfj5jgq2I+3mq8UGn4KIc3GuqDy6CNXDLdAcVkvGNssq51Ql6fb1cA/NrNNzd
ABs/wuqI0agqm7Cz5dsREbBKrauJqPUfbGus+dvEvzyl7dWzj73K+J/AHDQ+sp5y
YAykxFfWSgMYbvVxGtWCF+9Dr+kCDO7eHQoT8bBuWweoINcR7/xzIpJ0Ya1mo+Ux
T7eOgoNl99IwLAtdJBSsVioWJwIDAQABo1MwUTAdBgNVHQ4EFgQUt7v+huSBrVn1
fscDBQr8IhOqUtEwHwYDVR0jBBgwFoAUt7v+huSBrVn1fscDBQr8IhOqUtEwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAAD0Z+Fwm4yUAkweWj7nv
oW1gPAGlAgUtrxrKfCwP6EsV/7grJrvb463molfCbhUztVychr514F3hPFfGfUca
x7yBAn7KgSPZlQ6Za4/yK4j8A/5B/OXbpCwDo/ooEd11MPml5Tl3Sy/2JFk42/5x
+EvzNcYUILyD3LRddR1Hd+I+X1ALGs4GZ5ajRGSehFe9fi+K/ATDwTSOy+NHJgcN
/evFEF2jYMo0ZbgjhjineQ9Q/0QOUpvTFecIvZxWBlu10PQFXhVZpVp8w9MDNcGM
0Am2YBWQ503cIkm9hS4Md7eGs/np2pdqLHEFX9Qgekats7YY8yGx4tIZREG7Lsvj
/A==
-----END CERTIFICATE-----
"""

_TEST_CLIENT_KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCemuuDl+nPuXWF
GaPE3VWcjvczjPIhjx69dU/At+ksqBvp9bxraKRvFjCmbXAogZ/E5XIFHN9Lr0Dh
zJzpS9rSlPzeV35itKNLXBcsUlAGNs4EeyQrdvbsN1eUomsCRsp+PmOCrYj7earx
Qafgohzca6oPLoI1cMt0BxWS8Y2yyrnVCXp9vVwD82s03N0AGz/C6ojRqCqbsLPl
2xERsEqtq4mo9R9sa6z528S/PKXt1bOPvcr4n8AcND6ynnJgDKTEV9ZKAxhu9XEa
1YIX70Ov6QIM7t4dChPxsG5bB6gg1xHv/HMiknRhrWaj5TFPt46Cg2X30jAsC10k
FKxWKhYnAgMBAAECggEARATYgAUEZut4dO58B4+MianeZiNKLA4+9q162IGG5Hns
DrguaDnmWyW/E4DXmueMZ1qnPX4WXVT5lFy15m97ltgCYjBPqXEins0NuxpJ64BR
diaBaoqIduF9E+db0QwfB2kmMALQPlj9cJ3hqzqa+gTKV9xwRw7qkgX/wJcLV+Nf
wtfWxmmqMAoUrnavHkpfrUWhXCohRZdcp7EEOQoBOucNG6ukEobpF1cGccLx49AF
GvcKlKYwjZbyT9vSMJpcSndatlFcktIYCrTQqbmFSCVhLfJB47nEQ+U6g1QOswg2
uJ5aWQPVt0JEkEOIootERj4NABsef1XzI77gb/zwcQKBgQDbEzQiWFPI2OhFh86y
4RFIFgL8yYQ32nI5VoMl9Zwuq1Ct7nn9dbRgMK68zW8W4Bf5O4ymR9zV59PdTJ6U
Ch1ir/ntgj0Dh2CsF9p99PA6q+Wf4/vqD0MKJ05JjPUaKcpbj5TcRuxZ7YFYuMZX
Gjgtuyp58reUaP+ZDm/T2E29IwKBgQC5VoWq8GEikrZuxl06oozfk++FKnvQEpzN
hxN167quUWBqEUuSMhNQ9IfcmYe4DXymNWeBmUXh4a1PiedLLUm7ipQcktXQFH9g
ELnzoJ4FmA4Ci98B6rtJ3KjNhgB3DoEn9/n4ZXBNeA1qWGuOD+9+o/MBkaUkLtk/
H42+wv+9LQKBgQCxfNV4HeeDiXhlHKox8nZ12J+C/iDIv4pbOeP3FQ38IX+z6sma
t4nzhH2NKUMfXV8O5VgGP6KDCtdzK2guuCMmmTv7WNDl0wBXbNvvwEoAvtPCvoqS
DttZlo6uuCy7jbAMjj8kV7GVCf8JSmoml298XW8EbcidzJOIBVJEZ7JT0QKBgFzF
r5EaVDT/JWybxK0GEugTYBDTbDiCKJVEqPwc7Ew3lcBiOebLRBoA3UUSYPpDXgLA
trgLpgxkb+FhkTJzT/YdbvhCk95ddEj89me6HX+FVnX118JY4jkdyC1ghzkRPJtq
KGkrNaTBlE2IRwZMecB1hr1hTz2mab6MJ+K2L1ddAoGAWpgLDxUmwzPV0Dafnx8X
5uhxIiD+HhjKcRESYBYxBxwxR7r6rYq9heYzAIzB8f4S7Oj9DRSrU/EDMicPoSp7
kFWAyin6a2yYJzBwZwjLaso2NZqWvqBJyBMk2GzwN7UHhrAklTCLuFi8478GzNmv
3CVwcOEt635+vXb6Qrk/9zk=
-----END PRIVATE KEY-----
"""


def _config_with_mqtt_keys(extra: str) -> str:
    """MINIMAL_CONFIG with extra keys folded into its *existing* `mqtt:` block.

    Appending a second `mqtt:` section would be valid YAML and quietly wrong: duplicate top-level
    keys mean last-wins, so the required `server` would vanish -- the same trap docs/07 §2's own
    listing warns about.
    """
    return MINIMAL_CONFIG.replace(
        "  server: mqtt://192.168.1.10:1883", "  server: mqtt://192.168.1.10:1883\n" + extra
    )


def _client_certificate(tmp_path: Path) -> tuple[Path, Path]:
    cert_path = tmp_path / "client.crt"
    key_path = tmp_path / "client.key"
    cert_path.write_text(_TEST_CLIENT_CERT_PEM)
    key_path.write_text(_TEST_CLIENT_KEY_PEM)
    return cert_path, key_path


async def test_entity_exclude_and_include_reach_compile_options(tmp_path: Path) -> None:
    """`entities.exclude`/`include` (docs/07 §2): accepted and validated since WP9, and silently
    doing nothing until WP16.
    """
    path = _write(
        tmp_path,
        MINIMAL_CONFIG
        + """
entities:
  exclude: ["ABB1_*", "ABB2_ch0003"]
  include: ["ABB*"]
""",
    )
    config = await settings_to_supervisor_config(load_settings(path, environ={}))

    assert config.compile_options.exclude_patterns == ("ABB1_*", "ABB2_ch0003")
    assert config.compile_options.include_patterns == ("ABB*",)


async def test_stale_after_reaches_the_supervisor_config(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG + "\navailability:\n  stale_after: 3600\n")
    config = await settings_to_supervisor_config(load_settings(path, environ={}))
    assert config.stale_after_s == 3600


async def test_stale_after_defaults_to_disabled(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    config = await settings_to_supervisor_config(load_settings(path, environ={}))
    assert config.stale_after_s is None


async def test_log_to_mqtt_reaches_the_supervisor_config(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG + "\nadvanced:\n  log_to_mqtt: true\n")
    config = await settings_to_supervisor_config(load_settings(path, environ={}))
    assert config.log_to_mqtt is True


async def test_mqtt_client_certificate_is_loaded(tmp_path: Path) -> None:
    """`mqtt.cert`/`mqtt.key` were accepted and validated but never plumbed into an SSL context --
    named in `settings._build_mqtt_tls`'s own docstring as a real gap.
    """
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(_TEST_CA_PEM)
    cert_path, key_path = _client_certificate(tmp_path)
    path = _write(
        tmp_path,
        _config_with_mqtt_keys(f"  ca: {ca_path}\n  cert: {cert_path}\n  key: {key_path}\n"),
    )

    config = await settings_to_supervisor_config(load_settings(path, environ={}))

    assert config.mqtt_tls is not None
    # A context with a loaded chain reports it here; one without raises or reports nothing.
    loaded = config.mqtt_tls.get_ca_certs()
    assert loaded is not None
    assert config.mqtt_tls.cert_store_stats()["x509"] >= 1


async def test_a_client_certificate_without_its_key_is_rejected_at_load(tmp_path: Path) -> None:
    """Half a client certificate is a misconfiguration that must fail loudly at load time
    (docs/07 §1: fatal, with a precise field path), not at the first connect attempt.
    """
    cert_path, _key_path = _client_certificate(tmp_path)
    path = _write(tmp_path, _config_with_mqtt_keys(f"  cert: {cert_path}\n"))

    with pytest.raises(SettingsError, match=re.escape("mqtt.key")):
        load_settings(path, environ={})


async def test_a_key_without_its_certificate_is_rejected_at_load(tmp_path: Path) -> None:
    _cert_path, key_path = _client_certificate(tmp_path)
    path = _write(tmp_path, _config_with_mqtt_keys(f"  key: {key_path}\n"))

    with pytest.raises(SettingsError, match=re.escape("mqtt.cert")):
        load_settings(path, environ={})


# ------------------------------------------------------- WP16: the meta-test (docs/12 WP16)


# Knobs that `config.yaml` accepts and validates but that nothing acts on yet. Each entry must
# carry a reason. This list is the whole point of `test_no_silently_inert_settings`: the same
# defect has now been found three times (WP9's own docstring named five, the post-WP12 YAGNI pass
# found seven more, WP16 found four), always by someone reading code rather than by a test. An
# entry here is a deliberate, reviewed decision; a knob *missing* from both here and the wiring
# is the bug.
DELIBERATELY_INERT: dict[str, str] = {
    "mqtt.version": (
        "MQTT 3.1.1 only -- identifier+will on an MQTT 5 CONNECT hangs with the pinned "
        "paho-mqtt/aiomqtt pair (docs/04 §8, mqtt/client.py's docstring)"
    ),
    "homeassistant.legacy_entity_attributes": (
        "its payload shape is not specified anywhere in docs/04 §6; specifying or deleting it is "
        "an open decision (docs/12 §10.2)"
    ),
    "sysap.reconnect.jitter": (
        "the two link implementations use a fixed full-jitter policy (docs/06 §3); making the "
        "fraction configurable would mean threading it through both, and nothing has asked"
    ),
}

# Where a knob may legitimately be consumed. Not every setting reaches `SupervisorConfig`: the
# logging ones are applied before a Supervisor exists at all (secrets must be redacted from the
# first line, P-45) and `profiles_dir` is read while building the registry the Supervisor is
# handed. Listing the modules explicitly is what keeps "wired somewhere else" from becoming an
# excuse that hides a genuinely dropped knob.
_CONSUMER_MODULES = (freeathome2mqtt.settings, freeathome2mqtt.cli)


def _leaf_setting_names(model: type[BaseModel], prefix: str = "") -> list[str]:
    """Every leaf field in the `Settings` tree, as dotted paths (`mqtt.qos_state`)."""
    names: list[str] = []
    for field_name, field in model.model_fields.items():
        path = f"{prefix}{field_name}"
        annotation = field.annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            names.extend(_leaf_setting_names(annotation, prefix=f"{path}."))
        else:
            names.append(path)
    return names


def test_no_silently_inert_settings() -> None:
    """Every documented `config.yaml` knob either reaches the runtime or is listed as knowingly
    inert with a reason (docs/12 WP16).

    This is the durable half of WP16. Wiring the four knobs it found is a one-off; making it
    impossible to *add* an inert one by accident is what stops the defect recurring. The check is
    deliberately crude -- it looks for the field name in the translator's source -- because a
    precise one would need to model what "wired" means, and crude-but-honest catches the actual
    failure (a knob nobody ever mentions again) while a reviewer catches the rest.
    """
    sources = [inspect.getsource(module) for module in _CONSUMER_MODULES]

    unwired: list[str] = []
    for path in _leaf_setting_names(Settings):
        if path in DELIBERATELY_INERT:
            continue
        leaf = path.rsplit(".", 1)[-1]
        if not any(f".{leaf}" in source for source in sources):
            unwired.append(path)

    assert not unwired, (
        "these config.yaml knobs are accepted and validated but nothing reads them: "
        f"{sorted(unwired)}. Wire them, or add them to DELIBERATELY_INERT with a reason."
    )


def test_deliberately_inert_entries_are_real_settings() -> None:
    """The allowlist must not rot: an entry for a knob that no longer exists would silently
    excuse a *different* knob from the check above the day someone renames one.
    """
    known = set(_leaf_setting_names(Settings))
    stale = sorted(set(DELIBERATELY_INERT) - known)
    assert not stale, f"DELIBERATELY_INERT names settings that no longer exist: {stale}"


def test_every_inert_entry_carries_a_reason() -> None:
    assert all(reason.strip() for reason in DELIBERATELY_INERT.values())


async def test_adaptive_coalescing_settings_reach_the_supervisor_config(tmp_path: Path) -> None:
    """docs/05 §4.1's three knobs, inert until WP17 measured whether the feature was worth
    shipping (it publishes 160 messages against fixed coalescing's 400 under a scene ramp).
    """
    path = _write(
        tmp_path,
        MINIMAL_CONFIG
        + """
performance:
  coalesce_adaptive: true
  coalesce_max_ms: 150
  coalesce_burst_threshold: 10
""",
    )
    config = await settings_to_supervisor_config(load_settings(path, environ={}))

    assert config.coalesce_adaptive is True
    assert config.coalesce_max_ms == 150
    assert config.coalesce_burst_threshold == 10


async def test_adaptive_coalescing_is_off_by_default(tmp_path: Path) -> None:
    path = _write(tmp_path, MINIMAL_CONFIG)
    config = await settings_to_supervisor_config(load_settings(path, environ={}))
    assert config.coalesce_adaptive is False


def test_cache_config_is_no_longer_accepted(tmp_path: Path) -> None:
    """`advanced.cache_config` was removed in WP17 rather than implemented (docs/12 §7.2).

    The measurement did not support it: docs/05 §5 estimated the configuration cache would save
    "roughly 400 ms of the 3 s budget", but the compile it would skip actually measures ~29 ms at
    1 000 channels and cold start comes in at ~1.16 s against a 3 s budget. A cache file, its
    invalidation, and the docs/05 §6 risk of accidentally retaining the parsed configuration, in
    exchange for under 5 % of a budget with 60 % headroom, is not a trade worth making.

    Since `extra="forbid"` is set, a config file still naming it fails loudly with the field name
    -- which is the right outcome for a knob that never did anything.
    """
    path = _write(tmp_path, MINIMAL_CONFIG + "\nadvanced:\n  cache_config: true\n")

    with pytest.raises(SettingsError, match="cache_config"):
        load_settings(path, environ={})
