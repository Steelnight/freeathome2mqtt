"""Tests for settings.py: pydantic config model, env overrides, !env/!secret/!file, semantic
validation, and the Settings -> SupervisorConfig translation (docs/07; docs/11 WP9).
"""

from __future__ import annotations

import ssl
from pathlib import Path

import pytest
import yaml

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
