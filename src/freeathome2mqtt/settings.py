"""Pydantic configuration model, loader and environment overrides (docs/07; docs/11 WP9).

`config.yaml` is user-owned and validated at load time, fatal on error (docs/07 §1) -- a typo
stops the process with a precise field path, never a half-understood config. `load_settings()`
resolves the three secret mechanisms (docs/07 §2.1: environment override, `!env`, `!secret`) and
runs the semantic checks in docs/07 §2.2 before `Settings.model_validate()` even sees the data,
since some of those (`!secret`'s `data_dir` lookup) cannot be expressed as a pydantic validator.

`settings_to_supervisor_config()` is the one-way translator `supervisor.SupervisorConfig`'s own
docstring promises: a validated `Settings` becomes a `SupervisorConfig`, never the reverse. Several
`config.yaml` knobs documented in docs/07 §2 have no effect yet -- `mqtt.version` (MQTT 3.1.1 only,
see `mqtt/client.py`'s docstring), `homeassistant.legacy_entity_attributes` (its exact shape is not
specified anywhere in docs/04 §6, so it stays accepted-and-validated rather than guessed at),
`entities.exclude`/`include` (accepted and validated, not yet enforced by `model.compiler`),
adaptive coalescing, per-entity command overrides, `advanced.metrics`, `advanced.raw_mode`/
`cache_config`. Each is a named, deliberate gap, not a silent drop -- the schema still accepts and
validates them so a `config.yaml` written against the full docs/07 reference loads cleanly today
and picks up real behaviour as later work packages land. `homeassistant.enabled`/`discovery_topic`/
`status_topic`/`republish_delay` and `mqtt.maximum_packet_size` are wired as of WP10.
"""

from __future__ import annotations

import logging
import os
import ssl as ssl_module
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import pydantic
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from freeathome2mqtt.model.compiler import CompileOptions
from freeathome2mqtt.supervisor import SupervisorConfig
from freeathome2mqtt.sysap.codes.interfaces import Interface
from freeathome2mqtt.sysap.rest import build_ssl_context

logger = logging.getLogger(__name__)

_ENV_PREFIX = "FAH2MQTT_"
_DEFAULT_DATA_DIR = Path("/data")

# The full known universe of real interface codes (docs/01 §4.2) minus VD, which
# `entities.include_virtual_devices` already governs separately -- not part of this allowlist.
_KNOWN_INTERFACES = frozenset(i.value for i in Interface if i is not Interface.VIRTUAL_DEVICE)
_UNDEFINED_INTERFACE = "undefined"

_MAX_INFLIGHT_RANGE = range(1, 17)
_COALESCE_MS_RANGE = range(0, 1001)

_MQTT_DEFAULT_PORTS = {"mqtt": 1883, "mqtts": 8883}


class SettingsError(Exception):
    """`config.yaml` failed to load or validate; the message names the offending field."""


# ------------------------------------------------------------------------------------ sections


class ReconnectSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    initial: float = 1.0
    max: float = 60.0
    factor: float = 2.0
    jitter: float = 0.25


class SysApSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    username: str
    password: str
    verify_ssl: bool = True
    ssl_ca_file: Path | None = None
    max_inflight: int = 4
    request_timeout: float = 10.0
    ws_heartbeat: float = 30.0
    ws_idle_timeout: float = 90.0
    config_refresh_interval: float = 300.0
    reconnect: ReconnectSection = Field(default_factory=ReconnectSection)

    @field_validator("host")
    @classmethod
    def _normalize_host(cls, value: str) -> str:
        return _normalize_sysap_host(value)

    @field_validator("max_inflight")
    @classmethod
    def _max_inflight_range(cls, value: int) -> int:
        if value not in _MAX_INFLIGHT_RANGE:
            raise ValueError("max_inflight must be in 1..16")
        return value


class MqttSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    server: str
    base_topic: str = "freeathome2mqtt"
    client_id: str | None = None
    user: str | None = None
    password: str | None = None
    version: str | None = None
    keepalive: int = 60
    ca: Path | None = None
    cert: Path | None = None
    key: Path | None = None
    reject_unauthorized: bool = True
    maximum_packet_size: int = 1048576
    force_disable_retain: bool = False
    qos_state: int = 0
    qos_discovery: int = 1

    @field_validator("base_topic")
    @classmethod
    def _base_topic_is_safe(cls, value: str) -> str:
        _validate_base_topic(value)
        return value


class HomeAssistantSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    discovery_topic: str = "homeassistant"
    status_topic: str = "homeassistant/status"
    republish_delay: float = 5.0
    legacy_entity_attributes: bool = False


class EntitiesSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic_style: Literal["friendly", "id"] = "friendly"
    include_orphan_channels: bool = False
    include_virtual_devices: bool = False
    interfaces: list[str] = Field(
        default_factory=lambda: ["TP", "RF", "smokealarm", _UNDEFINED_INTERFACE]
    )
    publish_last_changed: bool = True
    exclude: list[str] = Field(default_factory=list)
    include: list[str] = Field(default_factory=list)

    @field_validator("interfaces")
    @classmethod
    def _known_interfaces(cls, value: list[str]) -> list[str]:
        unknown = set(value) - _KNOWN_INTERFACES - {_UNDEFINED_INTERFACE}
        if unknown:
            raise ValueError(f"unknown interfaces: {sorted(unknown)}")
        return value


class PerformanceSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    coalesce_ms: int = 20
    coalesce_adaptive: bool = False
    coalesce_max_ms: int = 200
    coalesce_burst_threshold: int = 25
    command_debounce_ms: int = 50
    optimistic: bool = True
    reconcile_after_ms: int = 3000
    get_rate_limit_s: float = 5.0

    @field_validator("coalesce_ms")
    @classmethod
    def _coalesce_ms_range(cls, value: int) -> int:
        if value not in _COALESCE_MS_RANGE:
            raise ValueError("coalesce_ms must be in 0..1000")
        return value


class AvailabilitySection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    grace_seconds: float = 10.0
    per_device: bool = True
    stale_after: float | None = None


class MetricsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    port: int = 9102


class AdvancedSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_dir: Path = _DEFAULT_DATA_DIR
    profiles_dir: Path = Path("/data/profiles")
    log_level: Literal["error", "warning", "info", "debug"] = "info"
    log_to_mqtt: bool = False
    log_format: Literal["text", "json"] = "text"
    raw_mode: Literal[False, "unsupported_only", True] = False
    cache_config: bool = True
    metrics: MetricsSection = Field(default_factory=MetricsSection)


class Settings(BaseModel):
    """The full `config.yaml` shape (docs/07 §2)."""

    model_config = ConfigDict(extra="forbid")

    sysap: SysApSection
    mqtt: MqttSection
    homeassistant: HomeAssistantSection = Field(default_factory=HomeAssistantSection)
    entities: EntitiesSection = Field(default_factory=EntitiesSection)
    performance: PerformanceSection = Field(default_factory=PerformanceSection)
    availability: AvailabilitySection = Field(default_factory=AvailabilitySection)
    advanced: AdvancedSection = Field(default_factory=AdvancedSection)

    @model_validator(mode="after")
    def _cross_section_checks(self) -> Settings:
        if self.homeassistant.discovery_topic == self.mqtt.base_topic:
            raise ValueError(
                "homeassistant.discovery_topic must differ from mqtt.base_topic "
                "(would interleave two incompatible topic trees)"
            )
        if (
            self.sysap.verify_ssl
            and self.sysap.ssl_ca_file is None
            and urlparse(self.sysap.host).scheme == "https"
        ):
            logger.warning(
                "sysap.verify_ssl is true with no ssl_ca_file and an https host -- the SysAP's "
                "stock self-signed certificate will fail verification"
            )
        return self


# --------------------------------------------------------------------------- field-level helpers


def _normalize_sysap_host(host: str) -> str:
    """docs/07 §2.2: `sysap.host` must parse as a URL with an http/https scheme -- a bare IP or
    hostname is accepted and normalised (assumed https) with a warning, not rejected outright.
    """
    parsed = urlparse(host)
    if parsed.scheme in ("http", "https"):
        return host
    logger.warning("sysap.host %r has no scheme; assuming https://", host)
    normalized = f"https://{host}"
    if not urlparse(normalized).netloc:
        raise ValueError(f"sysap.host is not a valid URL or bare host: {host!r}")
    return normalized


def _validate_base_topic(base_topic: str) -> None:
    if (
        "+" in base_topic
        or "#" in base_topic
        or base_topic.startswith("/")
        or base_topic.endswith("/")
    ):
        raise ValueError(
            f"mqtt.base_topic {base_topic!r} must not contain '+'/'#' or have a leading/"
            "trailing '/' (broker-level breakage)"
        )


def _parse_mqtt_server(server: str) -> tuple[str, int]:
    parsed = urlparse(server)
    if parsed.scheme not in _MQTT_DEFAULT_PORTS or not parsed.hostname:
        raise SettingsError(
            f"mqtt.server {server!r} must look like mqtt://host:port or mqtts://host:port"
        )
    port = parsed.port if parsed.port is not None else _MQTT_DEFAULT_PORTS[parsed.scheme]
    return parsed.hostname, port


# -------------------------------------------------------------------------------- env overrides


def _apply_env_overrides(data: dict[str, Any], *, environ: dict[str, str]) -> dict[str, Any]:
    """docs/07 §2.1: `FAH2MQTT_<SECTION>__<KEY>` overrides any scalar, `__` nesting."""
    for env_key, value in environ.items():
        if not env_key.startswith(_ENV_PREFIX):
            continue
        path = env_key[len(_ENV_PREFIX) :].lower().split("__")
        _set_nested(data, path, value, env_key=env_key)
    return data


def _set_nested(data: dict[str, Any], path: list[str], value: str, *, env_key: str) -> None:
    node = data
    for segment in path[:-1]:
        child = node.setdefault(segment, {})
        if not isinstance(child, dict):
            raise SettingsError(f"{env_key}: {'.'.join(path)} is not a nested section")
        node = child
    node[path[-1]] = value


# ------------------------------------------------------------------------------ !env/!secret/!file


def _make_loader(data_dir: Path, *, environ: dict[str, str]) -> type[yaml.SafeLoader]:
    """A fresh `SafeLoader` subclass per call (not a shared module-level class, CLAUDE.md rule 6)
    so `!secret`'s `data_dir` binding and `!env`'s `environ` binding never leak between
    concurrent loads -- both bound to the *same* `environ` the env-override pass uses, so tests
    never have to reach past `load_settings()`'s own `environ` parameter into real process state.
    """

    class _Loader(yaml.SafeLoader):
        pass

    def _construct_env(loader: yaml.SafeLoader, node: yaml.Node) -> str:
        var = loader.construct_scalar(node)  # type: ignore[arg-type]
        value = environ.get(var)
        if value is None:
            raise SettingsError(f"!env {var} is not set")
        return value

    def _construct_secret(loader: yaml.SafeLoader, node: yaml.Node) -> str:
        name = loader.construct_scalar(node)  # type: ignore[arg-type]
        secrets_path = data_dir / "secrets.yaml"
        if not secrets_path.exists():
            raise SettingsError(f"!secret {name}: {secrets_path} does not exist")
        secrets = yaml.safe_load(secrets_path.read_text())
        if not isinstance(secrets, dict) or name not in secrets:
            raise SettingsError(f"!secret {name}: not found in {secrets_path}")
        return str(secrets[name])

    def _construct_file(loader: yaml.SafeLoader, node: yaml.Node) -> str:
        path = Path(loader.construct_scalar(node))  # type: ignore[arg-type]
        try:
            return path.read_text().rstrip("\r\n")
        except OSError as exc:
            raise SettingsError(f"!file {path}: {exc}") from exc

    _Loader.add_constructor("!env", _construct_env)
    _Loader.add_constructor("!secret", _construct_secret)
    _Loader.add_constructor("!file", _construct_file)
    return _Loader


def _format_validation_error(path: Path, exc: pydantic.ValidationError) -> str:
    lines = [f"{path}: invalid configuration:"]
    for error in exc.errors():
        field = ".".join(str(part) for part in error["loc"])
        lines.append(f"  {field}: {error['msg']}")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------- loading


def load_settings(
    path: Path, *, data_dir: Path | None = None, environ: dict[str, str] | None = None
) -> Settings:
    """Load, resolve `!env`/`!secret`/`!file`, apply env overrides, validate -- fatal on error.

    `data_dir` resolves `!secret` and defaults to docs/07 §2's own default (`/data`); pass the
    `--data-dir` CLI flag's value so a config that changes `advanced.data_dir` and uses `!secret`
    in the same file is unambiguous about which directory that lookup used.
    """
    resolved_environ = environ if environ is not None else dict(os.environ)
    loader_cls = _make_loader(
        data_dir if data_dir is not None else _DEFAULT_DATA_DIR, environ=resolved_environ
    )
    try:
        text = path.read_text()
    except OSError as exc:
        raise SettingsError(f"{path}: {exc}") from exc
    try:
        raw = yaml.load(text, Loader=loader_cls)  # noqa: S506 -- loader_cls subclasses SafeLoader
    except yaml.YAMLError as exc:
        raise SettingsError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise SettingsError(f"{path}: top level must be a mapping")

    raw = _apply_env_overrides(raw, environ=resolved_environ)
    try:
        return Settings.model_validate(raw)
    except pydantic.ValidationError as exc:
        raise SettingsError(_format_validation_error(path, exc)) from exc


# -------------------------------------------------------------------- Settings -> SupervisorConfig


async def build_sysap_ssl(sysap: SysApSection) -> ssl_module.SSLContext | bool:
    if not sysap.verify_ssl:
        return await build_ssl_context("off")
    if sysap.ssl_ca_file is not None:
        return await build_ssl_context("ca_file", ca_file=sysap.ssl_ca_file)
    return await build_ssl_context("verify")


async def _build_mqtt_tls(mqtt: MqttSection) -> ssl_module.SSLContext | None:
    """Only a CA file is wired -- `mqtt.cert`/`mqtt.key` (client-certificate auth) are accepted
    by the schema but not yet plumbed into a context; a real, named gap, not silently dropped.
    """
    if mqtt.ca is None:
        return None
    context = await build_ssl_context("ca_file", ca_file=mqtt.ca)
    return cast(ssl_module.SSLContext, context)


async def settings_to_supervisor_config(settings: Settings) -> SupervisorConfig:
    """The one-way translation `SupervisorConfig`'s own docstring names as settings.py's job."""
    sysap_ssl = await build_sysap_ssl(settings.sysap)
    mqtt_tls = await _build_mqtt_tls(settings.mqtt)
    mqtt_host, mqtt_port = _parse_mqtt_server(settings.mqtt.server)

    configured = set(settings.entities.interfaces) - {_UNDEFINED_INTERFACE}
    excluded_interfaces = frozenset(_KNOWN_INTERFACES - configured)

    return SupervisorConfig(
        sysap_base_url=settings.sysap.host.rstrip("/"),
        sysap_username=settings.sysap.username,
        sysap_password=settings.sysap.password,
        sysap_ssl=sysap_ssl,
        sysap_max_inflight=settings.sysap.max_inflight,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=settings.mqtt.user,
        mqtt_password=settings.mqtt.password,
        mqtt_tls=mqtt_tls,
        mqtt_keepalive=settings.mqtt.keepalive,
        base_topic=settings.mqtt.base_topic,
        compile_options=CompileOptions(
            topic_prefix=settings.mqtt.base_topic,
            topic_style="slug" if settings.entities.topic_style == "friendly" else "id",
            include_orphan_channels=settings.entities.include_orphan_channels,
            include_virtual_devices=settings.entities.include_virtual_devices,
            excluded_interfaces=excluded_interfaces,
        ),
        data_dir=settings.advanced.data_dir,
        coalesce_ms=settings.performance.coalesce_ms,
        publish_last_changed=settings.entities.publish_last_changed,
        command_debounce_s=settings.performance.command_debounce_ms / 1000,
        reconcile_delay_s=settings.performance.reconcile_after_ms / 1000,
        get_rate_limit_s=settings.performance.get_rate_limit_s,
        grace_seconds=settings.availability.grace_seconds,
        config_refresh_interval_s=settings.sysap.config_refresh_interval,
        link_backoff_initial=settings.sysap.reconnect.initial,
        link_backoff_factor=settings.sysap.reconnect.factor,
        link_backoff_cap=settings.sysap.reconnect.max,
        ws_heartbeat_s=settings.sysap.ws_heartbeat,
        ws_idle_timeout_s=settings.sysap.ws_idle_timeout,
        homeassistant_enabled=settings.homeassistant.enabled,
        homeassistant_discovery_topic=settings.homeassistant.discovery_topic,
        homeassistant_status_topic=settings.homeassistant.status_topic,
        homeassistant_republish_delay_s=settings.homeassistant.republish_delay,
        mqtt_maximum_packet_size=settings.mqtt.maximum_packet_size,
    )
