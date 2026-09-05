"""The Home Assistant add-on's options -> ``config.yaml`` translation (docs/07 §5.3; docs/12 WP18).

docs/11 WP12 listed a Home Assistant add-on wrapper as an optional deliverable and did not build
one; `release.yml`'s header says so. This is that wrapper's one interesting part.

**Why this is Python and not shell.** An add-on's job is to turn `/data/options.json` plus the
Supervisor's MQTT service credentials into a config file, and that is exactly where an add-on's
bugs live: a missing option, a null that overrides a good default, an `ssl: true` service that
still gets an `mqtt://` URL. Written in `jq` and `sed` inside `run.sh` none of it can be tested;
written here, all of it is, including the assertion that matters most -- that what comes out loads
cleanly through the same `load_settings` the bridge itself uses (`test_addon.py`). `run.sh` is
consequently three lines and has nothing to get wrong.

**What is not verified.** The add-on *manifest* (`addon/config.yaml`), its build, and its
installation need a Home Assistant Supervisor, which no CI runner here has. Those files are
written from the documented add-on schema and reviewed by hand; that is not the same as running
them, and this note is here rather than in a commit message so the limitation stays visible.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_OPTIONS_PATH = Path("/data/options.json")
_DEFAULT_CONFIG_PATH = Path("/data/config.yaml")
_DATA_DIR = "/data"

_REQUIRED_OPTIONS = ("sysap_host", "sysap_username", "sysap_password")

# add-on option -> (config.yaml section, key). Only options the user actually set are written:
# an absent one must leave the schema's own default in place rather than overriding it with null.
_OPTIONAL_OPTIONS: dict[str, tuple[str, str]] = {
    "sysap_verify_ssl": ("sysap", "verify_ssl"),
    "sysap_max_inflight": ("sysap", "max_inflight"),
    "mqtt_base_topic": ("mqtt", "base_topic"),
    "homeassistant_enabled": ("homeassistant", "enabled"),
    "entities_include_virtual_devices": ("entities", "include_virtual_devices"),
    "performance_coalesce_ms": ("performance", "coalesce_ms"),
    "log_level": ("advanced", "log_level"),
}


class AddonError(Exception):
    """The add-on cannot produce a usable `config.yaml`; the message names what to fix."""


def build_bridge_config(
    options: dict[str, Any], *, mqtt_service: dict[str, Any] | None
) -> dict[str, Any]:
    """`config.yaml` as a plain dict, from add-on options plus the Supervisor's MQTT service.

    `mqtt_service` is what the Supervisor's MQTT service discovery API returns. The add-on
    declares `mqtt: need`, so `None` here means the Mosquitto add-on is not running -- an
    actionable condition, and one the user can only act on if told.
    """
    missing = [name for name in _REQUIRED_OPTIONS if not options.get(name)]
    if missing:
        raise AddonError(f"required add-on option(s) not set: {', '.join(missing)}")
    if mqtt_service is None:
        raise AddonError(
            "no MQTT service is available from the Supervisor; is the Mosquitto broker add-on "
            "installed and running?"
        )

    config: dict[str, Any] = {
        "sysap": {
            "host": options["sysap_host"],
            "username": options["sysap_username"],
            "password": options["sysap_password"],
        },
        "mqtt": {
            "server": _mqtt_url(mqtt_service),
            "user": mqtt_service.get("username"),
            "password": mqtt_service.get("password"),
        },
        "advanced": {"data_dir": _DATA_DIR},
    }
    for option_name, (section, key) in _OPTIONAL_OPTIONS.items():
        if option_name in options:
            config.setdefault(section, {})[key] = options[option_name]
    return config


def write_bridge_config(
    options: dict[str, Any], *, mqtt_service: dict[str, Any] | None, path: Path
) -> None:
    """Render `build_bridge_config` to `path` as readable YAML.

    Readable rather than a one-line JSON blob because users do open this file when something is
    wrong, and the add-on's whole purpose is to spare them configuration work, not to hide it.
    """
    config = build_bridge_config(options, mqtt_service=mqtt_service)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False, default_flow_style=False))


def _mqtt_url(service: dict[str, Any]) -> str:
    scheme = "mqtts" if service.get("ssl") else "mqtt"
    host = service.get("host", "core-mosquitto")
    port = service.get("port", 8883 if service.get("ssl") else 1883)
    return f"{scheme}://{host}:{port}"


def _load_json(path: Path) -> dict[str, Any]:
    body = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(body, dict):
        raise AddonError(f"{path}: expected a JSON object")
    return body


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m freeathome2mqtt.addon`, the whole of what `run.sh` does."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--options", type=Path, default=_DEFAULT_OPTIONS_PATH)
    parser.add_argument("--mqtt-service", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=_DEFAULT_CONFIG_PATH)
    args = parser.parse_args(argv)

    try:
        options = _load_json(args.options)
        service = _load_json(args.mqtt_service) if args.mqtt_service is not None else None
        write_bridge_config(options, mqtt_service=service, path=args.out)
    except (OSError, json.JSONDecodeError, AddonError) as exc:
        print(f"add-on configuration failed: {exc}", file=sys.stderr)
        return 1
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
