#!/usr/bin/env sh
# Three lines, deliberately (see src/freeathome2mqtt/addon.py's module docstring): every decision
# an add-on's shell script normally gets wrong -- a missing option, a null overriding a default,
# an ssl service given an mqtt:// URL -- lives in tested Python instead.
set -eu

bashio::services.available mqtt || true
bashio::services mqtt > /tmp/mqtt-service.json 2>/dev/null || rm -f /tmp/mqtt-service.json

exec_args=""
[ -f /tmp/mqtt-service.json ] && exec_args="--mqtt-service /tmp/mqtt-service.json"

# shellcheck disable=SC2086 -- exec_args is intentionally word-split; it is either empty or a flag pair
python -m freeathome2mqtt.addon --options /data/options.json --out /data/config.yaml $exec_args

exec freeathome2mqtt --config /data/config.yaml
