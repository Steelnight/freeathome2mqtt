# freeathome2mqtt (Home Assistant add-on)

Bridges an ABB / Busch-Jaeger free@home System Access Point to MQTT, and publishes Home Assistant
MQTT discovery so your devices appear without any further configuration.

## Setup

1. Install and start the **Mosquitto broker** add-on. This add-on declares `mqtt: need`, so it
   will refuse to start with a clear message rather than silently publishing nowhere.
2. Enable the **Local API** on your SysAP (Settings → free@home Settings → Local API).
3. Fill in `sysap_host`, `sysap_username` and `sysap_password` below and start the add-on.

Broker credentials are supplied by Home Assistant automatically — you never type them here.

## Options

| Option | Required | Notes |
|---|---|---|
| `sysap_host` | yes | `https://192.168.1.50`. Use `http://` only on a trusted network. |
| `sysap_username` | yes | Usually `installer`. |
| `sysap_password` | yes | The Local API password. |
| `sysap_verify_ssl` | no | Set `false` for the SysAP's stock self-signed certificate. |
| `sysap_max_inflight` | no | Concurrent requests to the SysAP; the default of 4 is deliberate. |
| `mqtt_base_topic` | no | Defaults to `freeathome2mqtt`. |
| `homeassistant_enabled` | no | Turn discovery off to use the plain MQTT topics only. |
| `entities_include_virtual_devices` | no | |
| `performance_coalesce_ms` | no | `0` for lowest latency; the default of 20 batches scene bursts. |
| `log_level` | no | `error`, `warning`, `info` (default) or `debug`. |

The full configuration reference, including everything this add-on does not expose, is in
[`docs/07`](https://github.com/Steelnight/freeathome2mqtt/blob/main/docs/07-configuration.md).
The generated `/data/config.yaml` is readable and is worth looking at if something is wrong.

## Troubleshooting

The add-on log is the bridge's log. Set `log_level: debug` for detail. `bridge/info` on MQTT
carries live counters and link state; `bridge/devices` lists every channel found, including the
ones no profile matched — that is the thing to attach to a bug report.
