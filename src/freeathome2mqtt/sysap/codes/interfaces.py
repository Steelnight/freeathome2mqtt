"""GENERATED — A Device.interface value (docs/01 §4.2); absent/null means undefined.

Do not hand-edit; regenerate with ``uv run python -m freeathome2mqtt.tools.gen_codes`` (docs/11
WP1). See ``sysap/codes/NOTICE`` for licence attribution.

Source: docs/01 §4.2 (no upstream file to snapshot).
"""

from enum import StrEnum


class Interface(StrEnum):
    """A Device.interface value (docs/01 §4.2); absent/null means undefined."""

    WIRED_BUS = "TP"
    WIRELESS_RF = "RF"
    HUE = "hue"
    SONOS = "sonos"
    SMOKEALARM = "smokealarm"
    VIRTUAL_DEVICE = "VD"
