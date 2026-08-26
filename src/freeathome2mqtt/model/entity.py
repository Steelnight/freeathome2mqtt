"""``Entity``, ``AttributeSpec``, ``CommandSpec``, ``Binding`` and ``EgressBinding`` (docs/03 §2,
§3.2, §3.3; docs/11 WP3).

`AttributeSpec`/`CommandSpec` are the pre-compilation shape: one profile's
``attributes.<name>``/``commands.<name>`` YAML entry, loaded and validated by `model/profiles.py`.
`Entity`/`Binding`/`EgressBinding` are the post-compilation shape `model/compiler.py` produces --
optimised for the hot path (docs/02 §4): attribute access by integer index, codecs already bound so
nothing on the hot path does name lookup or dispatch (ADR-004).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Literal

from freeathome2mqtt.sysap.codes import Pairing


class AttrKind(IntEnum):
    """Attribute kind (docs/03 §3.2): STATE is retained/coalesced/deduped, EVENT is neither."""

    STATE = 0
    EVENT = 1


@dataclass(frozen=True, slots=True)
class AttributeSpec:
    """One `attributes.<name>` entry from a profile, before compilation (docs/03 §3.2).

    `codec_params` carries any construction keyword a specific codec factory needs beyond the
    fixed schema keys above -- e.g. `scaled`'s `factor`/`offset` -- passed to `build_codec`
    verbatim (docs/03 §5's registry is a name -> factory map precisely so this stays open-ended
    without `AttributeSpec` needing to know every codec's parameter set).
    """

    name: str
    pairing: Pairing
    codec: str
    kind: AttrKind = AttrKind.STATE
    unit: str | None = None
    values: Mapping[str, str] | None = None
    default: Any = None
    entity_category: Literal["diagnostic", "config"] | None = None
    precision: int | None = None
    codec_params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """One `commands.<name>` entry from a profile, before compilation (docs/03 §3.3).

    `values` is the **inverse** direction of an `AttributeSpec`'s map -- symbolic name -> raw
    string, never derived from the attribute's raw -> symbolic map (P-03).
    """

    name: str
    pairing: Pairing
    codec: str
    values: Mapping[str, str] | None = None
    range: tuple[float, float] | None = None
    continuous: bool = False
    optimistic: str | None = None
    confirm: bool = True
    codec_params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Entity:
    """One compiled channel (docs/03 §2). Not frozen -- declared this way in docs/03 §2, unlike
    `Binding`/`EgressBinding` below.
    """

    idx: int
    id: str
    profile: str
    name: str
    area: str | None
    device_serial: str
    channel_id: str
    attr_names: tuple[str, ...]
    attr_kinds: tuple[int, ...]
    state_topic: str
    set_topic: str
    get_topic: str
    availability_topic: str | None
    optimistic: bool
    discovery: tuple[tuple[str, bytes], ...]


@dataclass(frozen=True, slots=True)
class Binding:
    """Ingress: what a WS datapoint key means (docs/03 §2). Keyed in the ingress table by
    ``"SERIAL/chXXXX/odpXXXX"``.
    """

    entity_idx: int
    attr_idx: int
    decode: Callable[[str], Any]
    kind: int
    attr_bit: int


@dataclass(frozen=True, slots=True)
class EgressBinding:
    """Egress: how to execute a command (docs/03 §2). Keyed in the egress table by
    ``(entity_idx, command_name)``.
    """

    entity_idx: int
    rest_path: str
    encode: Callable[[Any], str]
    continuous: bool
    optimistic_attr: int | None
    validate: Callable[[Any], Any]
    confirm: bool = True
