"""Slugify, alias resolution and deterministic collision handling (ADR-010; docs/03 §1.1; WP3)."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_MAX_SLUG_LENGTH = 64
_FALLBACK_SLUG = "unnamed"
_NON_ALNUM_RUN = re.compile(r"[^a-z0-9]+")

# Explicit German transliteration (docs/03 §1.1): NFKD alone drops combining marks and does
# nothing for ß, so "Küche" -> "kche" and "Straße" -> "strae" -- both unusable. Applied before
# NFKD so these specific substitutions are never touched by the generic accent-stripping below.
_UMLAUT_TABLE = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
}


class SlugResolutionError(Exception):
    """Every escalation, including the immutable entity id, still collided (docs/03 §1.1).

    Documented as "impossible, but assert it" -- this is that assertion, as a real exception
    rather than a bare `assert` (CLAUDE.md rule 5), since it must survive `python -O`.
    """


def slugify(name: str) -> str:
    """Turn `name` into a topic-safe slug matching ``^[a-z0-9_]{1,64}$`` (docs/03 §1.1).

    Lowercase, transliterate German umlauts/eszett explicitly, NFKD-normalise and drop remaining
    combining marks (a lossy but acceptable fallback for other accents), replace any run of
    non-``[a-z0-9]`` with ``_``, strip/collapse, truncate to 64 chars. Never empty (P-40).
    """
    text = name.lower()
    for char, replacement in _UMLAUT_TABLE.items():
        text = text.replace(char, replacement)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _NON_ALNUM_RUN.sub("_", text)
    text = text.strip("_")
    text = text[:_MAX_SLUG_LENGTH].strip("_")
    return text or _FALLBACK_SLUG


@dataclass(frozen=True, slots=True)
class SlugCandidate:
    """One entity's naming inputs, ready for collision resolution (docs/03 §1.1)."""

    entity_id: str
    name: str
    area: str | None
    channel_id: str


def _slug_options(candidate: SlugCandidate) -> list[str]:
    primary = slugify(candidate.name)
    options = [primary]
    if candidate.area:
        options.append(slugify(f"{candidate.area}_{candidate.name}"))
    options.append(f"{options[-1]}_{slugify(candidate.channel_id)}")
    options.append(slugify(candidate.entity_id))
    return options


def resolve_slugs(candidates: Iterable[SlugCandidate]) -> dict[str, str]:
    """Assign each candidate a unique topic-segment slug, deterministically (docs/03 §1.1, P-39).

    A bare name shared by two or more candidates is resolved globally, up front: every candidate
    whose primary slug collides with another's is excluded from that tier entirely, so all of
    them escalate to the area-prefixed tier together. Deciding this any other way -- e.g. "first
    in some order keeps the plain slug" -- would let a *new* device with an alphabetically-earlier
    id silently steal an *existing* device's topic the moment their names collide, which is
    exactly the instability P-39/P-54 rule out. Only once the bare tier is settled does order
    matter: remaining ties escalate one candidate at a time, in `entity_id` order, to the
    channel-id suffix and finally the entity's immutable id, so the result never depends on input
    order or set/dict iteration order.
    """
    ordered = sorted(candidates, key=lambda c: c.entity_id)
    primary_counts = Counter(slugify(c.name) for c in ordered)
    claimed_by: dict[str, str] = {}
    result: dict[str, str] = {}

    for candidate in ordered:
        options = _slug_options(candidate)
        eligible = options[1:] if primary_counts[options[0]] > 1 else options
        chosen = next((option for option in eligible if option not in claimed_by), None)
        if chosen is None:
            raise SlugResolutionError(
                f"could not resolve a unique slug for {candidate.entity_id!r}; "
                f"every escalation collided: {options!r}"
            )
        if chosen != options[0]:
            logger.warning(
                "slug collision on %r: %s escalated to %r", options[0], candidate.entity_id, chosen
            )
        claimed_by[chosen] = candidate.entity_id
        result[candidate.entity_id] = chosen

    return result
