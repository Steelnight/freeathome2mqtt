"""Tests for model/naming.py: slugify and collision resolution (docs/03 §1.1; docs/11 WP3)."""

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freeathome2mqtt.model.naming import SlugCandidate, resolve_slugs, slugify

SLUG_RE = re.compile(r"^[a-z0-9_]{1,64}$")


# ------------------------------------------------------------------------------- slugify


def test_slugify_german_umlauts() -> None:
    # P-40: NFKD alone drops combining marks and does nothing for ß -- "Küche" must not become
    # "kche", and "Straße" must not become "strae".
    assert slugify("Küche") == "kueche"
    assert slugify("Straße") == "strasse"
    assert slugify("Grün") == "gruen"
    assert slugify("Björk") == "bjoerk"


def test_slugify_uppercase_umlauts() -> None:
    assert slugify("ÜBER") == "ueber"
    assert slugify("Ötzi") == "oetzi"


def test_slugify_lowercases() -> None:
    assert slugify("Deckenlicht") == "deckenlicht"


def test_slugify_replaces_non_alnum_runs_with_single_underscore() -> None:
    assert slugify("Living Room #1!!") == "living_room_1"


def test_slugify_strips_leading_and_trailing_underscores() -> None:
    assert slugify("  --Light--  ") == "light"


def test_slugify_truncates_to_64_chars() -> None:
    slug = slugify("x" * 200)
    assert len(slug) == 64


def test_slugify_truncation_does_not_leave_a_trailing_underscore() -> None:
    # 63 letters then a boundary that would fall exactly on a separator run.
    name = ("a" * 63) + " " + ("b" * 10)
    slug = slugify(name)
    assert len(slug) <= 64
    assert not slug.endswith("_")


def test_slugify_is_stable() -> None:
    assert slugify("Küche") == slugify("Küche")


@pytest.mark.parametrize("name", ["", "###", "   ", "́́"])  # empty / pure-symbol / combining marks
def test_slugify_degenerate_input_still_matches_the_slug_pattern(name: str) -> None:
    assert SLUG_RE.match(slugify(name))


# ------------------------------------------------------------------------------- property tests


@given(name=st.text(max_size=200))
def test_slug_validity(name: str) -> None:
    # docs/10 §5: for any Unicode string, the slug matches ^[a-z0-9_]{1,64}$.
    assert SLUG_RE.match(slugify(name)) is not None


@given(name=st.text(max_size=200))
def test_slug_stability_property(name: str) -> None:
    assert slugify(name) == slugify(name)


# ----------------------------------------------------------------------- collision resolution


def test_slug_collision_resolution_is_deterministic() -> None:
    # P-39: "Deckenlicht" in two different rooms both slug to "deckenlicht".
    candidates = [
        SlugCandidate(
            entity_id="AAA_ch0001", name="Deckenlicht", area="Wohnzimmer", channel_id="ch0001"
        ),
        SlugCandidate(
            entity_id="BBB_ch0002", name="Deckenlicht", area="Küche", channel_id="ch0002"
        ),
    ]
    first = resolve_slugs(candidates)
    second = resolve_slugs(list(reversed(candidates)))
    assert first == second
    assert len(set(first.values())) == 2
    assert first["AAA_ch0001"] == "wohnzimmer_deckenlicht"
    assert first["BBB_ch0002"] == "kueche_deckenlicht"


def test_slug_collision_escalates_to_channel_id_when_area_prefix_still_collides() -> None:
    candidates = [
        SlugCandidate(
            entity_id="AAA_ch0001", name="Deckenlicht", area="Küche", channel_id="ch0001"
        ),
        SlugCandidate(
            entity_id="BBB_ch0002", name="Deckenlicht", area="Küche", channel_id="ch0002"
        ),
    ]
    slugs = resolve_slugs(candidates)
    assert len(set(slugs.values())) == 2
    assert slugs["AAA_ch0001"] == "kueche_deckenlicht"
    assert slugs["BBB_ch0002"] == "kueche_deckenlicht_ch0002"


def test_slug_collision_falls_back_to_entity_id_as_a_last_resort() -> None:
    # Contrived: same name, no area (so there is only one escalation tier before the fallback),
    # AND the same channel id string (only possible across two different devices) -- escalation
    # exhausts itself and must fall back to the immutable id.
    candidates = [
        SlugCandidate(entity_id="AAA_ch0001", name="Deckenlicht", area=None, channel_id="ch0001"),
        SlugCandidate(entity_id="BBB_ch0001", name="Deckenlicht", area=None, channel_id="ch0001"),
    ]
    slugs = resolve_slugs(candidates)
    assert len(set(slugs.values())) == 2
    assert slugs["AAA_ch0001"] == "deckenlicht_ch0001"
    assert slugs["BBB_ch0001"] == "bbb_ch0001"


def test_no_collision_uses_the_plain_slug() -> None:
    candidates = [
        SlugCandidate(
            entity_id="AAA_ch0001", name="Deckenlicht", area="Wohnzimmer", channel_id="ch0001"
        ),
        SlugCandidate(entity_id="BBB_ch0002", name="Steckdose", area="Küche", channel_id="ch0002"),
    ]
    slugs = resolve_slugs(candidates)
    assert slugs["AAA_ch0001"] == "deckenlicht"
    assert slugs["BBB_ch0002"] == "steckdose"


def test_collision_resolution_without_area_escalates_straight_to_channel_id() -> None:
    candidates = [
        SlugCandidate(entity_id="AAA_ch0001", name="Deckenlicht", area=None, channel_id="ch0001"),
        SlugCandidate(entity_id="BBB_ch0002", name="Deckenlicht", area=None, channel_id="ch0002"),
    ]
    slugs = resolve_slugs(candidates)
    assert len(set(slugs.values())) == 2
    assert slugs["AAA_ch0001"] == "deckenlicht_ch0001"
    assert slugs["BBB_ch0002"] == "deckenlicht_ch0002"


@given(
    st.lists(
        st.tuples(st.text(min_size=1, max_size=20), st.text(min_size=0, max_size=20)),
        min_size=1,
        max_size=15,
        unique_by=lambda pair: pair,
    )
)
def test_collision_resolution_slugs_are_always_unique_and_order_independent(
    pairs: list[tuple[str, str]],
) -> None:
    candidates = [
        SlugCandidate(
            entity_id=f"ENTITY{i:03d}", name=name, area=area or None, channel_id=f"ch{i:04d}"
        )
        for i, (name, area) in enumerate(pairs)
    ]
    forward = resolve_slugs(candidates)
    backward = resolve_slugs(list(reversed(candidates)))
    assert forward == backward
    assert len(set(forward.values())) == len(candidates)
