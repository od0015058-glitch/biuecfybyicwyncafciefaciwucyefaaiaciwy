"""Tests for ``models_catalog._parse_price``.

Pre-fix bug: ``_parse_price`` used ``float(raw)`` with no
``math.isfinite`` / non-negative check. ``float()`` accepts the
strings ``"NaN"``, ``"inf"``, ``"-inf"`` (case-insensitive) and
returns the corresponding IEEE-754 special. A non-finite or negative
price slipping through here flows into ``ModelPrice.input_per_1m_usd``
/ ``output_per_1m_usd``, then through ``pricing._apply_markup`` where
``raw * markup`` is also NaN/Inf. ``_apply_markup`` clamps via
``max(raw, 0.0)`` so a NaN drops to NaN and a negative price drops to
0. The downstream ``database.deduct_balance`` finite guard refuses the
SQL on NaN/Inf — but ``ai_engine.chat_with_model`` then logs
``cost=0`` and the user gets a free reply on what should have been
a paid model.

Post-fix: ``_parse_price`` returns ``None`` for non-finite or
negative prices, so the caller (``_fetch_from_openrouter``) drops
the model from the catalog entirely. The user then can't even pick
the broken model — and if they're already on it from a previous
catalog version, ``get_model_price`` falls through to the static
``MODEL_PRICES`` table or to ``FALLBACK_PRICE``, both of which are
finite + positive.
"""

from __future__ import annotations

import math

import pytest

from models_catalog import _parse_price


def test_none_returns_none():
    assert _parse_price(None) is None


def test_explicit_zero_string_returns_zero():
    """Free models legitimately report ``"0"`` for the per-token
    price. The post-fix behaviour must NOT confuse this with the
    ``None`` "missing/malformed" sentinel — otherwise the
    free-tier picker would silently fall back to ``FALLBACK_PRICE``
    and start charging users $10/$30 per 1M for a free model.
    """
    assert _parse_price("0") == 0.0
    assert _parse_price("0.0") == 0.0
    assert _parse_price(0) == 0.0
    assert _parse_price(0.0) == 0.0


def test_positive_float_string_returns_float():
    """OpenRouter's real prices are tiny per-token decimals; pin a
    representative value so the parse path stays a no-op for
    well-formed input.
    """
    # gpt-4o input price as USD per token: 0.0000025
    assert _parse_price("0.0000025") == pytest.approx(2.5e-6)


def test_garbage_string_returns_none():
    assert _parse_price("not-a-number") is None
    assert _parse_price("") is None


def test_unparseable_object_returns_none():
    """Lists / dicts / ints are not what OpenRouter sends but
    ``float()`` raises ``TypeError`` for them — make sure the parser
    swallows it rather than blowing up the catalog refresh.
    """
    assert _parse_price([]) is None
    assert _parse_price({}) is None


@pytest.mark.parametrize("bad", ["NaN", "nan", "NAN", "Inf", "inf", "Infinity", "-inf", "-Infinity"])
def test_non_finite_string_returns_none(bad):
    """The whole point of this PR. ``float("NaN")`` / ``float("inf")``
    succeed silently; the post-fix guard rejects them.
    """
    assert _parse_price(bad) is None


def test_non_finite_floats_return_none():
    assert _parse_price(float("nan")) is None
    assert _parse_price(float("inf")) is None
    assert _parse_price(float("-inf")) is None


@pytest.mark.parametrize("neg", ["-0.001", "-1", "-1e-7", -1.0, -0.0001])
def test_negative_price_returns_none(neg):
    """A negative price would round to zero through
    ``_apply_markup``'s ``max(raw, 0.0)`` clamp, silently turning
    the model into a free one. Reject upstream.
    """
    assert _parse_price(neg) is None


def test_negative_zero_accepted_as_zero():
    """``-0.0`` is *not* less than ``0.0`` per IEEE-754 (they compare
    equal) so it's a legitimate "free" signal, not the bug class
    we're rejecting. Keep the guard tight to actually-negative
    values so a free-tier model that happens to serialize as
    ``"-0"`` doesn't get dropped from the catalog.
    """
    parsed = _parse_price("-0.0")
    assert parsed is not None
    assert parsed == 0.0
    assert math.isfinite(parsed)
