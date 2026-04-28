"""Tests for pricing.calculate_cost and the markup logic.

Money-touching pure-Python — perfect first target for unit tests after the
IPN verifier (HANDOFF.md §8 P3-Op-3).
"""

from __future__ import annotations

import pytest

from pricing import (
    FALLBACK_PRICE,
    MODEL_PRICES,
    _apply_markup,
    calculate_cost,
    get_markup,
    get_price,
)


def test_known_model_uses_table_price():
    """gpt-4o-mini is in the table at (0.15, 0.60). 1M input + 1M output, no
    markup environment override here so default 1.5x applies."""
    cost = calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000)
    # (1.0M * 0.15 + 1.0M * 0.60) / 1M = 0.75. * 1.5 markup = 1.125.
    assert cost == pytest.approx(1.125)


def test_unknown_model_uses_fallback_price():
    """Unmapped models charge at FALLBACK_PRICE so we never silently undercharge."""
    cost = calculate_cost("vendor/never-heard-of-it", 1_000_000, 1_000_000)
    # FALLBACK is (10, 30): (10 + 30) * 1.5 = 60.
    assert cost == pytest.approx(60.0)


def test_zero_tokens_zero_cost():
    assert calculate_cost("openai/gpt-3.5-turbo", 0, 0) == 0.0


def test_cost_scales_linearly_with_tokens():
    a = calculate_cost("openai/gpt-4o", 1000, 1000)
    b = calculate_cost("openai/gpt-4o", 2000, 2000)
    # 2x tokens -> 2x cost (within float).
    assert b == pytest.approx(2 * a)


def test_markup_env_override(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    assert get_markup() == 2.0


def test_markup_below_1_clamped(monkeypatch):
    """COST_MARKUP=0.5 would mean charging less than cost; treat as a typo
    and clamp to 1.0 (no profit, no loss)."""
    monkeypatch.setenv("COST_MARKUP", "0.5")
    assert get_markup() == 1.0


def test_markup_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "not-a-number")
    assert get_markup() == 1.5


def test_markup_unset_default(monkeypatch):
    monkeypatch.delenv("COST_MARKUP", raising=False)
    assert get_markup() == 1.5


def test_apply_markup_never_negative():
    """Defensive: cost should never be negative even on weird inputs."""
    cost = _apply_markup(FALLBACK_PRICE, prompt_tokens=0, completion_tokens=0)
    assert cost == 0.0


def test_get_price_returns_fallback_for_unmapped():
    assert get_price("totally-fake/model") is FALLBACK_PRICE


def test_get_price_returns_table_entry_for_mapped():
    p = get_price("anthropic/claude-3.5-sonnet")
    assert p is MODEL_PRICES["anthropic/claude-3.5-sonnet"]
    assert p.input_per_1m_usd == 3.00
    assert p.output_per_1m_usd == 15.00


# ---------------------------------------------------------------------
# Non-finite / negative pricing guards (PR: pricing-finite-guards)
# ---------------------------------------------------------------------
#
# Pre-fix bugs:
#
# (a) ``get_markup`` parsed ``COST_MARKUP`` with a bare ``float()``,
#     which accepts the strings ``"nan"``, ``"inf"``, ``"-inf"``
#     (case-insensitive). A NaN markup propagates through every
#     IEEE-754 op (``raw * NaN`` is NaN, ``max(NaN, 1.0)`` is NaN),
#     eventually reaching ``database.deduct_balance``. The DB-layer
#     ``_is_finite_amount`` guard refuses the SQL — but the caller
#     in ``ai_engine.chat_with_model`` then logs ``cost=0`` and the
#     user gets a free reply on a paid model, with the platform
#     eating the OpenRouter cost.
#
# (b) ``_apply_markup`` had no defensive check on the ModelPrice
#     fields themselves. A non-finite or negative price slipping in
#     from the live OpenRouter catalog (or a stub in a test) would
#     short-circuit to ``cost=0`` via the same path.
#
# Both regressions are pinned below.

@pytest.mark.parametrize("bad_value", ["nan", "NaN", "inf", "Infinity", "-inf", "-Infinity"])
def test_markup_rejects_non_finite_env(monkeypatch, bad_value):
    """COST_MARKUP=nan / inf / -inf must fall back to the default 1.5x.

    Pre-fix ``float("nan")`` slipped through ``max(nan, 1.0)`` —
    Python's ``max`` returns the first arg when neither comparison
    is true, so ``max(nan, 1.0)`` returns ``nan``. That NaN then
    poisoned every cost calculation downstream.
    """
    monkeypatch.setenv("COST_MARKUP", bad_value)
    assert get_markup() == 1.5


def test_markup_rejects_negative_inf_env_via_finite_check(monkeypatch):
    """Belt-and-suspenders: ``float("-inf")`` is also < 1.0 so the
    pre-existing ``max(markup, 1.0)`` clamp would have caught it,
    but the explicit ``isfinite`` guard is what catches ``+inf``
    (which is > 1.0 and would otherwise pass through unchanged
    into ``raw * inf = inf`` cost).
    """
    monkeypatch.setenv("COST_MARKUP", "inf")
    assert get_markup() == 1.5


@pytest.mark.parametrize(
    "input_per_1m, output_per_1m",
    [
        (float("nan"), 1.0),
        (1.0, float("nan")),
        (float("inf"), 1.0),
        (1.0, float("inf")),
        (float("-inf"), 1.0),
        (-1.0, 1.0),
        (1.0, -1.0),
    ],
)
def test_apply_markup_falls_back_for_non_finite_or_negative_price(
    monkeypatch, input_per_1m, output_per_1m
):
    """A non-finite or negative price must NOT silently round to $0.

    Pre-fix a NaN price gave ``raw = NaN``, then ``max(NaN, 0.0) =
    NaN``; ``deduct_balance`` refused the SQL but ``log_usage``
    recorded ``cost=0``, so the user got a free paid-model reply.
    Post-fix we substitute ``FALLBACK_PRICE`` (the conservative
    $10/$30 per-1M default) so paid models stay paid even when the
    upstream price is corrupted.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    bad_price = type(FALLBACK_PRICE)(
        input_per_1m_usd=input_per_1m, output_per_1m_usd=output_per_1m
    )
    cost = _apply_markup(bad_price, prompt_tokens=1_000_000, completion_tokens=1_000_000)
    # FALLBACK_PRICE is (10, 30): (10 + 30) * 1.5 markup = 60.
    assert cost == pytest.approx(60.0)


def test_calculate_cost_happy_path_pin_with_finite_guards(monkeypatch):
    """Regression pin: the finite/negative guards in ``_apply_markup``
    must NOT alter the happy-path cost for a normal positive price.
    Without this pin a refactor of the guard could accidentally
    round every legitimate cost to FALLBACK_PRICE.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    # 1M input + 1M output at gpt-4o-mini's (0.15, 0.60) per 1M.
    # (0.15 + 0.60) * 1.5 = 1.125.
    assert calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000) == pytest.approx(1.125)
