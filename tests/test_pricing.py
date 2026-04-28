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
