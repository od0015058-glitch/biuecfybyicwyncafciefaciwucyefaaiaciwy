"""Tests for admin_toggles — in-memory disabled model / gateway cache.

Stage-14. The module is pure in-memory; these tests never touch the DB.
"""

from __future__ import annotations

import admin_toggles


def _reset():
    """Reset the module-level caches to empty before each test."""
    admin_toggles._disabled_models = set()
    admin_toggles._disabled_gateways = set()


# ---- is_model_disabled ---------------------------------------------


def test_model_not_disabled_by_default():
    _reset()
    assert admin_toggles.is_model_disabled("openai/gpt-4o") is False


def test_model_disabled_after_cache_set():
    _reset()
    admin_toggles._disabled_models = {"openai/gpt-4o", "anthropic/claude-3"}
    assert admin_toggles.is_model_disabled("openai/gpt-4o") is True
    assert admin_toggles.is_model_disabled("anthropic/claude-3") is True
    assert admin_toggles.is_model_disabled("google/gemini-2") is False


# ---- is_gateway_disabled -------------------------------------------


def test_gateway_not_disabled_by_default():
    _reset()
    assert admin_toggles.is_gateway_disabled("btc") is False


def test_gateway_disabled_after_cache_set():
    _reset()
    admin_toggles._disabled_gateways = {"tetrapay", "btc"}
    assert admin_toggles.is_gateway_disabled("tetrapay") is True
    assert admin_toggles.is_gateway_disabled("btc") is True
    assert admin_toggles.is_gateway_disabled("eth") is False


# ---- get_disabled_models / get_disabled_gateways --------------------


def test_get_disabled_models_snapshot():
    _reset()
    admin_toggles._disabled_models = {"a", "b"}
    result = admin_toggles.get_disabled_models()
    assert isinstance(result, frozenset)
    assert result == frozenset({"a", "b"})


def test_get_disabled_gateways_snapshot():
    _reset()
    admin_toggles._disabled_gateways = {"tetrapay"}
    result = admin_toggles.get_disabled_gateways()
    assert isinstance(result, frozenset)
    assert result == frozenset({"tetrapay"})


# ---- _active_pay_currencies helper in handlers ----------------------


def test_active_pay_currencies_filters_disabled(monkeypatch):
    """handlers._active_pay_currencies filters based on admin_toggles cache.

    Unrelated Stage-15 gate: also requires ``NOWPAYMENTS_API_KEY`` to
    be set (otherwise every crypto ticker is dropped — see the new
    Stage-15-Step-D bug fix). We pin the env var to a dummy value
    here so this test stays scoped to the toggle-filter behaviour
    it was originally written to cover; the new env-gate path has
    its own dedicated test below.
    """
    monkeypatch.setenv("NOWPAYMENTS_API_KEY", "dummy-key-for-test")
    _reset()
    from handlers import _active_pay_currencies, SUPPORTED_PAY_CURRENCIES

    # All enabled by default.
    active = _active_pay_currencies()
    assert len(active) == len(SUPPORTED_PAY_CURRENCIES)

    # Disable "btc".
    admin_toggles._disabled_gateways = {"btc"}
    active = _active_pay_currencies()
    tickers = [ticker for _, ticker in active]
    assert "btc" not in tickers
    assert "eth" in tickers

    _reset()


def test_active_pay_currencies_empty_when_nowpayments_unset(monkeypatch):
    """Stage-15-Step-D bundled bug fix: the picker must NOT surface
    crypto tickers when ``NOWPAYMENTS_API_KEY`` is unset.

    Pre-fix the picker still listed BTC / ETH / etc. even though
    every invoice-creation attempt would fail with a cryptic
    "Invalid API key" error from NowPayments. Post-fix the list is
    empty so the dual-currency entry / wallet hub falls back to
    showing only TetraPay (Rial), which is the correct UX for a
    deploy that hasn't yet enabled NowPayments.
    """
    monkeypatch.delenv("NOWPAYMENTS_API_KEY", raising=False)
    _reset()
    from handlers import _active_pay_currencies

    assert _active_pay_currencies() == []

    # Whitespace-only key is treated identically (we ``strip()`` the
    # value so an operator who left ``NOWPAYMENTS_API_KEY=  `` in
    # ``.env`` doesn't accidentally re-enable the broken picker).
    monkeypatch.setenv("NOWPAYMENTS_API_KEY", "   ")
    assert _active_pay_currencies() == []

    monkeypatch.setenv("NOWPAYMENTS_API_KEY", "real-key")
    assert len(_active_pay_currencies()) > 0


def test_currency_grid_layout():
    from handlers import _currency_grid_layout

    assert _currency_grid_layout(9) == (3, 3, 3)
    assert _currency_grid_layout(7) == (3, 3, 1)
    assert _currency_grid_layout(0) == ()
    assert _currency_grid_layout(1) == (1,)
    assert _currency_grid_layout(3) == (3,)
