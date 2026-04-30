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


def test_active_pay_currencies_filters_disabled():
    """handlers._active_pay_currencies filters based on admin_toggles cache."""
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


def test_currency_grid_layout():
    from handlers import _currency_grid_layout

    assert _currency_grid_layout(9) == (3, 3, 3)
    assert _currency_grid_layout(7) == (3, 3, 1)
    assert _currency_grid_layout(0) == ()
    assert _currency_grid_layout(1) == (1,)
    assert _currency_grid_layout(3) == (3,)


# ---- refresh_disabled_* fail-soft (Stage-15-Step-D #3-extension) ----
#
# These tests use ``async def`` rather than ``asyncio.run(...)`` because
# pytest-asyncio is configured in ``pytest.ini`` with ``mode=Mode.AUTO``
# (auto-detects async tests) and several other tests in this suite use
# the deprecated ``asyncio.get_event_loop()`` pattern that breaks after
# a sibling test calls ``asyncio.run()`` and closes the global loop.


import asyncio

import pytest


class _FailingDB:
    """Stub asyncpg-shaped DB that raises on the SELECT calls."""

    def __init__(self, exc: BaseException):
        self._exc = exc

    async def get_disabled_models(self):  # noqa: D401 — DB shape
        raise self._exc

    async def get_disabled_gateways(self):  # noqa: D401 — DB shape
        raise self._exc


class _OkDB:
    """Stub DB that returns canned snapshots."""

    def __init__(self, models: set[str], gateways: set[str]):
        self._models = models
        self._gateways = gateways

    async def get_disabled_models(self):
        return set(self._models)

    async def get_disabled_gateways(self):
        return set(self._gateways)


async def test_refresh_disabled_models_fail_soft_preserves_cache():
    """A transient DB error after an admin toggle must NOT clobber
    the in-memory cache. Pre-fix, the bare ``await`` propagated up and
    returned a 500 to the admin panel even though the canonical
    ``disabled_models`` row write had already succeeded — and worse,
    a clear-on-error design would falsely re-enable every disabled
    model in the meantime.
    """
    _reset()
    # Seed the cache with two disabled models.
    admin_toggles._disabled_models = {"openai/gpt-4o", "anthropic/claude-3"}

    failing = _FailingDB(RuntimeError("transient asyncpg ConnectionDoesNotExist"))
    # Refresh swallows the exception (no propagation).
    await admin_toggles.refresh_disabled_models(failing)

    # Cache is preserved as-is.
    assert admin_toggles._disabled_models == {"openai/gpt-4o", "anthropic/claude-3"}
    assert admin_toggles.is_model_disabled("openai/gpt-4o") is True


async def test_refresh_disabled_gateways_fail_soft_preserves_cache():
    """Mirror test for the gateways side."""
    _reset()
    admin_toggles._disabled_gateways = {"btc", "tetrapay"}

    failing = _FailingDB(RuntimeError("simulated DB outage"))
    await admin_toggles.refresh_disabled_gateways(failing)

    assert admin_toggles._disabled_gateways == {"btc", "tetrapay"}
    assert admin_toggles.is_gateway_disabled("btc") is True


async def test_refresh_disabled_models_happy_path_replaces_cache():
    """A successful refresh still replaces the cache with the new snapshot."""
    _reset()
    admin_toggles._disabled_models = {"old/model"}

    ok = _OkDB(models={"new/model-a", "new/model-b"}, gateways=set())
    await admin_toggles.refresh_disabled_models(ok)

    assert admin_toggles._disabled_models == {"new/model-a", "new/model-b"}
    assert admin_toggles.is_model_disabled("old/model") is False


async def test_refresh_disabled_gateways_happy_path_replaces_cache():
    """Gateways-side mirror of the happy-path test."""
    _reset()
    admin_toggles._disabled_gateways = {"old-gateway"}

    ok = _OkDB(models=set(), gateways={"btc"})
    await admin_toggles.refresh_disabled_gateways(ok)

    assert admin_toggles._disabled_gateways == {"btc"}
    assert admin_toggles.is_gateway_disabled("old-gateway") is False


async def test_refresh_disabled_models_does_not_swallow_cancelled_error():
    """``asyncio.CancelledError`` must propagate even with the
    fail-soft try/except — it inherits from ``BaseException`` since
    Python 3.8, so a bare ``except Exception`` correctly skips it.
    Pinning the behaviour so a future widening to ``except BaseException``
    can't silently break task cancellation."""
    _reset()
    admin_toggles._disabled_models = {"sentinel/model"}

    failing = _FailingDB(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await admin_toggles.refresh_disabled_models(failing)

    # Cache still preserved (CancelledError raised before assignment).
    assert admin_toggles._disabled_models == {"sentinel/model"}
