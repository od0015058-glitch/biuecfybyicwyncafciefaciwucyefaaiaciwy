"""Unit tests for ``payments.effective_min_usd`` /
``payments.find_cheaper_alternative`` / ``refresh_min_amounts_loop``.

These helpers land alongside the $2 global floor + per-currency
pre-flight check (Stage-10-Step-A). They exist to make the checkout
flow's sub-minimum refusal actionable — telling the user ``"min for
BTC is $X, you can pay $Y with USDT-TRC20 instead"`` instead of the
prior "pick a cheaper coin" dead-end.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

import payments


@pytest.fixture(autouse=True)
def _clear_min_amount_cache():
    payments._min_amount_cache.clear()
    yield
    payments._min_amount_cache.clear()


def _seed(ticker: str, min_usd: float | None) -> None:
    """Shortcut: prime the module cache as if the background refresher
    had just populated it.

    We don't care about the stored timestamp for these tests — the
    lookups under test read the cached min value directly, not the TTL.
    """
    payments._min_amount_cache[ticker.lower()] = (min_usd, 0.0)


# ---------------------------------------------------------------------------
# effective_min_usd
# ---------------------------------------------------------------------------

def test_effective_min_usd_missing_cache_returns_global_floor():
    """Cache miss → fall back to the global $2 floor only."""
    assert payments.effective_min_usd("btc") == payments.GLOBAL_MIN_TOPUP_USD


def test_effective_min_usd_none_cached_value_returns_global_floor():
    """A cached ``None`` (lookup failed) still collapses to the floor."""
    _seed("btc", None)
    assert payments.effective_min_usd("btc") == payments.GLOBAL_MIN_TOPUP_USD


def test_effective_min_usd_takes_max_of_per_currency_and_floor():
    _seed("btc", 10.0)
    _seed("trx", 0.5)
    assert payments.effective_min_usd("btc") == 10.0
    # per-currency floor < global floor → global floor wins so we
    # never accept a top-up below GLOBAL_MIN_TOPUP_USD even when
    # NowPayments would.
    assert payments.effective_min_usd("trx") == payments.GLOBAL_MIN_TOPUP_USD


def test_effective_min_usd_is_case_insensitive():
    _seed("usdttrc20", 2.5)
    assert payments.effective_min_usd("USDTTRC20") == 2.5
    assert payments.effective_min_usd("UsdtTrc20") == 2.5


# ---------------------------------------------------------------------------
# find_cheaper_alternative
# ---------------------------------------------------------------------------

_CANDIDATES = [
    ("₿ Bitcoin", "btc"),
    ("Ξ Ethereum", "eth"),
    ("⚡ TRON (TRX)", "trx"),
    ("💵 USDT (TRC20)", "usdttrc20"),
    ("💵 USDT (BEP20)", "usdtbsc"),
]


def test_find_alt_returns_cheapest_viable_coin():
    """Given a $3 request and BTC min $10, the helper should point at
    the coin with the lowest effective min that still clears $3."""
    _seed("btc", 10.0)
    _seed("eth", 8.0)
    _seed("trx", 2.0)          # below $2 floor → collapses to $2 floor
    _seed("usdttrc20", 2.5)
    _seed("usdtbsc", 2.5)
    alt = payments.find_cheaper_alternative(
        requested_usd=3.0,
        excluded_currency="btc",
        candidates=_CANDIDATES,
    )
    assert alt is not None
    label, ticker = alt
    # TRX has the lowest effective floor ($2 global floor) — ties
    # broken by ticker sort so the result is deterministic.
    assert ticker == "trx"
    assert "TRX" in label


def test_find_alt_excludes_current_currency():
    """We never suggest the coin the user just tried."""
    _seed("btc", 2.0)
    _seed("eth", 2.0)
    alt = payments.find_cheaper_alternative(
        requested_usd=2.0,
        excluded_currency="btc",
        candidates=[("₿ Bitcoin", "btc"), ("Ξ Ethereum", "eth")],
    )
    assert alt is not None
    assert alt[1] == "eth"


def test_find_alt_returns_none_when_request_below_global_floor():
    """A $1 request can't clear the $2 global floor on any coin, so
    there's no viable alternative to suggest."""
    _seed("btc", 10.0)
    _seed("trx", 0.5)  # under-floor; effective = $2
    alt = payments.find_cheaper_alternative(
        requested_usd=1.0,
        excluded_currency="btc",
        candidates=_CANDIDATES,
    )
    assert alt is None


def test_find_alt_returns_none_when_every_other_coin_also_too_small():
    """If every candidate's effective min is above the request, no
    alternative exists. Caller falls back to the "pick a higher
    amount" message."""
    for _, t in _CANDIDATES:
        _seed(t, 20.0)
    alt = payments.find_cheaper_alternative(
        requested_usd=5.0,
        excluded_currency="btc",
        candidates=_CANDIDATES,
    )
    assert alt is None


def test_find_alt_missing_cache_falls_back_to_global_floor_match():
    """When we have no cached per-currency data at all, every
    candidate's effective min = GLOBAL_MIN_TOPUP_USD. A request at
    or above the floor matches the first non-excluded candidate."""
    alt = payments.find_cheaper_alternative(
        requested_usd=payments.GLOBAL_MIN_TOPUP_USD,
        excluded_currency="btc",
        candidates=_CANDIDATES,
    )
    assert alt is not None
    assert alt[1] != "btc"


# ---------------------------------------------------------------------------
# refresh_min_amounts_once / refresh_min_amounts_loop
# ---------------------------------------------------------------------------

async def test_refresh_once_repopulates_stale_cache():
    """A cold cache + one refresh pass should end up populated."""
    with patch.object(
        payments,
        "_query_min_amount",
        AsyncMock(side_effect=lambda c_from, c_to: 3.0),
    ):
        await payments.refresh_min_amounts_once(["btc", "eth"], concurrency=2)

    assert "btc" in payments._min_amount_cache
    assert "eth" in payments._min_amount_cache


async def test_refresh_once_tolerates_per_ticker_failure():
    """An exception inside ``get_min_amount_usd`` for one ticker must
    not prevent other tickers from being refreshed."""
    async def flaky(*_args, **_kwargs):
        raise RuntimeError("simulated outage for this ticker")

    # Pick one ticker that errors and one that succeeds.
    call_state: dict[str, int] = {}

    async def side(c_from, c_to):
        call_state[c_from] = call_state.get(c_from, 0) + 1
        if c_from == "btc":
            raise RuntimeError("simulated network error")
        return 4.2

    with patch.object(payments, "_query_min_amount", AsyncMock(side_effect=side)):
        await payments.refresh_min_amounts_once(["btc", "eth"], concurrency=2)

    # btc's cache entry either missing or None — critically, eth's
    # value still got populated.
    btc_cached = payments._min_amount_cache.get("btc")
    eth_cached = payments._min_amount_cache.get("eth")
    assert eth_cached is not None
    assert eth_cached[0] == 4.2
    # btc may be present with None (both queries failed) or absent.
    if btc_cached is not None:
        assert btc_cached[0] is None


async def test_refresh_loop_cancels_cleanly():
    """Spawning and cancelling the forever-loop must not raise."""
    with patch.object(
        payments,
        "_query_min_amount",
        AsyncMock(return_value=2.5),
    ):
        task = asyncio.create_task(
            payments.refresh_min_amounts_loop(
                ["btc"], interval_seconds=60
            )
        )
        # Let one iteration run.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
