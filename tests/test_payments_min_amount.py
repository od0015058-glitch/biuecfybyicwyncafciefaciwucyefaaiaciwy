"""Unit tests for ``payments.get_min_amount_usd``.

Pinned bug (this PR): the cache hit short-circuited BEFORE the
``attempted_usd`` trustworthiness filter ran, so a cached value that
was correct for one user's attempt could mislead another user whose
attempt is differently sized.

Concrete repro: user A attempts $0.10 (below the real $0.16 floor).
The lookup returns $0.16, the trustworthiness check is happy
(``0.16 < 0.10`` is False), the value gets cached as $0.16. User B
then attempts $5 in the same currency. Pre-fix, the cache hit returns
$0.16 directly, the rejection UI renders "min $0.16" — actively
misleading because the user just sent $5 and got rejected, so the
real floor is clearly *above* $5. Post-fix the trustworthiness check
runs on cache hits too and returns ``None`` so the UI falls back to
"unknown min" / generic language.

The cache also now stores the raw (un-suppressed) value so a
follow-up small-attempt call can re-surface the floor that's
genuinely correct for it. Pre-fix the cache stored the post-
suppression value, so once $5-was-rejected suppressed the cache to
``None``, every follow-up — including legitimate small-attempt ones
that actually deserved the $0.16 number — saw ``None``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import payments


@pytest.fixture(autouse=True)
def _clear_min_amount_cache():
    """Each test gets a fresh module-level cache."""
    payments._min_amount_cache.clear()
    yield
    payments._min_amount_cache.clear()


async def test_fresh_lookup_returns_floor_when_attempt_is_below():
    """Baseline: small attempt ($0.10 < real $0.16 floor) — lookup
    returns $0.16, the trustworthiness check is happy, value passes
    through to the caller."""
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ):
        result = await payments.get_min_amount_usd(
            "btc", attempted_usd=0.10
        )
    assert result == 0.16


async def test_fresh_lookup_suppresses_floor_when_attempt_is_above():
    """The trustworthiness check fires on a fresh fetch when the
    rejected attempt is above the looked-up floor."""
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ):
        result = await payments.get_min_amount_usd(
            "btc", attempted_usd=5.0
        )
    assert result is None


async def test_cache_hit_after_small_attempt_does_not_mislead_large_attempt():
    """The original repro: cache warmed by a small-attempt call
    returns the suppressed ``None`` for a large-attempt call.

    Pre-fix, the cache hit path returned the cached $0.16 *directly*
    without re-running the trustworthiness filter, so the second
    call (with attempted_usd=$5) got back $0.16 and the UI rendered
    the misleading "min $0.16" against a $5 rejection.
    """
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ) as mock_query:
        first = await payments.get_min_amount_usd(
            "btc", attempted_usd=0.10
        )
        assert first == 0.16
        # Second call should use the cache (no extra HTTP).
        second = await payments.get_min_amount_usd(
            "btc", attempted_usd=5.0
        )
    # 2 calls total in the first lookup, none added by the cache hit.
    assert mock_query.await_count == 2
    # Pre-fix this returned 0.16 (the cached value) — wrong.
    assert second is None


async def test_cache_hit_after_large_attempt_resurfaces_floor_for_small_attempt():
    """Symmetric repro: cache warmed by a large-attempt call (where
    the trustworthiness filter fires) must NOT poison a follow-up
    small-attempt call.

    Pre-fix the cache stored the *post-suppression* ``None`` value,
    so the second call returned ``None`` and the user lost the real
    "$0.16 min" hint — even though their $0.10 attempt is clearly
    below that floor and the floor *is* the right thing to render.
    """
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ) as mock_query:
        first = await payments.get_min_amount_usd(
            "btc", attempted_usd=5.0
        )
        assert first is None
        # Same currency, smaller attempt → cache hit, no extra HTTP,
        # but the trustworthiness check should re-evaluate and pass
        # this time.
        second = await payments.get_min_amount_usd(
            "btc", attempted_usd=0.10
        )
    assert mock_query.await_count == 2
    # Pre-fix this was ``None`` (the cached suppressed value) — wrong.
    assert second == 0.16


async def test_cache_hit_with_no_attempted_usd_returns_raw_value():
    """A caller that didn't supply ``attempted_usd`` (e.g. an
    operator probing the floor for diagnostics) gets the unfiltered
    cached value back."""
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ):
        await payments.get_min_amount_usd("btc", attempted_usd=0.10)
        result = await payments.get_min_amount_usd("btc")
    assert result == 0.16


async def test_cache_stores_raw_value_not_suppressed_value():
    """The cache snapshot is the raw lookup result, not whatever the
    current call decided to return after filtering. This is what
    enables the previous test to work: a re-evaluation against a
    different ``attempted_usd`` needs the un-filtered value."""
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ):
        # Fire a lookup that gets suppressed for *this* call.
        await payments.get_min_amount_usd("btc", attempted_usd=5.0)
    # The cache should still hold the raw $0.16 — not ``None``.
    cached_value, _ts = payments._min_amount_cache["btc"]
    assert cached_value == 0.16


async def test_uppercase_currency_normalized_in_cache_key():
    """Regression pin: the cache is keyed on the lowercased
    currency. ``get_min_amount_usd("BTC", ...)`` and
    ``get_min_amount_usd("btc", ...)`` must hit the same bucket."""
    with patch.object(
        payments, "_query_min_amount", AsyncMock(side_effect=[0.16, 0.10])
    ) as mock_query:
        await payments.get_min_amount_usd("BTC", attempted_usd=0.10)
        # Same call again with lowercase. If the cache key normalised
        # correctly, this is a cache hit (no new HTTP).
        await payments.get_min_amount_usd("btc", attempted_usd=0.10)
    assert mock_query.await_count == 2


# ---------------------------------------------------------------------
# ``_query_min_amount`` finite / non-negative guards
# ---------------------------------------------------------------------
#
# Pre-fix bug: ``_query_min_amount`` parsed the API response's
# ``fiat_equivalent`` field with a bare ``float()``. ``float("NaN")``
# / ``float("inf")`` succeed silently and return the IEEE-754
# special. The non-finite value then:
#
# 1. was cached by ``get_min_amount_usd`` against the pay_currency
# 2. slipped past the trustworthiness filter unchanged because every
#    comparison against NaN is False (``nan < attempted`` is False),
# 3. was returned to ``create_crypto_invoice`` and stored on
#    ``MinAmountError.min_usd``,
# 4. was rendered to the user as ``f"min ${nan:.2f}"`` ⇒ ``"min $nan"``.
#
# A negative ``fiat_equivalent`` (clearly wrong — a min amount is by
# definition non-negative) similarly slipped through and rendered
# nonsensically.
#
# Post-fix: the parser returns ``None`` for non-finite or negative
# input, so ``get_min_amount_usd`` falls back to the generic
# "unknown min" branch of the UI rather than rendering nonsense.

import math  # noqa: E402  (placed here to keep the diff localised)


@pytest.mark.parametrize("bad", ["NaN", "nan", "Inf", "inf", "-inf"])
async def test_query_min_amount_rejects_non_finite_strings(bad):
    """``_query_min_amount`` must NOT return NaN/Inf even though the
    API surfaced them as parseable strings.

    Pre-fix ``float("NaN")`` returned ``nan`` and the value
    propagated all the way to the user-facing error message
    (``"min $nan"``). Post-fix returns ``None``.
    """
    fake_response = AsyncMock()
    fake_response.status = 200
    fake_response.json = AsyncMock(return_value={"fiat_equivalent": bad})
    fake_response.__aenter__ = AsyncMock(return_value=fake_response)
    fake_response.__aexit__ = AsyncMock(return_value=None)

    fake_session = AsyncMock()
    fake_session.get = lambda *a, **kw: fake_response
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=None)

    with patch.object(payments.aiohttp, "ClientSession", lambda *a, **kw: fake_session):
        result = await payments._query_min_amount("btc", "usd")
    assert result is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
async def test_query_min_amount_rejects_non_finite_numerics(bad):
    """Same guard applied when the API returns a JSON number that
    deserialises to a non-finite Python float (e.g. some libraries
    parse ``Infinity`` literals as ``inf``).
    """
    fake_response = AsyncMock()
    fake_response.status = 200
    fake_response.json = AsyncMock(return_value={"fiat_equivalent": bad})
    fake_response.__aenter__ = AsyncMock(return_value=fake_response)
    fake_response.__aexit__ = AsyncMock(return_value=None)

    fake_session = AsyncMock()
    fake_session.get = lambda *a, **kw: fake_response
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=None)

    with patch.object(payments.aiohttp, "ClientSession", lambda *a, **kw: fake_session):
        result = await payments._query_min_amount("btc", "usd")
    assert result is None


@pytest.mark.parametrize("neg", [-0.001, -1.0, -100.0, "-0.5", "-1"])
async def test_query_min_amount_rejects_negative(neg):
    """A negative min-amount is meaningless; the API shouldn't send
    one but if it does, treat it as malformed rather than caching
    a value that would render as ``"min -$1.00"`` in the UI.
    """
    fake_response = AsyncMock()
    fake_response.status = 200
    fake_response.json = AsyncMock(return_value={"fiat_equivalent": neg})
    fake_response.__aenter__ = AsyncMock(return_value=fake_response)
    fake_response.__aexit__ = AsyncMock(return_value=None)

    fake_session = AsyncMock()
    fake_session.get = lambda *a, **kw: fake_response
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=None)

    with patch.object(payments.aiohttp, "ClientSession", lambda *a, **kw: fake_session):
        result = await payments._query_min_amount("btc", "usd")
    assert result is None


async def test_query_min_amount_accepts_zero():
    """Edge: a returned ``"0"`` is unusual but not malformed —
    it parses as a finite non-negative number. Don't drop it; let
    the trustworthiness filter and downstream callers decide.
    """
    fake_response = AsyncMock()
    fake_response.status = 200
    fake_response.json = AsyncMock(return_value={"fiat_equivalent": 0})
    fake_response.__aenter__ = AsyncMock(return_value=fake_response)
    fake_response.__aexit__ = AsyncMock(return_value=None)

    fake_session = AsyncMock()
    fake_session.get = lambda *a, **kw: fake_response
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=None)

    with patch.object(payments.aiohttp, "ClientSession", lambda *a, **kw: fake_session):
        result = await payments._query_min_amount("btc", "usd")
    assert result == 0.0
    assert math.isfinite(result)


async def test_get_min_amount_usd_returns_none_on_non_finite_lookup():
    """End-to-end pin: when ``_query_min_amount`` rejects the
    upstream NaN/Inf and returns ``None``, ``get_min_amount_usd``
    must NOT hand back a non-finite value to ``create_crypto_invoice``.
    Pre-fix a NaN flowed all the way into ``MinAmountError.min_usd``.
    """
    with patch.object(
        payments,
        "_query_min_amount",
        AsyncMock(side_effect=[None, None]),
    ):
        result = await payments.get_min_amount_usd(
            "btc", attempted_usd=5.0
        )
    assert result is None
