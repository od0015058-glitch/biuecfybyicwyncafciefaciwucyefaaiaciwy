"""Stage-11-Step-D: wallet balance display with the live USD→Toman annotation.

Covers:

* ``format_toman_annotation`` returns ``""`` when no FX snapshot is
  cached (cold-boot / prolonged source outage).
* Fresh snapshot renders the ``≈ N تومان`` line with thousands
  separators and no fractional digits.
* Stale snapshot (older than ``is_stale`` threshold) renders the
  ``(نرخ تقریبی)`` / ``(approx)`` marker so the user sees the figure
  is informational, not a quote.
* English locale uses ``TMN`` instead of ``تومان``.
* Non-finite balances (NaN / ±Inf) and balances that overflow when
  multiplied by the rate are rejected with an empty annotation, NOT
  a literal ``≈ nan تومان``.
* The hub-wallet and back-to-wallet handlers splice the annotation
  into ``wallet_text`` (smoke-tested via the public ``t`` lookup).
* Toman top-up entry uses the SAME snapshot for both the displayed
  rate and the computed USD figure (Stage-11-Step-D bundled bug
  fix). Pre-fix, ``handlers.convert_toman_to_usd`` and a separate
  ``handlers.get_usd_to_toman_snapshot`` could observe two
  different cache values.
"""

from __future__ import annotations

import math
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import fx_rates
from wallet_display import format_balance_block, format_toman_annotation


def _snap(rate: float = 100_000.0, *, age_seconds: float = 0.0, source: str = "test") -> fx_rates.FxRateSnapshot:
    """Build a snapshot whose ``fetched_at`` is ``age_seconds`` in the
    past. ``age_seconds=0`` -> brand-new (not stale)."""
    return fx_rates.FxRateSnapshot(
        toman_per_usd=rate,
        fetched_at=time.time() - age_seconds,
        source=source,
    )


# ---------------------------------------------------------------------
# format_toman_annotation: rendering rules
# ---------------------------------------------------------------------


def test_no_snapshot_returns_empty_string():
    assert format_toman_annotation("fa", 12.34, None) == ""
    assert format_toman_annotation("en", 12.34, None) == ""


def test_fresh_snapshot_renders_persian_line():
    out = format_toman_annotation("fa", 4.0, _snap(rate=100_000.0))
    # Leading newline so the caller can splice without a separator.
    assert out.startswith("\n")
    # 4 USD * 100 000 TMN/USD = 400 000 TMN
    assert "400,000" in out
    assert "تومان" in out
    assert "تقریبی" not in out


def test_fresh_snapshot_renders_english_line():
    out = format_toman_annotation("en", 4.0, _snap(rate=100_000.0))
    assert out.startswith("\n")
    assert "400,000" in out
    assert "TMN" in out
    assert "approx" not in out


def test_stale_snapshot_appends_approx_marker_persian():
    # is_stale uses 4 * FX_REFRESH_INTERVAL_SECONDS (default 600) =
    # 2400s. Force well past that.
    out = format_toman_annotation("fa", 4.0, _snap(rate=100_000.0, age_seconds=10_000))
    assert "400,000" in out
    assert "تقریبی" in out


def test_stale_snapshot_appends_approx_marker_english():
    out = format_toman_annotation("en", 4.0, _snap(rate=100_000.0, age_seconds=10_000))
    assert "400,000" in out
    assert "approx" in out.lower()


def test_zero_balance_renders_zero_toman():
    """A fresh wallet should still show the Toman line — the rate is
    known, the conversion is well-defined, the result is just zero."""
    out = format_toman_annotation("fa", 0.0, _snap(rate=100_000.0))
    assert out  # non-empty
    assert "0" in out
    assert "تومان" in out


def test_nan_balance_returns_empty_string():
    """Defense-in-depth: a corrupted balance must NOT render
    ``≈ nan تومان`` — it must collapse to empty so the wallet still
    renders only the USD figure (which has its own format spec)."""
    assert format_toman_annotation("fa", math.nan, _snap()) == ""
    assert format_toman_annotation("fa", math.inf, _snap()) == ""
    assert format_toman_annotation("fa", -math.inf, _snap()) == ""


def test_non_numeric_balance_returns_empty_string():
    # mypy would reject this; runtime defensive behaviour matters
    # because admin overrides / future call sites can slip non-floats
    # through.
    assert format_toman_annotation("fa", None, _snap()) == ""  # type: ignore[arg-type]
    assert format_toman_annotation("fa", "12.34", _snap()) == ""  # type: ignore[arg-type]


def test_extreme_magnitude_balance_returns_empty_string():
    """A finite balance times a finite rate could overflow at extreme
    magnitudes; we guard the product, not just the inputs."""
    huge = 1e308
    out = format_toman_annotation("fa", huge, _snap(rate=1e308))
    # 1e308 * 1e308 = inf -> rejected
    assert out == ""


def test_unknown_locale_falls_through_to_default():
    """Same fallback rule as :func:`strings.t`: an unknown lang
    renders the default locale's template (fa)."""
    out = format_toman_annotation("zz", 4.0, _snap(rate=100_000.0))
    assert "400,000" in out
    assert "تومان" in out  # fa is DEFAULT_LANGUAGE


# ---------------------------------------------------------------------
# format_balance_block: combined USD + Toman rendering
# ---------------------------------------------------------------------


def test_balance_block_no_snapshot():
    assert format_balance_block("fa", 12.34, None) == "$12.34"


def test_balance_block_fresh_snapshot_persian():
    block = format_balance_block("fa", 12.34, _snap(rate=100_000.0))
    head, tail = block.split("\n", 1)
    assert head == "$12.34"
    assert "1,234,000" in tail  # 12.34 * 100 000 = 1 234 000
    assert "تومان" in tail


def test_balance_block_fresh_snapshot_english():
    block = format_balance_block("en", 12.34, _snap(rate=100_000.0))
    head, tail = block.split("\n", 1)
    assert head == "$12.34"
    assert "1,234,000" in tail
    assert "TMN" in tail


# ---------------------------------------------------------------------
# Wallet handlers: ``wallet_text`` is templated with ``{toman_line}``
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _seed_fx_rate():
    fx_rates._cache = _snap(rate=100_000.0)
    yield
    fx_rates._reset_cache_for_tests()


def _make_callback(*, user_id: int = 42):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="test"),
        message=msg,
        answer=AsyncMock(),
        data="hub_wallet",
    )


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        get_data=AsyncMock(return_value={}),
        set_data=AsyncMock(),
        update_data=AsyncMock(),
        set_state=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_hub_wallet_handler_renders_toman_annotation():
    """The hub wallet view should pull the cached FX snapshot and
    render ``$X.YZ\\n≈ N تومان`` to the user."""
    from handlers import hub_wallet_handler

    callback = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value={"balance_usd": 4.0})
    ):
        await hub_wallet_handler(callback, state)

    rendered = callback.message.edit_text.await_args.args[0]
    assert "$4.00" in rendered
    assert "400,000" in rendered
    assert "تومان" in rendered


@pytest.mark.asyncio
async def test_hub_wallet_handler_no_rate_renders_usd_only():
    """When the FX cache is cold, the wallet still renders — just
    without the Toman annotation. No literal ``{toman_line}`` should
    leak to the user."""
    from handlers import hub_wallet_handler

    fx_rates._reset_cache_for_tests()  # blow the autouse fixture
    callback = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value={"balance_usd": 4.0})
    ), patch(
        "handlers.get_usd_to_toman_snapshot", new=AsyncMock(return_value=None)
    ):
        await hub_wallet_handler(callback, state)

    rendered = callback.message.edit_text.await_args.args[0]
    assert "$4.00" in rendered
    assert "{toman_line}" not in rendered  # literal placeholder must not leak
    assert "تومان" not in rendered  # no Toman line at all


@pytest.mark.asyncio
async def test_back_to_wallet_handler_renders_toman_annotation():
    from handlers import back_to_wallet_handler

    callback = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value={"balance_usd": 12.5})
    ):
        await back_to_wallet_handler(callback, state)

    rendered = callback.message.edit_text.await_args.args[0]
    assert "$12.50" in rendered
    # 12.5 * 100 000 = 1 250 000
    assert "1,250,000" in rendered
    assert "TMN" in rendered


# ---------------------------------------------------------------------
# Bundled bug fix: Toman entry uses ONE snapshot, not two
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toman_input_uses_single_snapshot_consistently(monkeypatch):
    """Pre-fix: ``process_toman_amount_input`` called
    ``convert_toman_to_usd`` (which read snapshot A) and then
    ``get_usd_to_toman_snapshot`` (which could read snapshot B if the
    background refresher rotated the cache between the two awaits).
    The (entered_toman, usd_amount, toman_rate_at_entry) triple
    became internally inconsistent — the user saw a USD figure
    computed at A but a rate label from B.

    Fix: read the snapshot ONCE and compute USD locally. The triple
    is now an algebraic identity: usd_amount * rate == entered_toman.
    """
    from handlers import process_toman_amount_input

    # Two different snapshots — first call returns A, every subsequent
    # call returns B. If the handler still reads twice, the test fails
    # because the stashed rate (B) won't match the rate used to
    # compute usd_amount (A).
    snap_a = _snap(rate=100_000.0)
    snap_b = _snap(rate=110_000.0)
    call_count = {"n": 0}

    async def _rotating_snapshot():
        call_count["n"] += 1
        return snap_a if call_count["n"] == 1 else snap_b

    msg = SimpleNamespace(
        from_user=SimpleNamespace(id=42, username="test"),
        text="400000",
        answer=AsyncMock(),
        chat=SimpleNamespace(id=42),
    )
    state = _make_state()

    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.get_usd_to_toman_snapshot", side_effect=_rotating_snapshot
    ):
        await process_toman_amount_input(msg, state)

    # Snapshot accessor should have been called exactly once. If the
    # handler regresses to two reads, this rises to 2 and fails.
    assert call_count["n"] == 1, (
        f"handler must read FX snapshot exactly once; observed {call_count['n']} reads"
    )

    # Verify the (entered_toman, custom_amount, toman_rate_at_entry)
    # triple is internally consistent. The stash should use snap_a's
    # rate (the only one read) — NOT snap_b's.
    state.update_data.assert_called_once()
    stashed = state.update_data.await_args.kwargs
    assert stashed["toman_entry"] == 400_000.0
    assert stashed["toman_rate_at_entry"] == snap_a.toman_per_usd
    # 400 000 / 100 000 = $4.00 (rounded to 2dp downstream)
    assert stashed["custom_amount"] == 4.0
