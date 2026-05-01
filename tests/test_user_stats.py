"""Tests for ``user_stats`` + ``Database.get_user_spending_summary``
+ ``handlers.hub_stats_handler`` (Stage-15-Step-E #2, first slice)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import database as database_module
from user_stats import format_stats_summary


# ---------------------------------------------------------------------
# Database helpers (mirrors test_database_queries.py)
# ---------------------------------------------------------------------


class _PoolStub:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        outer = self

        class _Ctx:
            async def __aenter__(self_inner):
                return outer.connection

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()


def _make_conn():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    return conn


def _attach_summary_fetches(
    conn,
    *,
    lifetime: dict | None,
    window: dict | None,
    top_models: list[dict],
):
    """Wire ``conn.fetchrow`` / ``conn.fetch`` so the three
    sub-queries inside ``get_user_spending_summary`` return the
    given fakes in order: lifetime row, window row, top-models
    rows."""
    fetchrow_returns = [lifetime, window]
    conn.fetchrow = AsyncMock(side_effect=fetchrow_returns)
    conn.fetch = AsyncMock(return_value=top_models)


# ---------------------------------------------------------------------
# Database.get_user_spending_summary
# ---------------------------------------------------------------------


async def test_summary_returns_zeros_when_no_rows():
    conn = _make_conn()
    _attach_summary_fetches(
        conn,
        lifetime={"calls": 0, "tokens": 0, "cost": 0},
        window={"calls": 0, "tokens": 0, "cost": 0},
        top_models=[],
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    out = await db.get_user_spending_summary(telegram_id=42)

    assert out == {
        "lifetime": {
            "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
        },
        "window_days": 30,
        "window": {
            "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
        },
        "top_models": [],
    }


async def test_summary_handles_none_rows_defensively():
    """``fetchrow`` returning ``None`` (shouldn't happen for
    SUM/COUNT but belt-and-suspenders) must not crash the
    formatter — return the zero shape instead."""
    conn = _make_conn()
    _attach_summary_fetches(
        conn, lifetime=None, window=None, top_models=[]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.get_user_spending_summary(telegram_id=42)
    assert out["lifetime"] == {
        "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
    }
    assert out["window"] == {
        "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
    }


async def test_summary_returns_populated_shape():
    conn = _make_conn()
    _attach_summary_fetches(
        conn,
        lifetime={"calls": 100, "tokens": 50_000, "cost": 1.234},
        window={"calls": 30, "tokens": 12_000, "cost": 0.456},
        top_models=[
            {"model": "openai/gpt-4o", "calls": 12, "cost": 0.30},
            {"model": "anthropic/claude-3-opus", "calls": 8, "cost": 0.10},
        ],
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.get_user_spending_summary(telegram_id=42)
    assert out["lifetime"]["total_calls"] == 100
    assert out["lifetime"]["total_tokens"] == 50_000
    assert out["lifetime"]["total_cost_usd"] == pytest.approx(1.234)
    assert out["window"]["total_calls"] == 30
    assert out["window_days"] == 30
    assert len(out["top_models"]) == 2
    assert out["top_models"][0] == {
        "model": "openai/gpt-4o", "calls": 12, "cost_usd": pytest.approx(0.30),
    }


async def test_summary_hard_codes_user_filter_in_every_subquery():
    """Defense-in-depth: a future caller mustn't be able to drop
    the user-scope. Every SQL string we issue MUST mention
    ``WHERE telegram_id = $1`` (or pass it through the cursor
    binding)."""
    conn = _make_conn()
    _attach_summary_fetches(
        conn,
        lifetime={"calls": 0, "tokens": 0, "cost": 0},
        window={"calls": 0, "tokens": 0, "cost": 0},
        top_models=[],
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    await db.get_user_spending_summary(telegram_id=42)

    sqls = [c.args[0] for c in conn.fetchrow.await_args_list]
    sqls += [c.args[0] for c in conn.fetch.await_args_list]
    for sql in sqls:
        assert "WHERE telegram_id = $1" in sql, (
            f"missing user-scope filter in {sql!r}"
        )

    binds = [c.args[1] for c in conn.fetchrow.await_args_list]
    binds += [c.args[1] for c in conn.fetch.await_args_list]
    for bind in binds:
        assert bind == 42


async def test_summary_refuses_non_positive_telegram_id():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.get_user_spending_summary(telegram_id=0)
    with pytest.raises(ValueError):
        await db.get_user_spending_summary(telegram_id=-7)
    with pytest.raises(ValueError):
        await db.get_user_spending_summary(telegram_id="42")  # type: ignore[arg-type]


async def test_summary_clamps_window_days():
    """``window_days`` is clamped to ``[1, 365]`` to keep a buggy
    caller from issuing a runaway interval."""
    conn = _make_conn()
    # All sub-queries return the same zero-shape regardless of
    # how many times they're called — the clamp test calls the
    # whole method three times.
    conn.fetchrow = AsyncMock(
        return_value={"calls": 0, "tokens": 0, "cost": 0}
    )
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    out = await db.get_user_spending_summary(telegram_id=1, window_days=99999)
    assert out["window_days"] == database_module.Database.USER_STATS_WINDOW_DAYS_MAX
    out = await db.get_user_spending_summary(telegram_id=1, window_days=0)
    assert out["window_days"] == 1
    out = await db.get_user_spending_summary(telegram_id=1, window_days=-3)
    assert out["window_days"] == 1


async def test_summary_clamps_top_models_limit():
    """The screen can't usefully render more than the cap; a
    caller passing 999 must not punch through to a huge LIMIT."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={"calls": 0, "tokens": 0, "cost": 0}
    )
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    await db.get_user_spending_summary(
        telegram_id=1, top_models_limit=999
    )
    # The LIMIT bound is the third positional arg of the top-models
    # fetch; assert it's clamped.
    bind3 = conn.fetch.await_args.args[3]
    assert bind3 == database_module.Database.USER_STATS_TOP_MODELS_LIMIT


# ---------------------------------------------------------------------
# user_stats.format_stats_summary (pure-function formatter)
# ---------------------------------------------------------------------


def _empty_snapshot() -> dict:
    return {
        "lifetime": {
            "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
        },
        "window_days": 30,
        "window": {
            "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
        },
        "top_models": [],
    }


def _populated_snapshot() -> dict:
    return {
        "lifetime": {
            "total_calls": 100,
            "total_tokens": 50_000,
            "total_cost_usd": 1.234,
        },
        "window_days": 30,
        "window": {
            "total_calls": 30,
            "total_tokens": 12_000,
            "total_cost_usd": 0.456,
        },
        "top_models": [
            {"model": "openai/gpt-4o", "calls": 12, "cost_usd": 0.30},
            {
                "model": "anthropic/claude-3-opus",
                "calls": 8,
                "cost_usd": 0.10,
            },
        ],
    }


def test_formatter_renders_title_in_both_languages():
    out_en = format_stats_summary(_populated_snapshot(), "en")
    out_fa = format_stats_summary(_populated_snapshot(), "fa")
    assert "Your usage stats" in out_en
    assert "آمار مصرف" in out_fa


def test_formatter_empty_state_renders_placeholder():
    out = format_stats_summary(_empty_snapshot(), "en")
    assert "No usage logged yet" in out
    # Empty state must NOT render the lifetime/window/models tables —
    # rendering "0 calls, 0 tokens, $0.0000" three times is hostile UX.
    assert "Lifetime totals" not in out
    assert "Top models" not in out


def test_formatter_populated_state_renders_all_sections():
    out = format_stats_summary(_populated_snapshot(), "en")
    assert "Lifetime totals" in out
    assert "Last 30 days" in out
    assert "Top models" in out
    # Numbers from each section show up.
    assert "100" in out  # lifetime calls
    assert "50,000" in out  # lifetime tokens (thousands separator)
    assert "$1.2340" in out  # lifetime cost (4 dp)
    assert "30" in out  # window calls
    assert "openai/gpt-4o" in out
    assert "anthropic/claude-3-opus" in out


def test_formatter_renders_balance_line_when_provided():
    out = format_stats_summary(
        _populated_snapshot(), "en", balance_usd=2.50
    )
    assert "$2.50" in out
    assert "Current balance" in out


def test_formatter_omits_balance_line_when_none():
    out = format_stats_summary(
        _populated_snapshot(), "en", balance_usd=None
    )
    assert "Current balance" not in out


def test_formatter_skips_nan_balance():
    """A corrupted balance must not render as ``$nan`` — same NaN
    defense as ``wallet_display.format_toman_annotation``."""
    out = format_stats_summary(
        _populated_snapshot(), "en", balance_usd=float("nan")
    )
    assert "Current balance" not in out
    assert "nan" not in out.lower()


def test_formatter_skips_inf_balance():
    out = format_stats_summary(
        _populated_snapshot(), "en", balance_usd=float("inf")
    )
    assert "Current balance" not in out


def test_formatter_truncates_long_model_names():
    """A pathologically long OpenRouter slug must not blow past the
    Telegram 4 KB message limit. The truncated line ends with an
    ellipsis and the visible width is exactly the cap."""
    long_name = "p" * 200
    snap = _populated_snapshot()
    snap["top_models"] = [
        {"model": long_name, "calls": 1, "cost_usd": 0.0001}
    ]
    out = format_stats_summary(snap, "en")
    assert long_name not in out  # raw 200-char slug must not appear
    assert "…" in out
    # The body line is "  1. `<truncated>` — 1 calls, $0.0001" and
    # the truncated slug should be exactly 50 chars (49 + ellipsis).
    import re
    m = re.search(r"`(p+…)`", out)
    assert m is not None
    assert len(m.group(1)) == 50


def test_formatter_skips_malformed_top_model_rows():
    """Rows missing the ``model`` field (or with a non-string id)
    must be silently dropped rather than rendered as broken
    lines."""
    snap = _populated_snapshot()
    snap["top_models"] = [
        {"model": None, "calls": 1, "cost_usd": 0.01},
        {"model": "", "calls": 1, "cost_usd": 0.01},
        {"calls": 1, "cost_usd": 0.01},  # no "model" key at all
        {"model": "real/model", "calls": 5, "cost_usd": 0.05},
    ]
    out = format_stats_summary(snap, "en")
    assert "real/model" in out
    assert "None" not in out
    # Only one rank line should render.
    import re
    rank_lines = re.findall(r"^\s+\d+\.", out, flags=re.MULTILINE)
    assert len(rank_lines) == 1


def test_formatter_handles_corrupt_aggregate_values():
    """A NaN cost or non-numeric token count from a buggy DB
    column must not crash the formatter — coerce to 0 / empty
    on the lifetime + window aggregates so the screen still
    renders.

    For the **top-models** list the policy is different (per the
    Stage-15-Step-E #2 follow-up): a row with non-finite cost is
    DROPPED rather than rendered as ``$0.0000`` next to a real
    model name, since silently coercing corruption to zero would
    misattribute the user's spend ("you spent $0 on model X" when
    in fact the value is just unrepresentable). The lifetime /
    window aggregates are still coerced because there's no row to
    drop — the alternative would be a 500 to the user.
    """
    snap = {
        "lifetime": {
            "total_calls": 5,
            "total_tokens": float("nan"),
            "total_cost_usd": float("nan"),
        },
        "window_days": 30,
        "window": {
            "total_calls": "five",  # type: ignore[dict-item]
            "total_tokens": None,
            "total_cost_usd": float("inf"),
        },
        "top_models": [
            {"model": "x/y", "calls": "boom", "cost_usd": float("nan")},
        ],
    }
    # type-ignored keys above mimic a corrupted DB row; the
    # formatter must not raise.
    out = format_stats_summary(snap, "en")
    assert "$0.0000" in out  # lifetime / window NaN cost coerced to $0.0000
    assert "nan" not in out.lower()
    assert "inf" not in out.lower()
    # Stage-15-Step-E #2 follow-up bundled bug fix: the top-models
    # row with a non-finite cost is now dropped (was: silently
    # rendered as "$0.0000" next to the model name, lying about
    # which model the spend went to).
    assert "x/y" not in out


def test_top_models_drops_non_finite_cost_rows():
    """Stage-15-Step-E #2 follow-up bundled bug fix.

    Pre-fix, ``_iter_top_models`` silently coerced a row's
    non-finite ``cost_usd`` to ``0.0`` via :func:`_safe_float`, so
    a corrupted aggregate (operator-injected bogus
    ``cost_deducted_usd`` values landing as ``Decimal('Infinity')``
    in ``SUM`` → asyncpg → ``float`` cast → ``inf``) showed up in
    the user-facing screen as "you spent $0.0000 on model X" — a
    confident lie.

    Post-fix the row is dropped entirely so the "top models" list
    shrinks rather than misattributes spend. Same policy applies
    to ``calls`` (a non-finite call count gets the row dropped
    too — it's also corruption).
    """
    snap = {
        "lifetime": {
            "total_calls": 100, "total_tokens": 1000,
            "total_cost_usd": 1.0,
        },
        "window_days": 30,
        "window": {
            "total_calls": 10, "total_tokens": 100,
            "total_cost_usd": 0.5,
        },
        "top_models": [
            {"model": "good/model", "calls": 5, "cost_usd": 0.10},
            # Corruption — must be skipped.
            {"model": "corrupt/cost", "calls": 7, "cost_usd": float("inf")},
            {"model": "corrupt/nan", "calls": 7, "cost_usd": float("nan")},
            {"model": "corrupt/calls", "calls": float("inf"), "cost_usd": 0.05},
            # Boolean cost / calls — also corruption (bool is a
            # subclass of int but rendering ``True`` as ``$1`` was
            # never the intent).
            {"model": "corrupt/bool", "calls": True, "cost_usd": False},
        ],
    }
    out = format_stats_summary(snap, "en")
    assert "good/model" in out
    assert "corrupt/cost" not in out
    assert "corrupt/nan" not in out
    assert "corrupt/calls" not in out
    assert "corrupt/bool" not in out
    # Only one row rendered → exactly one rank line in the top-models
    # block.
    import re
    rank_lines = re.findall(r"^\s+\d+\.", out, flags=re.MULTILINE)
    assert len(rank_lines) == 1


def test_formatter_does_not_render_negative_balance():
    """Defensive: a negative balance (shouldn't happen — wallet
    floor is 0 — but defensive against a broken refund path) is
    silently skipped rather than rendering as ``$-1.00``."""
    out = format_stats_summary(
        _populated_snapshot(), "en", balance_usd=-1.0
    )
    assert "Current balance" not in out


def test_formatter_window_days_renders_in_header():
    snap = _populated_snapshot()
    snap["window_days"] = 7
    out = format_stats_summary(snap, "en")
    assert "Last 7 days" in out
    assert "Top models (last 7 days)" in out


# ---------------------------------------------------------------------
# Wallet keyboard wiring + handler integration
# ---------------------------------------------------------------------


def _make_callback(user_id: int = 12345):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="alice"),
        message=msg,
        answer=AsyncMock(),
        data="hub_stats",
    )


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
        set_data=AsyncMock(),
        get_data=AsyncMock(return_value={}),
    )


def test_wallet_keyboard_includes_stats_button():
    """Regression: the wallet menu must surface the new
    ``hub_stats`` callback so users can reach the screen without
    knowing the slash command (which doesn't exist yet)."""
    from handlers import _build_wallet_keyboard

    builder = _build_wallet_keyboard("en")
    markup = builder.as_markup()
    flat = [b for row in markup.inline_keyboard for b in row]
    callbacks = [b.callback_data for b in flat]
    assert "hub_stats" in callbacks


async def test_hub_stats_handler_renders_populated_screen():
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": 5.00}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await hub_stats_handler(cb, state)

    cb.message.edit_text.assert_awaited_once()
    text = cb.message.edit_text.await_args.args[0]
    assert "Your usage stats" in text
    assert "openai/gpt-4o" in text
    assert "$5.00" in text  # balance
    cb.answer.assert_awaited_once()


async def test_hub_stats_handler_renders_empty_screen_for_zero_usage():
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    snap = _empty_snapshot()
    user_row = {"balance_usd": 0.0}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await hub_stats_handler(cb, state)

    text = cb.message.edit_text.await_args.args[0]
    assert "No usage logged yet" in text


async def test_hub_stats_handler_clears_fsm_state():
    """FSM clear is the same defensive policy as
    ``hub_receipts_handler`` / ``hub_wallet_handler`` — the
    wallet menu is reachable from inside the charge flows, so
    leaving any in-flight ``waiting_custom_amount`` /
    ``waiting_promo_code`` state would intercept the user's next
    free-text message."""
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    snap = _empty_snapshot()
    user_row = {"balance_usd": 0.0}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await hub_stats_handler(cb, state)

    state.clear.assert_awaited()


async def test_hub_stats_handler_survives_db_value_error():
    """A DB-layer ``ValueError`` (e.g. corrupted ``from_user.id``)
    must not 500 the user — render the empty-state screen instead."""
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    user_row = {"balance_usd": 0.0}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(side_effect=ValueError("bad telegram_id")),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await hub_stats_handler(cb, state)

    text = cb.message.edit_text.await_args.args[0]
    assert "No usage logged yet" in text
    cb.answer.assert_awaited_once()


async def test_hub_stats_handler_survives_nan_balance():
    """A corrupted ``balance_usd`` (NaN) on the user row must not
    render as ``$nan`` in the screen — same NaN guard as
    ``hub_wallet_handler``."""
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": float("nan")}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await hub_stats_handler(cb, state)

    text = cb.message.edit_text.await_args.args[0]
    assert "nan" not in text.lower()


def test_strings_keys_present_in_both_languages():
    """Smoke test: every new i18n key resolves in both ``fa`` and
    ``en``. ``strings.t`` falls through to ``en`` on a missing
    Persian key, so this catches a typo where we forgot to add
    the Persian translation."""
    from strings import _STRINGS  # type: ignore[attr-defined]

    new_keys = [
        "btn_my_stats",
        "stats_title",
        "stats_balance_line",
        "stats_empty",
        "stats_lifetime_header",
        "stats_lifetime_line",
        "stats_window_header",
        "stats_window_line",
        "stats_top_models_header",
        "stats_top_models_line",
        # Stage-15-Step-E #2 follow-up: new window-selector button.
        "stats_window_btn",
    ]
    fa = _STRINGS["fa"]
    en = _STRINGS["en"]
    for k in new_keys:
        assert k in fa, f"missing fa: {k}"
        assert k in en, f"missing en: {k}"


# ---------------------------------------------------------------------
# Stage-15-Step-E #2 follow-up: /stats slash command + window selector
# ---------------------------------------------------------------------


def _make_message(user_id: int = 12345, text: str = "/stats"):
    """Mock ``Message`` for the slash-command handler tests.

    Mirrors ``_make_callback`` but for the message-side path.
    ``answer`` is the AsyncMock the handler is expected to call.
    """
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="alice"),
        text=text,
        chat=SimpleNamespace(id=user_id),
        answer=AsyncMock(),
    )


async def test_cmd_stats_renders_fresh_message():
    """``/stats`` must call ``message.answer`` (fresh message)
    instead of the wallet-menu's ``edit_text`` (in-place edit) —
    typing a slash command into chat should land as its own
    bubble, not silently replace some scrolled-up older
    message."""
    from handlers import cmd_stats

    msg = _make_message()
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": 5.00}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await cmd_stats(msg, state)

    msg.answer.assert_awaited_once()
    text = msg.answer.await_args.args[0]
    assert "Your usage stats" in text
    assert "openai/gpt-4o" in text
    state.clear.assert_awaited()


async def test_cmd_stats_default_window_is_30_days():
    """Plain ``/stats`` (no arg) requests the 30-day window."""
    from handlers import cmd_stats

    msg = _make_message(text="/stats")
    state = _make_state()
    snap = _populated_snapshot()
    snap["window_days"] = 30
    user_row = {"balance_usd": 0.0}
    summary_mock = AsyncMock(return_value=snap)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary", new=summary_mock,
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await cmd_stats(msg, state)

    summary_mock.assert_awaited_once()
    kwargs = summary_mock.await_args.kwargs
    assert kwargs.get("window_days") == 30


async def test_cmd_stats_accepts_window_arg():
    """``/stats 7`` requests the 7-day window. Other recognised
    choices (90 / 365) work the same way."""
    from handlers import cmd_stats

    for arg in (7, 90, 365):
        msg = _make_message(text=f"/stats {arg}")
        state = _make_state()
        snap = _populated_snapshot()
        snap["window_days"] = arg
        user_row = {"balance_usd": 0.0}
        summary_mock = AsyncMock(return_value=snap)
        with patch(
            "handlers._get_user_language", new=AsyncMock(return_value="en")
        ), patch(
            "handlers.db.get_user_spending_summary", new=summary_mock,
        ), patch(
            "handlers.db.get_user", new=AsyncMock(return_value=user_row)
        ):
            await cmd_stats(msg, state)
        kwargs = summary_mock.await_args.kwargs
        assert kwargs.get("window_days") == arg, (
            f"expected window_days={arg} for arg {arg!r}, got {kwargs!r}"
        )


async def test_cmd_stats_silently_coerces_garbage_arg_to_default():
    """``/stats abc`` / ``/stats -7`` / ``/stats 99999`` all fall
    back to the 30-day default rather than 500-ing the user.

    Same forgiveness policy as the receipts pagination cursor —
    a fat-fingered arg should never break the screen.
    """
    from handlers import cmd_stats

    for bad in ("abc", "-7", "99999", "0"):
        msg = _make_message(text=f"/stats {bad}")
        state = _make_state()
        snap = _populated_snapshot()
        user_row = {"balance_usd": 0.0}
        summary_mock = AsyncMock(return_value=snap)
        with patch(
            "handlers._get_user_language", new=AsyncMock(return_value="en")
        ), patch(
            "handlers.db.get_user_spending_summary", new=summary_mock,
        ), patch(
            "handlers.db.get_user", new=AsyncMock(return_value=user_row)
        ):
            await cmd_stats(msg, state)
        kwargs = summary_mock.await_args.kwargs
        assert kwargs.get("window_days") == 30, (
            f"expected coerce-to-30 for {bad!r}, got {kwargs!r}"
        )


async def test_cmd_stats_skips_when_no_from_user():
    """``message.from_user is None`` (anonymous group admin /
    channel-bot edge case) must early-return — same defensive
    guard as ``cmd_start`` / ``cmd_redeem``."""
    from handlers import cmd_stats

    msg = _make_message()
    msg.from_user = None
    state = _make_state()
    summary_mock = AsyncMock()
    with patch(
        "handlers.db.get_user_spending_summary", new=summary_mock,
    ):
        await cmd_stats(msg, state)
    msg.answer.assert_not_awaited()
    summary_mock.assert_not_awaited()


async def test_cmd_stats_supports_at_bot_suffix():
    """``/stats@MyBot 7`` (the suffixed shape Telegram uses in
    group chats) parses as ``/stats 7`` — same shape support as
    ``cmd_redeem``."""
    from handlers import cmd_stats

    msg = _make_message(text="/stats@meowbot 7")
    state = _make_state()
    snap = _populated_snapshot()
    snap["window_days"] = 7
    user_row = {"balance_usd": 0.0}
    summary_mock = AsyncMock(return_value=snap)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary", new=summary_mock,
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await cmd_stats(msg, state)
    kwargs = summary_mock.await_args.kwargs
    assert kwargs.get("window_days") == 7


def test_stats_keyboard_includes_window_selector_buttons():
    """The stats screen must surface a 4-button window selector
    (7 / 30 / 90 / 365) so users can pivot to other windows
    without typing the slash command."""
    from handlers import _build_stats_keyboard

    builder = _build_stats_keyboard("en", window_days=30)
    markup = builder.as_markup()
    flat = [b for row in markup.inline_keyboard for b in row]
    callbacks = [b.callback_data for b in flat]
    assert "stats_window:7" in callbacks
    assert "stats_window:30" in callbacks
    assert "stats_window:90" in callbacks
    assert "stats_window:365" in callbacks
    # Plus the existing back-to-wallet + home buttons.
    assert "back_to_wallet" in callbacks
    assert "close_menu" in callbacks


def test_stats_keyboard_marks_selected_window_with_check():
    """The currently-selected window button is prefixed with
    ``✓`` so a user can read which window they're on without
    scrolling to the section header."""
    from handlers import _build_stats_keyboard

    for selected in (7, 30, 90, 365):
        builder = _build_stats_keyboard("en", window_days=selected)
        markup = builder.as_markup()
        flat = [b for row in markup.inline_keyboard for b in row]
        for b in flat:
            if b.callback_data == f"stats_window:{selected}":
                assert b.text.startswith("✓ "), (
                    f"selected={selected} button text {b.text!r} "
                    f"missing checkmark"
                )
            elif (b.callback_data or "").startswith("stats_window:"):
                assert not b.text.startswith("✓"), (
                    f"non-selected button text {b.text!r} "
                    f"has checkmark"
                )


def test_stats_keyboard_unknown_window_falls_back_to_30():
    """A stale callback-payload window value (e.g. an old client
    sending ``stats_window:14`` from a deploy that supported 14d
    but the current deploy doesn't) renders the keyboard with
    ``30d`` highlighted as the safe default."""
    from handlers import _build_stats_keyboard

    builder = _build_stats_keyboard("en", window_days=14)
    markup = builder.as_markup()
    flat = [b for row in markup.inline_keyboard for b in row]
    for b in flat:
        if b.callback_data == "stats_window:30":
            assert b.text.startswith("✓ ")


async def test_stats_window_select_handler_re_renders_with_new_window():
    """Clicking ``stats_window:7`` re-fetches the snapshot with
    ``window_days=7`` and edits the message in place — no fresh
    message bubble."""
    from handlers import stats_window_select_handler

    cb = _make_callback()
    cb.data = "stats_window:7"
    state = _make_state()
    snap = _populated_snapshot()
    snap["window_days"] = 7
    user_row = {"balance_usd": 5.00}
    summary_mock = AsyncMock(return_value=snap)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary", new=summary_mock,
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await stats_window_select_handler(cb, state)

    cb.message.edit_text.assert_awaited_once()
    cb.answer.assert_awaited_once()
    kwargs = summary_mock.await_args.kwargs
    assert kwargs.get("window_days") == 7


async def test_stats_window_select_handler_silently_recovers_from_garbage():
    """A malformed ``stats_window:abc`` payload (stale client /
    crafted callback) falls back to the 30d default rather than
    500-ing."""
    from handlers import stats_window_select_handler

    cb = _make_callback()
    cb.data = "stats_window:abc"
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": 0.0}
    summary_mock = AsyncMock(return_value=snap)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary", new=summary_mock,
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await stats_window_select_handler(cb, state)

    kwargs = summary_mock.await_args.kwargs
    assert kwargs.get("window_days") == 30
    cb.answer.assert_awaited_once()


async def test_stats_window_select_handler_swallows_message_not_modified():
    """Double-tap on the already-selected window button renders
    identical text + keyboard, which Telegram refuses to edit
    with ``message is not modified``. The handler must swallow
    that error rather than 500-ing the user."""
    from aiogram.exceptions import TelegramBadRequest
    from handlers import stats_window_select_handler

    cb = _make_callback()
    cb.data = "stats_window:30"
    cb.message.edit_text = AsyncMock(
        side_effect=TelegramBadRequest(
            method=MagicMock(), message="message is not modified"
        )
    )
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": 0.0}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await stats_window_select_handler(cb, state)

    cb.answer.assert_awaited_once()


async def test_stats_window_select_handler_clears_fsm_state():
    """Same defensive FSM clear as ``hub_stats_handler``: the
    wallet menu is reachable from inside charge / promo / gift
    flows."""
    from handlers import stats_window_select_handler

    cb = _make_callback()
    cb.data = "stats_window:30"
    state = _make_state()
    snap = _populated_snapshot()
    user_row = {"balance_usd": 0.0}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=snap),
    ), patch(
        "handlers.db.get_user", new=AsyncMock(return_value=user_row)
    ):
        await stats_window_select_handler(cb, state)
    state.clear.assert_awaited()


def test_coerce_stats_window_accepts_recognised_choices():
    """Every value in :data:`_STATS_WINDOW_CHOICES` round-trips."""
    from handlers import _coerce_stats_window, _STATS_WINDOW_CHOICES

    for choice in _STATS_WINDOW_CHOICES:
        assert _coerce_stats_window(choice) == choice
        assert _coerce_stats_window(str(choice)) == choice


def test_coerce_stats_window_falls_back_to_30():
    """Anything else (garbage str, negative, unknown int, None,
    bool) falls back to ``30``."""
    from handlers import _coerce_stats_window

    for bad in ("abc", "-7", "99999", "", "0", None, [], {}, "14"):
        assert _coerce_stats_window(bad) == 30, (
            f"expected 30 for {bad!r}"
        )


def test_hub_stats_keyboard_default_window_highlighted():
    """The wallet-menu entry-point (``hub_stats_handler``) lands
    on the 30d default — so the rendered keyboard must show 30d
    as the selected window."""
    from handlers import _build_stats_keyboard

    builder = _build_stats_keyboard("en", window_days=30)
    markup = builder.as_markup()
    flat = [b for row in markup.inline_keyboard for b in row]
    for b in flat:
        if b.callback_data == "stats_window:30":
            assert b.text.startswith("✓ ")
        elif (b.callback_data or "").startswith("stats_window:"):
            assert not b.text.startswith("✓")
