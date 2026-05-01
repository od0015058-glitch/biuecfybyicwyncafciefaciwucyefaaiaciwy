"""Tests for Stage-10-Step-C: ``model_discovery``.

Covers:

* ``_compute_discovery`` diff logic (bootstrap, no-op, new-models,
  notify-allowlist filtering).
* ``_format_notification`` rendering (cap + overflow footer).
* ``notify_admins`` fan-out (per-admin fault isolation, empty
  admin-set short-circuit, empty-notified short-circuit).
* ``run_discovery_pass`` integration through mocked ``force_refresh``
  and the database layer.
* ``discover_new_models_loop`` cancellation & error swallowing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

import model_discovery
from models_catalog import CatalogModel
from pricing import ModelPrice


def _cm(model_id: str, name: str | None = None) -> CatalogModel:
    """Tiny factory — the discovery module only reads ``.id``,
    ``.name``, and ``.provider``, so we can hand it a dummy price.
    """
    provider, _, _ = model_id.partition("/")
    return CatalogModel(
        id=model_id,
        name=name or model_id,
        provider=provider,
        price=ModelPrice(input_per_1m_usd=1.0, output_per_1m_usd=2.0),
    )


# ---------------------------------------------------------------------
# _compute_discovery
# ---------------------------------------------------------------------


async def test_compute_discovery_bootstrap_suppresses_notifications():
    """First-ever run (prior_seen empty): every live model is
    'newly seen' for the DB, but nothing goes to the notifier —
    otherwise admins would get spammed with 200+ models on deploy.
    """
    live = (_cm("openai/gpt-4o"), _cm("anthropic/claude-3.5-sonnet"))
    result = await model_discovery._compute_discovery(
        live_models=live, prior_seen=set()
    )
    assert result.bootstrap is True
    assert result.total_live_models == 2
    assert result.newly_seen_ids == {"openai/gpt-4o", "anthropic/claude-3.5-sonnet"}
    assert result.notified_models == ()


async def test_compute_discovery_no_new_models_returns_empty_diff():
    live = (_cm("openai/gpt-4o"),)
    result = await model_discovery._compute_discovery(
        live_models=live, prior_seen={"openai/gpt-4o"}
    )
    assert result.bootstrap is False
    assert result.newly_seen_ids == frozenset()
    assert result.notified_models == ()


async def test_compute_discovery_new_model_in_allowlist_notifies():
    """A new model in an allowlisted provider shows up in
    ``notified_models``; the existing one is filtered out."""
    live = (
        _cm("openai/gpt-4o"),
        _cm("openai/gpt-5-mini", "OpenAI: GPT-5 Mini"),
    )
    result = await model_discovery._compute_discovery(
        live_models=live, prior_seen={"openai/gpt-4o"}
    )
    assert result.bootstrap is False
    assert result.newly_seen_ids == {"openai/gpt-5-mini"}
    assert len(result.notified_models) == 1
    assert result.notified_models[0].id == "openai/gpt-5-mini"


async def test_compute_discovery_new_model_outside_allowlist_records_but_does_not_notify(
    monkeypatch,
):
    """Long-tail provider (e.g. ``fake-provider``) is recorded in
    newly_seen_ids so we don't re-diff it forever, but omitted from
    notified_models so admins don't get DM'd about 50 Meta variants.
    """
    # Default allowlist: openai / anthropic / google / x-ai / deepseek.
    live = (
        _cm("fake-provider/weird-model"),
        _cm("google/gemini-new-thing"),
    )
    result = await model_discovery._compute_discovery(
        live_models=live, prior_seen={"openai/gpt-4o"}
    )
    assert result.newly_seen_ids == {
        "fake-provider/weird-model",
        "google/gemini-new-thing",
    }
    # Only the google one gets notified.
    assert len(result.notified_models) == 1
    assert result.notified_models[0].id == "google/gemini-new-thing"


async def test_compute_discovery_wildcard_allowlist_notifies_everything(
    monkeypatch,
):
    """Setting ``ADMIN_NOTIFY_DISCOVERY_PROVIDERS=*`` disables the
    allowlist — every new model triggers a notification. Regression
    guard for operators who want the full firehose."""
    monkeypatch.setattr(model_discovery, "_NOTIFY_PROVIDERS", frozenset())
    live = (_cm("fake-provider/thing"), _cm("meta-llama/llama-4"))
    result = await model_discovery._compute_discovery(
        live_models=live, prior_seen={"openai/gpt-4o"}
    )
    assert len(result.notified_models) == 2


# ---------------------------------------------------------------------
# _format_notification
# ---------------------------------------------------------------------


def test_format_notification_renders_all_below_cap(monkeypatch):
    monkeypatch.setattr(model_discovery, "_MAX_NEW_MODELS_PER_NOTIFICATION", 10)
    notified = (
        _cm("openai/gpt-5", "OpenAI: GPT-5"),
        _cm("anthropic/claude-4", "Anthropic: Claude 4"),
    )
    text = model_discovery._format_notification(notified)
    assert "Discovered 2 new OpenRouter model(s)" in text
    assert "openai/gpt-5 — OpenAI: GPT-5" in text
    assert "anthropic/claude-4 — Anthropic: Claude 4" in text
    assert "more" not in text  # no overflow footer


def test_format_notification_caps_and_appends_overflow(monkeypatch):
    monkeypatch.setattr(model_discovery, "_MAX_NEW_MODELS_PER_NOTIFICATION", 2)
    notified = tuple(_cm(f"openai/model-{i}") for i in range(5))
    text = model_discovery._format_notification(notified)
    assert "Discovered 5 new OpenRouter model(s)" in text
    assert "openai/model-0" in text
    assert "openai/model-1" in text
    # Model-2 onwards collapsed into footer.
    assert "openai/model-2" not in text
    assert "…and 3 more" in text


# ---------------------------------------------------------------------
# notify_admins
# ---------------------------------------------------------------------


async def test_notify_admins_empty_notified_short_circuits():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({1})):
        sent = await model_discovery.notify_admins(bot, notified=())
    assert sent == 0
    bot.send_message.assert_not_called()


async def test_notify_admins_no_admins_configured_short_circuits():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset()):
        sent = await model_discovery.notify_admins(
            bot, notified=(_cm("openai/gpt-5"),)
        )
    assert sent == 0
    bot.send_message.assert_not_called()


async def test_notify_admins_dispatches_to_every_admin():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({10, 20, 30})):
        sent = await model_discovery.notify_admins(
            bot, notified=(_cm("openai/gpt-5"),)
        )
    assert sent == 3
    assert bot.send_message.await_count == 3
    ids_called = {call.args[0] for call in bot.send_message.await_args_list}
    assert ids_called == {10, 20, 30}


async def test_notify_admins_tolerates_forbidden_from_one_admin():
    """Admin who blocked the bot must not poison the notification
    for the other admins. Returns the count of successful sends.
    """
    bot = MagicMock()

    async def side_effect(admin_id, text, **kwargs):
        if admin_id == 20:
            raise TelegramForbiddenError(method=None, message="blocked")
        return None

    bot.send_message = AsyncMock(side_effect=side_effect)
    with patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({10, 20, 30})):
        sent = await model_discovery.notify_admins(
            bot, notified=(_cm("openai/gpt-5"),)
        )
    # 10 + 30 succeeded, 20 was blocked.
    assert sent == 2


async def test_notify_admins_tolerates_api_error_from_one_admin():
    """Transient Telegram 5xx on one admin must not bubble out."""
    bot = MagicMock()

    async def side_effect(admin_id, text, **kwargs):
        if admin_id == 10:
            raise TelegramAPIError(method=None, message="boom")
        return None

    bot.send_message = AsyncMock(side_effect=side_effect)
    with patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({10, 20})):
        sent = await model_discovery.notify_admins(
            bot, notified=(_cm("openai/gpt-5"),)
        )
    assert sent == 1


# ---------------------------------------------------------------------
# run_discovery_pass
# ---------------------------------------------------------------------


async def _fake_catalog(models):
    cat = MagicMock()
    cat.models = tuple(models)
    return cat


async def test_run_discovery_pass_bootstrap_records_but_does_not_notify():
    """End-to-end bootstrap: empty seen-set, 2 live models, result
    records 2 ids and sends 0 DMs."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    live = [_cm("openai/gpt-4o"), _cm("anthropic/claude-3.5-sonnet")]

    with (
        patch.object(
            model_discovery,
            "force_refresh",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(model_discovery.db, "get_seen_model_ids", AsyncMock(return_value=set())),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=2)
        ) as record_mock,
        patch.object(
            model_discovery.db, "get_model_prices", AsyncMock(return_value={})
        ),
        patch.object(
            model_discovery.db, "upsert_model_prices", AsyncMock(return_value=2)
        ),
        patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({42})),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.bootstrap is True
    assert result.notified_models == ()
    assert result.price_deltas == ()
    bot.send_message.assert_not_called()
    # The ids we recorded must match the live set verbatim.
    (args, _) = record_mock.call_args
    recorded = set(args[0])
    assert recorded == {"openai/gpt-4o", "anthropic/claude-3.5-sonnet"}


async def test_run_discovery_pass_notifies_on_new_prominent_model():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    live = [_cm("openai/gpt-4o"), _cm("openai/gpt-5-mini", "GPT-5 Mini")]
    prior = {"openai/gpt-4o"}

    with (
        patch.object(
            model_discovery,
            "force_refresh",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(
            model_discovery.db, "get_seen_model_ids", AsyncMock(return_value=prior)
        ),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=1)
        ),
        patch.object(
            model_discovery.db,
            "get_model_prices",
            AsyncMock(return_value={"openai/gpt-4o": (1.0, 2.0)}),
        ),
        patch.object(
            model_discovery.db, "upsert_model_prices", AsyncMock(return_value=2)
        ),
        patch.object(
            model_discovery, "get_admin_user_ids", return_value=frozenset({42})
        ),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.bootstrap is False
    assert len(result.notified_models) == 1
    assert result.price_deltas == ()  # existing model's price was a match
    bot.send_message.assert_awaited_once()
    call_args = bot.send_message.await_args
    assert call_args.args[0] == 42
    assert "openai/gpt-5-mini" in call_args.args[1]


async def test_run_discovery_pass_no_new_models_skips_db_write_and_notify():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    live = [_cm("openai/gpt-4o")]

    with (
        patch.object(
            model_discovery,
            "force_refresh",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(
            model_discovery.db,
            "get_seen_model_ids",
            AsyncMock(return_value={"openai/gpt-4o"}),
        ),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=0)
        ) as record_mock,
        patch.object(
            model_discovery.db,
            "get_model_prices",
            AsyncMock(return_value={"openai/gpt-4o": (1.0, 2.0)}),
        ),
        patch.object(
            model_discovery.db, "upsert_model_prices", AsyncMock(return_value=1)
        ),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.newly_seen_ids == frozenset()
    assert result.price_deltas == ()
    bot.send_message.assert_not_called()
    record_mock.assert_not_called()


# ---------------------------------------------------------------------
# discover_new_models_loop
# ---------------------------------------------------------------------


async def test_discover_new_models_loop_cancels_cleanly():
    """Spawning and cancelling the forever-loop must not raise."""
    import asyncio

    bot = MagicMock()

    async def fast_pass(_bot):
        # Return immediately so the loop iterates quickly.
        pass

    with patch.object(model_discovery, "run_discovery_pass", AsyncMock(side_effect=fast_pass)):
        task = asyncio.create_task(
            model_discovery.discover_new_models_loop(bot, interval_seconds=0)
        )
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_discover_new_models_loop_swallows_pass_exceptions():
    """A crashing discovery pass must NOT kill the forever-loop —
    otherwise a transient OpenRouter 503 takes the loop off the air
    until the next deploy."""
    import asyncio

    bot = MagicMock()
    call_count = 0

    async def sometimes_crash(_bot):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("simulated transient failure")

    with patch.object(
        model_discovery, "run_discovery_pass", AsyncMock(side_effect=sometimes_crash)
    ):
        task = asyncio.create_task(
            model_discovery.discover_new_models_loop(bot, interval_seconds=0)
        )
        # Give the loop a few iterations to cycle through the crashes
        # and at least one success.
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # At least 3 passes happened — the loop survived the exceptions.
    assert call_count >= 3


# ---------------------------------------------------------------------
# database.record_seen_models / get_seen_model_ids SQL shape
# ---------------------------------------------------------------------


async def test_record_seen_models_uses_on_conflict_do_nothing():
    """The insert must use ON CONFLICT so concurrent discovery passes
    across worker processes can't crash on duplicate keys."""
    from unittest.mock import MagicMock

    import database as database_module

    # Tiny ``asyncpg.pool``-lookalike (same pattern as
    # test_database_queries.py).
    class _Ctx:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Ctx(self.conn)

    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[{"model_id": "openai/x"}])
    database_module.db.pool = _Pool(conn)

    inserted = await database_module.db.record_seen_models(
        ["openai/x", "anthropic/y"]
    )
    assert inserted == 1
    conn.fetch.assert_awaited_once()
    sql = conn.fetch.await_args.args[0]
    # The pin: ON CONFLICT DO NOTHING must be in the query.
    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql


async def test_record_seen_models_empty_input_short_circuits():
    """Don't hit the DB for an empty iterable — avoids a useless
    round-trip and a possible asyncpg complaint about a zero-length
    array parameter."""
    import database as database_module

    class _FailingPool:
        def acquire(self):
            pytest.fail("record_seen_models should NOT acquire a connection for empty input")

    database_module.db.pool = _FailingPool()
    inserted = await database_module.db.record_seen_models([])
    assert inserted == 0


# ---------------------------------------------------------------------
# _compute_price_deltas + Step-D end-to-end
# ---------------------------------------------------------------------


def _cm_priced(model_id: str, input_per_1m: float, output_per_1m: float) -> CatalogModel:
    provider, _, _ = model_id.partition("/")
    return CatalogModel(
        id=model_id,
        name=model_id,
        provider=provider,
        price=ModelPrice(
            input_per_1m_usd=input_per_1m, output_per_1m_usd=output_per_1m
        ),
    )


def test_compute_price_deltas_flags_above_threshold():
    """A 40% input-side move with threshold=20% must land in the
    output tuple."""
    live = (_cm_priced("openai/gpt-5", 1.4, 2.0),)
    prior = {"openai/gpt-5": (1.0, 2.0)}
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert len(deltas) == 1
    d = deltas[0]
    assert d.model_id == "openai/gpt-5"
    assert d.input_delta_pct == pytest.approx(40.0)
    assert d.output_delta_pct == pytest.approx(0.0)


def test_compute_price_deltas_ignores_moves_below_threshold():
    """A 5% wobble when threshold=20% must NOT trigger an alert —
    OpenRouter re-denominates tokens-per-dollar occasionally and we
    don't want a 0.5¢ adjustment to spam every admin."""
    live = (_cm_priced("openai/gpt-5", 1.05, 2.0),)
    prior = {"openai/gpt-5": (1.0, 2.0)}
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert deltas == ()


def test_compute_price_deltas_ignores_models_with_no_prior():
    """A model we've never priced before belongs to the Step-C
    new-model path, not the Step-D delta path. Otherwise a first
    deploy that populates ``model_prices`` would emit a 200-row
    delta DM for every model — the exact anti-pattern the Step-C
    bootstrap was designed to prevent."""
    live = (_cm_priced("openai/gpt-5", 99.0, 99.0),)
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices={}, threshold_pct=20.0
    )
    assert deltas == ()


def test_compute_price_deltas_skips_zero_prior_side():
    """A model that was previously ``$0`` on a side (free tier) and
    is now paid produces an undefined percent change — skip it
    rather than crash with ZeroDivisionError. The new-model /
    non-free transition is rare; if it does happen, the operator
    can spot it in the price-upsert logs."""
    live = (_cm_priced("meta-llama/llama", 1.0, 1.0),)
    prior = {"meta-llama/llama": (0.0, 0.0)}
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert deltas == ()


def test_compute_price_deltas_catches_output_side_moves():
    """Both sides are evaluated independently — an input price that
    stays flat with a big output-side move must still fire."""
    live = (_cm_priced("anthropic/claude-4", 3.0, 15.0),)
    prior = {"anthropic/claude-4": (3.0, 10.0)}
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert len(deltas) == 1
    assert deltas[0].input_delta_pct == pytest.approx(0.0)
    assert deltas[0].output_delta_pct == pytest.approx(50.0)


def test_compute_price_deltas_detects_price_cuts():
    """A negative move (upstream cut) is also worth surfacing so
    the operator can recompute average margin."""
    live = (_cm_priced("openai/gpt-5", 0.5, 1.0),)
    prior = {"openai/gpt-5": (1.0, 1.0)}
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert len(deltas) == 1
    assert deltas[0].input_delta_pct == pytest.approx(-50.0)


def test_compute_price_deltas_sorts_by_largest_absolute_move_first():
    """When multiple models move, the biggest swing should be at
    the top of the DM so the operator sees it without scrolling."""
    live = (
        _cm_priced("openai/a", 1.25, 1.0),  # +25%
        _cm_priced("openai/b", 2.0, 1.0),  # +100%
        _cm_priced("openai/c", 1.5, 1.0),  # +50%
    )
    prior = {
        "openai/a": (1.0, 1.0),
        "openai/b": (1.0, 1.0),
        "openai/c": (1.0, 1.0),
    }
    deltas = model_discovery._compute_price_deltas(
        live_models=live, prior_prices=prior, threshold_pct=20.0
    )
    assert [d.model_id for d in deltas] == ["openai/b", "openai/c", "openai/a"]


def test_format_price_delta_notification_renders_arrows_and_percents():
    deltas = (
        model_discovery.PriceDelta(
            model_id="openai/gpt-5",
            old_input_per_1m_usd=1.0,
            new_input_per_1m_usd=1.5,
            old_output_per_1m_usd=2.0,
            new_output_per_1m_usd=2.0,
            input_delta_pct=50.0,
            output_delta_pct=0.0,
        ),
    )
    text = model_discovery._format_price_delta_notification(deltas, 20.0)
    assert "moved price by more than 20%" in text
    assert "openai/gpt-5" in text
    assert "↑50.0%" in text
    # The unchanged output side must render as a flat arrow, not
    # ``↓0.0%`` — that would imply a downward move where none exists.
    assert "→0.0%" in text


def test_format_price_delta_notification_caps_and_adds_overflow(monkeypatch):
    monkeypatch.setattr(model_discovery, "_MAX_NEW_MODELS_PER_NOTIFICATION", 2)
    deltas = tuple(
        model_discovery.PriceDelta(
            model_id=f"openai/m{i}",
            old_input_per_1m_usd=1.0,
            new_input_per_1m_usd=2.0,
            old_output_per_1m_usd=1.0,
            new_output_per_1m_usd=1.0,
            input_delta_pct=100.0,
            output_delta_pct=0.0,
        )
        for i in range(5)
    )
    text = model_discovery._format_price_delta_notification(deltas, 20.0)
    assert "openai/m0" in text
    assert "openai/m1" in text
    assert "openai/m2" not in text
    assert "…and 3 more" in text


async def test_notify_admins_of_price_deltas_fans_out_and_isolates_failures():
    bot = MagicMock()

    async def side_effect(admin_id, text, **kwargs):
        if admin_id == 20:
            raise TelegramForbiddenError(method=None, message="blocked")
        return None

    bot.send_message = AsyncMock(side_effect=side_effect)
    d = model_discovery.PriceDelta(
        model_id="openai/gpt-5",
        old_input_per_1m_usd=1.0,
        new_input_per_1m_usd=2.0,
        old_output_per_1m_usd=1.0,
        new_output_per_1m_usd=1.0,
        input_delta_pct=100.0,
        output_delta_pct=0.0,
    )
    with patch.object(
        model_discovery, "get_admin_user_ids", return_value=frozenset({10, 20, 30})
    ):
        sent = await model_discovery.notify_admins_of_price_deltas(
            bot, (d,), threshold_pct=20.0
        )
    assert sent == 2


async def test_run_discovery_pass_fires_delta_dm_alongside_new_model_dm():
    """End-to-end: a pass that simultaneously discovers a new model
    AND sees a big price move on an existing one produces two
    independent DMs (one per code path)."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    live = [
        _cm_priced("openai/gpt-4o", 3.0, 20.0),  # price MOVED: +50% / +100%
        _cm_priced("openai/gpt-5-mini", 1.0, 2.0),  # NEW model
    ]
    prior_seen = {"openai/gpt-4o"}
    prior_prices = {"openai/gpt-4o": (2.0, 10.0)}

    with (
        patch.object(
            model_discovery,
            "force_refresh",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(
            model_discovery.db,
            "get_seen_model_ids",
            AsyncMock(return_value=prior_seen),
        ),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=1)
        ),
        patch.object(
            model_discovery.db,
            "get_model_prices",
            AsyncMock(return_value=prior_prices),
        ),
        patch.object(
            model_discovery.db, "upsert_model_prices", AsyncMock(return_value=2)
        ) as upsert_mock,
        patch.object(
            model_discovery, "get_admin_user_ids", return_value=frozenset({42})
        ),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    # Two DMs: one new-model, one price-delta.
    assert bot.send_message.await_count == 2
    sent_bodies = [call.args[1] for call in bot.send_message.await_args_list]
    assert any("Discovered 1 new OpenRouter" in body for body in sent_bodies)
    assert any("moved price by more than 20%" in body for body in sent_bodies)
    # Prices upserted for every live model (new + existing).
    upsert_mock.assert_awaited_once()
    (args, _) = upsert_mock.call_args
    assert set(args[0].keys()) == {"openai/gpt-4o", "openai/gpt-5-mini"}
    assert result.price_deltas[0].model_id == "openai/gpt-4o"


# ---------------------------------------------------------------------
# database.upsert_model_prices / get_model_prices SQL shape
# ---------------------------------------------------------------------


async def test_upsert_model_prices_uses_single_query_and_on_conflict():
    """Bulk upsert must be a single INSERT with ON CONFLICT DO
    UPDATE — otherwise a 200-model catalog pays 200 round-trips."""
    import database as database_module

    class _Ctx:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Ctx(self.conn)

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    database_module.db.pool = _Pool(conn)

    n = await database_module.db.upsert_model_prices(
        {"openai/x": (1.0, 2.0), "anthropic/y": (3.0, 4.0)}
    )
    assert n == 2
    conn.execute.assert_awaited_once()
    sql = conn.execute.await_args.args[0]
    assert "ON CONFLICT" in sql
    assert "DO UPDATE" in sql
    assert "model_prices" in sql


async def test_upsert_model_prices_empty_short_circuits():
    import database as database_module

    class _FailingPool:
        def acquire(self):
            pytest.fail("upsert_model_prices should NOT acquire for empty input")

    database_module.db.pool = _FailingPool()
    n = await database_module.db.upsert_model_prices({})
    assert n == 0


# ---------------------------------------------------------------------
# _parse_float_env — NaN / Inf guard (Stage-15-Step-E #6 bundled bug fix)
# ---------------------------------------------------------------------


class TestParseFloatEnvNonFiniteGuard:
    """Guard against ``PRICE_ALERT_THRESHOLD_PERCENT=nan`` (or
    ``inf``) silently disabling the price-move alert.

    Pre-fix, ``float("nan")`` parsed successfully and propagated
    through to the call site, where ``abs(delta_pct) >= NaN`` is
    always ``False`` — every alert was silently dropped no matter
    how big the price move. With the guard the value falls back to
    the default 20% threshold and alerts fire normally. The same
    fix lives in ``fx_rates._parse_float_env``; this test class
    pins the discovery side independently so a regression in either
    module doesn't go unnoticed.
    """

    def test_nan_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_NAN", "nan")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_NAN", 20.0
        ) == 20.0

    def test_uppercase_nan_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_NAN_U", "NaN")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_NAN_U", 20.0
        ) == 20.0

    def test_inf_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_INF", "inf")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_INF", 20.0
        ) == 20.0

    def test_negative_inf_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_NEG_INF", "-inf")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_NEG_INF", 20.0
        ) == 20.0

    def test_finite_value_passes_through(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_OK", "15")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_OK", 20.0
        ) == 15.0

    def test_blank_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_BLANK", "")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_BLANK", 20.0
        ) == 20.0

    def test_garbage_falls_back_to_default(self, monkeypatch):
        import model_discovery
        monkeypatch.setenv("DISCOVERY_TEST_GARBAGE", "twenty-five")
        assert model_discovery._parse_float_env(
            "DISCOVERY_TEST_GARBAGE", 20.0
        ) == 20.0
