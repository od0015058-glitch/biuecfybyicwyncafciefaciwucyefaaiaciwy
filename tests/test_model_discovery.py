"""Tests for Stage-10-Step-C: ``model_discovery``.

Covers:

* ``_compute_discovery`` diff logic (bootstrap, no-op, new-models,
  notify-allowlist filtering).
* ``_format_notification`` rendering (cap + overflow footer).
* ``notify_admins`` fan-out (per-admin fault isolation, empty
  admin-set short-circuit, empty-notified short-circuit).
* ``run_discovery_pass`` integration through mocked ``get_catalog``
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
            "get_catalog",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(model_discovery.db, "get_seen_model_ids", AsyncMock(return_value=set())),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=2)
        ) as record_mock,
        patch.object(model_discovery, "get_admin_user_ids", return_value=frozenset({42})),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.bootstrap is True
    assert result.notified_models == ()
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
            "get_catalog",
            AsyncMock(return_value=await _fake_catalog(live)),
        ),
        patch.object(
            model_discovery.db, "get_seen_model_ids", AsyncMock(return_value=prior)
        ),
        patch.object(
            model_discovery.db, "record_seen_models", AsyncMock(return_value=1)
        ),
        patch.object(
            model_discovery, "get_admin_user_ids", return_value=frozenset({42})
        ),
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.bootstrap is False
    assert len(result.notified_models) == 1
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
            "get_catalog",
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
    ):
        result = await model_discovery.run_discovery_pass(bot)

    assert result.newly_seen_ids == frozenset()
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
