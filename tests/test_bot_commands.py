"""Tests for ``bot_commands.publish_bot_commands``.

Pre-fix the bot never called ``set_my_commands`` so Telegram kept
serving whatever was last typed into BotFather's ``Edit Commands``
panel — including stale commands the bot had no handlers for
(``/new``, ``/redo``, ``/img``, ``/version`` were observed live).
The new module overwrites that list on every startup.

We don't drive ``aiogram.Bot`` against a live Telegram session; we
patch ``set_my_commands`` and assert on the (commands, scope) pairs
it was called with.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
)

from bot_commands import (
    ADMIN_COMMANDS,
    ADMIN_ONLY_COMMANDS,
    PUBLIC_COMMANDS,
    publish_bot_commands,
)


def test_public_commands_are_a_subset_of_admin_commands():
    """Admins always see public commands too. Catches a refactor that
    accidentally drops /start from the admin scope."""
    public_set = {c for c, _ in PUBLIC_COMMANDS}
    admin_set = {c for c, _ in ADMIN_COMMANDS}
    assert public_set.issubset(admin_set)


def test_admin_only_commands_are_disjoint_from_public():
    """ADMIN_ONLY_COMMANDS is just the delta. If a future PR adds
    /admin_foo to BOTH lists by accident, dedupe it here."""
    public_set = {c for c, _ in PUBLIC_COMMANDS}
    admin_only_set = {c for c, _ in ADMIN_ONLY_COMMANDS}
    assert public_set & admin_only_set == set()


def test_no_legacy_commands_published():
    """Regression: /new /redo /img /version were the leftover
    BotFather entries that prompted this module. Make sure none of
    them sneak back in if a future hand types them into the tuples."""
    legacy = {"new", "redo", "img", "version"}
    for command, _description in ADMIN_COMMANDS:
        assert command not in legacy


def test_redeem_command_is_publicly_advertised():
    """The redeem command must be in the public scope so users see
    it in the slash menu without needing to be admin."""
    public_set = {c for c, _ in PUBLIC_COMMANDS}
    assert "redeem" in public_set


def test_start_command_is_publicly_advertised():
    public_set = {c for c, _ in PUBLIC_COMMANDS}
    assert "start" in public_set


@pytest.mark.asyncio
async def test_publish_bot_commands_no_admins_publishes_only_public():
    """An empty admin set still publishes the public scope.
    Verifies a deploy with ADMIN_USER_IDS unset still gets the
    canonical menu (no /admin* entries leak)."""
    bot = AsyncMock()
    await publish_bot_commands(bot, [])
    # One call: public scope.
    assert bot.set_my_commands.await_count == 1
    args, kwargs = bot.set_my_commands.await_args
    commands_arg = args[0] if args else kwargs.get("commands")
    scope = kwargs.get("scope")
    assert isinstance(scope, BotCommandScopeAllPrivateChats)
    commands_list = list(commands_arg)
    assert len(commands_list) == len(PUBLIC_COMMANDS)
    assert all(isinstance(c, BotCommand) for c in commands_list)
    published_names = {c.command for c in commands_list}
    assert published_names == {c for c, _ in PUBLIC_COMMANDS}


@pytest.mark.asyncio
async def test_publish_bot_commands_publishes_admin_scope_per_admin():
    """Each admin id gets a per-chat scope so non-admins never see
    /admin* in their slash menu."""
    bot = AsyncMock()
    admin_ids = [111, 222, 333]
    await publish_bot_commands(bot, admin_ids)
    # 1 public + 3 per-admin calls.
    assert bot.set_my_commands.await_count == 1 + len(admin_ids)
    # Collect the per-admin scope chat_ids.
    chat_id_scopes = []
    for call in bot.set_my_commands.await_args_list:
        scope = call.kwargs.get("scope")
        if isinstance(scope, BotCommandScopeChat):
            chat_id_scopes.append(scope.chat_id)
    assert sorted(chat_id_scopes) == sorted(admin_ids)


@pytest.mark.asyncio
async def test_publish_bot_commands_swallows_public_scope_failure():
    """A failure publishing the public scope must not raise — that
    would crash startup. We log and continue."""
    bot = AsyncMock()
    bot.set_my_commands.side_effect = RuntimeError("network down")
    # Should not raise.
    await publish_bot_commands(bot, [])


@pytest.mark.asyncio
async def test_publish_bot_commands_continues_after_one_admin_fails():
    """If admin #1 hasn't /start'd the bot yet (Telegram returns 400
    'chat not found') we still publish for admin #2."""
    bot = AsyncMock()
    call_count = {"n": 0}

    async def flaky(*args, **kwargs):
        call_count["n"] += 1
        # Public scope (call 1): succeed. Admin 1 (call 2): fail. Admin 2 (call 3): succeed.
        if call_count["n"] == 2:
            raise RuntimeError("chat not found")

    bot.set_my_commands.side_effect = flaky
    await publish_bot_commands(bot, [111, 222])
    # All 3 calls were made even though one raised.
    assert call_count["n"] == 3
