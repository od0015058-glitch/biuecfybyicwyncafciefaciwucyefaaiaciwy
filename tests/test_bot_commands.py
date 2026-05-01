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


def _registered_admin_commands_in_admin_py() -> set[str]:
    """Scan ``admin.py`` for every ``@router.message(Command("..."))``
    decorator and return the bare command names. Keeps this regression
    test source-of-truth-driven so a future PR that adds a new
    ``Command("admin_foo")`` handler but forgets to advertise it in
    :data:`bot_commands.ADMIN_ONLY_COMMANDS` fails this test
    immediately instead of shipping a hidden command.

    Implemented with a regex against the source rather than importing
    ``admin`` and walking the router because the router introspection
    surface differs across aiogram minor versions and the source
    decorators are stable.
    """
    import pathlib
    import re

    admin_path = pathlib.Path(__file__).resolve().parent.parent / "admin.py"
    source = admin_path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'@router\.message\(\s*Command\(\s*"([A-Za-z0-9_]+)"\s*\)\s*\)'
    )
    return set(pattern.findall(source))


def test_every_registered_admin_command_is_in_the_slash_menu():
    """Every ``Command("admin_*")`` handler registered in ``admin.py``
    must have a matching entry in :data:`ADMIN_ONLY_COMMANDS` so it
    shows up in the per-admin slash menu.

    Pre-fix (Stage-15-Step-E #5): the role-system PR added handlers
    for ``/admin_role_grant``, ``/admin_role_revoke``,
    ``/admin_role_list`` but didn't update this module, so admins
    typing ``/`` in the bot chat saw every other admin command except
    the three new ones. The handlers still worked (the dispatcher
    gates on ``admin.is_admin``, not on what's published) but the
    autocomplete entries were silently missing — the operator had to
    type the full command name from memory. This test reads
    ``admin.py`` and pins every registered ``admin_*`` handler against
    :data:`ADMIN_ONLY_COMMANDS`.
    """
    registered = _registered_admin_commands_in_admin_py()
    advertised = {c for c, _ in ADMIN_ONLY_COMMANDS}
    # Filter the registered set to admin-prefixed commands so a
    # public ``Command("start")`` handler in ``admin.py`` (none today,
    # but the predicate stays robust for the future) doesn't break
    # this test.
    registered_admin_only = {c for c in registered if c.startswith("admin")}
    missing = registered_admin_only - advertised
    assert not missing, (
        "Admin commands registered in admin.py but missing from "
        "bot_commands.ADMIN_ONLY_COMMANDS: "
        f"{sorted(missing)}. Add them so the slash menu matches the "
        "set of installed handlers."
    )


def test_every_advertised_admin_command_has_a_handler():
    """Reverse direction: every entry in :data:`ADMIN_ONLY_COMMANDS`
    must have a matching ``Command("...")`` handler in ``admin.py``.

    Catches a typo in the slash-menu entry (e.g. ``admin_role_revoek``
    instead of ``admin_role_revoke``) — Telegram would advertise the
    typo'd command in the autocomplete menu and the click would fall
    through to no handler. The user would type ``/admin_role_revoek``
    and the bot would silently ignore it (or reply with the
    unknown-command flash if one is configured).
    """
    registered = _registered_admin_commands_in_admin_py()
    advertised = {c for c, _ in ADMIN_ONLY_COMMANDS}
    # The bare ``admin`` hub command is always present; everything
    # else should be admin-prefixed and have a matching handler.
    advertised_admin_only = {c for c in advertised if c.startswith("admin")}
    orphans = advertised_admin_only - registered
    assert not orphans, (
        "Admin commands advertised in bot_commands.ADMIN_ONLY_COMMANDS "
        "but with no matching @router.message(Command(...)) handler "
        f"in admin.py: {sorted(orphans)}. Either add the handler or "
        "drop the menu entry."
    )


def test_admin_role_commands_are_advertised():
    """Direct regression pin for the Stage-15-Step-E #5 bug fix.

    Even if the source-scan helpers above break in some future
    refactor, this test ensures the three role-CRUD commands stay in
    the published slash menu by name.
    """
    advertised = {c for c, _ in ADMIN_ONLY_COMMANDS}
    for cmd in ("admin_role_grant", "admin_role_revoke", "admin_role_list"):
        assert cmd in advertised, (
            f"/{cmd} is implemented in admin.py but not advertised "
            "in the per-admin slash menu — admins typing '/' won't "
            "see it in autocomplete."
        )


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
