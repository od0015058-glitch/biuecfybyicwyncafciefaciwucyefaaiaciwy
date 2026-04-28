"""Canonical Telegram slash-command list publisher.

Telegram caches whatever was last entered into BotFather's ``Edit
Commands`` panel and shows it as the user-side ``/`` menu. If the
project never calls ``bot.set_my_commands(...)``, those leftover
entries (``/new``, ``/redo``, ``/img``, ``/version``, …) persist
forever even though the bot has zero handlers for them.

This module is the single source of truth for what shows up in the
``/`` menu. It overwrites BotFather's list on every startup with
``set_my_commands`` so the menu always matches the handlers we
actually ship.

Two scopes:

* :data:`PUBLIC_COMMANDS` — commands every user sees. Published at
  the global ``BotCommandScopeAllPrivateChats`` scope.
* :data:`ADMIN_COMMANDS` — public + admin-only ``/admin*`` commands,
  published per-admin at ``BotCommandScopeChat`` so non-admins never
  see them in the slash menu (security through obscurity is not the
  point — :func:`admin.is_admin` still gates handler execution — but
  not advertising commands the user can't run reduces noise).
"""

from __future__ import annotations

import logging
from typing import Iterable

from aiogram import Bot
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
)

log = logging.getLogger("bot.commands")


# (command, description) tuples. Description is what Telegram shows
# next to each entry in the ``/`` menu. Bilingual fa/en is fine — the
# Telegram client doesn't translate. Keep them short; Telegram caps
# descriptions at 256 chars but anything over ~50 looks ugly in the
# narrow popup.
PUBLIC_COMMANDS: tuple[tuple[str, str], ...] = (
    ("start", "🏠 منوی اصلی · Main menu"),
    ("redeem", "🎁 استفاده از کد هدیه · Redeem a gift code"),
)

ADMIN_ONLY_COMMANDS: tuple[tuple[str, str], ...] = (
    ("admin", "⚙️ Admin hub"),
    ("admin_metrics", "📊 System metrics"),
    ("admin_balance", "💰 Look up a user's balance"),
    ("admin_credit", "➕ Credit a user's wallet"),
    ("admin_debit", "➖ Debit a user's wallet"),
    ("admin_promo_create", "🎟️ Create a promo code"),
    ("admin_promo_list", "🎟️ List active promo codes"),
    ("admin_promo_revoke", "🚫 Revoke a promo code"),
    ("admin_broadcast", "📣 Broadcast to all users"),
)

# Admins see public + admin commands.
ADMIN_COMMANDS: tuple[tuple[str, str], ...] = PUBLIC_COMMANDS + ADMIN_ONLY_COMMANDS


def _to_bot_commands(
    pairs: tuple[tuple[str, str], ...],
) -> list[BotCommand]:
    return [BotCommand(command=c, description=d) for c, d in pairs]


async def publish_bot_commands(
    bot: Bot, admin_ids: Iterable[int]
) -> None:
    """Push the canonical command list to Telegram.

    Idempotent — Telegram dedupes if the published list hasn't
    changed. Errors are logged and swallowed so a transient network
    blip during startup doesn't take the bot down. Callers should
    invoke this once during boot, after ``Bot`` is constructed but
    before ``dp.start_polling``.

    Per-admin scoping uses ``BotCommandScopeChat`` keyed by the
    admin's telegram_id. Telegram requires the bot to have an active
    chat with the user before per-chat commands stick — which means
    a brand-new admin who hasn't sent ``/start`` yet will see only
    the public list until they do. That's fine; ``/admin`` still
    works (the dispatcher gates on ``is_admin``, not on what's in
    the menu).
    """
    public = _to_bot_commands(PUBLIC_COMMANDS)
    admin = _to_bot_commands(ADMIN_COMMANDS)

    try:
        await bot.set_my_commands(
            public, scope=BotCommandScopeAllPrivateChats()
        )
        log.info(
            "published %d public bot commands to AllPrivateChats scope",
            len(public),
        )
    except Exception:
        # Network blip / Telegram 5xx / bot-token revoked — log and
        # continue. The bot is still functional; just the slash
        # menu may show whatever was previously cached.
        log.exception("set_my_commands(public) failed")

    for admin_id in admin_ids:
        try:
            await bot.set_my_commands(
                admin, scope=BotCommandScopeChat(chat_id=admin_id)
            )
            log.info(
                "published %d admin bot commands to chat %s",
                len(admin),
                admin_id,
            )
        except Exception:
            # Most common cause: admin hasn't started a chat with
            # the bot yet (Telegram returns 400 "chat not found").
            # Demote to a warning so a misconfigured ADMIN_USER_IDS
            # entry doesn't spam the log with a full traceback.
            log.warning(
                "set_my_commands for admin %s failed (the admin "
                "may not have /start'd the bot yet)",
                admin_id,
                exc_info=True,
            )


__all__ = [
    "ADMIN_COMMANDS",
    "ADMIN_ONLY_COMMANDS",
    "PUBLIC_COMMANDS",
    "publish_bot_commands",
]
