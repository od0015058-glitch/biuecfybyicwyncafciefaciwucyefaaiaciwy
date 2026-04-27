"""Aiogram middlewares.

``UserUpsertMiddleware`` idempotently upserts the Telegram user into our
``users`` table on every incoming Message / CallbackQuery *before* any
handler runs. This guarantees that any FK-referenced operation (e.g.
inserting a PENDING transaction with ``telegram_id``) cannot violate
``transactions_telegram_id_fkey`` because the ``users`` row is always
present first.

Why a middleware instead of upserting per handler:
  * There are ~30 handlers; missing one is a bug class waiting to bite.
  * Telegram clients re-open chats without re-sending ``/start``, so the
    ``/start``-only upsert path was unreliable in practice. Devin Review
    bug ticket on PR #23 deploy: a friend testing the bot tapped Wallet
    -> charge directly and the FK constraint blew up
    (``Key (telegram_id)=(...) is not present in table "users"``).
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from database import db

log = logging.getLogger("bot.middlewares")


class UserUpsertMiddleware(BaseMiddleware):
    """Upsert the sender into ``users`` before every handler runs.

    Registered as an *outer* middleware on both ``dp.message`` and
    ``dp.callback_query`` so it fires for both keyboard button presses
    (text messages) and inline button taps (callbacks).
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        from_user = getattr(event, "from_user", None)
        if from_user is not None and from_user.id is not None:
            try:
                await db.create_user(
                    telegram_id=from_user.id,
                    username=from_user.username or "Unknown",
                )
            except Exception:
                # Don't let upsert errors block the handler. ON CONFLICT
                # DO NOTHING makes this near-impossible in practice; if
                # it does happen we'd rather surface the downstream FK
                # error in the log than silently drop the interaction.
                log.exception(
                    "user upsert failed for telegram_id=%s",
                    from_user.id,
                )
        return await handler(event, data)
