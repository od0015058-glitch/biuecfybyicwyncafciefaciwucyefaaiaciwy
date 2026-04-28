"""Telegram-side admin gating + admin command handlers.

Why Telegram instead of a standalone CLI: the bot is already running,
already auth'd to the user, and already has a writable shell to the
DB pool. Spinning up a separate admin binary just means another thing
to deploy and SSH into. Per-user gating via ``ADMIN_USER_IDS`` env var
is sufficient for the threat model (the secret the attacker would need
is the env file, which already protects the bot token / DB password /
NowPayments keys).

Public surface so far:
* ``parse_admin_user_ids`` — env-string parser.
* ``set_admin_user_ids`` — runtime override (mostly for tests).
* ``is_admin`` — gate predicate.
* ``router`` — aiogram ``Router`` with the admin commands; included
  by ``main.py`` after the public router so admin commands take
  precedence on overlapping prefixes (``/start`` would never overlap,
  but defensive ordering matters).

Each command handler **silently no-ops** for non-admins. We don't
want to leak the existence of the admin surface to a curious user
poking at the bot.
"""

from __future__ import annotations

import logging
import os

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from database import db

log = logging.getLogger("bot.admin")

router = Router()


def parse_admin_user_ids(raw: str | None) -> frozenset[int]:
    """Parse the ``ADMIN_USER_IDS`` env value into a frozenset of ints.

    Tolerant: empty / None → empty set. Whitespace-only entries and
    non-integer entries are silently dropped (with a WARNING log) so
    a typo in the env doesn't crash the bot at startup.
    """
    if not raw:
        return frozenset()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            log.warning(
                "ADMIN_USER_IDS: ignoring non-integer entry %r", part
            )
    return frozenset(out)


_ADMIN_USER_IDS: frozenset[int] = parse_admin_user_ids(
    os.getenv("ADMIN_USER_IDS")
)


def set_admin_user_ids(ids: frozenset[int] | set[int] | list[int]) -> None:
    """Override the admin set at runtime. Intended for tests; production
    populates this once from the env at import time."""
    global _ADMIN_USER_IDS
    _ADMIN_USER_IDS = frozenset(int(i) for i in ids)


def is_admin(telegram_id: int | None) -> bool:
    if telegram_id is None:
        return False
    return telegram_id in _ADMIN_USER_IDS


# ---------------------------------------------------------------------
# /admin   →  hub message
# ---------------------------------------------------------------------

_ADMIN_HUB_TEXT = (
    "🛠 *Admin hub*\n\n"
    "Available commands:\n"
    "• `/admin` — this menu\n"
    "• `/admin_metrics` — system stats (users, revenue, top models)\n"
    "\n"
    "_More commands will be added in subsequent PRs (balance ops,"
    " promo creation, broadcast)._"
)


@router.message(Command("admin"))
async def admin_hub(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        log.info(
            "non-admin /admin attempt by telegram_id=%s",
            getattr(message.from_user, "id", None),
        )
        return  # silent no-op
    await message.answer(_ADMIN_HUB_TEXT, parse_mode="Markdown")


# ---------------------------------------------------------------------
# /admin_metrics  →  system stats
# ---------------------------------------------------------------------


def format_metrics(rows: dict) -> str:
    """Pretty-print the metrics dict produced by ``Database.get_system_metrics``.

    Pulled out for testability so we don't need a real DB to verify
    the output shape.
    """
    lines = [
        "📊 *System metrics*",
        "",
        f"👥 Users (total): *{rows['users_total']:,}*",
        f"🟢 Active 7d: *{rows['users_active_7d']:,}*",
        f"💰 Revenue (USD credited): *${rows['revenue_usd']:.2f}*",
        f"🤖 AI spend (USD deducted): *${rows['spend_usd']:.4f}*",
    ]
    if rows.get("top_models"):
        lines.append("")
        lines.append("🔝 *Top models* (by call count, 30d)")
        for i, row in enumerate(rows["top_models"], start=1):
            model = row["model"]
            count = row["count"]
            cost = row["cost_usd"]
            lines.append(
                f"  {i}. `{model}` — {count:,} calls, ${cost:.4f}"
            )
    else:
        lines.append("")
        lines.append("_(no usage logged yet)_")
    return "\n".join(lines)


@router.message(Command("admin_metrics"))
async def admin_metrics(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op
    try:
        metrics = await db.get_system_metrics()
    except Exception:
        log.exception("admin_metrics: get_system_metrics failed")
        await message.answer("❌ Failed to query metrics — see logs.")
        return
    await message.answer(format_metrics(metrics), parse_mode="Markdown")
