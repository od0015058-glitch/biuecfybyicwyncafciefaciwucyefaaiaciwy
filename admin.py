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
    "• `/admin_balance <user_id>` — view a user's wallet + last 5 txs\n"
    "• `/admin_credit <user_id> <usd> <reason>` — add USD to wallet\n"
    "• `/admin_debit <user_id> <usd> <reason>` — subtract USD from wallet\n"
    "\n"
    "_More commands will be added in subsequent PRs (promo creation,"
    " broadcast)._"
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


# ---------------------------------------------------------------------
# Balance ops:
#   /admin_balance <user_id>
#   /admin_credit  <user_id> <usd> <reason words...>
#   /admin_debit   <user_id> <usd> <reason words...>
# ---------------------------------------------------------------------


def parse_balance_args(text: str) -> tuple[int, float, str] | str:
    """Parse '/admin_credit 12345 5.50 stuck-invoice refund' into
    (12345, 5.50, 'stuck-invoice refund'). Returns an error key string
    on failure: ``"missing"`` / ``"bad_user_id"`` / ``"bad_amount"``
    / ``"missing_reason"``.

    The leading word (the command itself) is stripped before parsing
    so callers can pass ``message.text`` directly.
    """
    parts = text.strip().split(None, 3)
    if len(parts) < 4:
        # Need: command + user_id + amount + reason
        if len(parts) < 2:
            return "missing"
        if len(parts) < 3:
            return "bad_amount"
        return "missing_reason"
    _cmd, user_id_raw, amount_raw, reason = parts
    try:
        user_id = int(user_id_raw)
    except ValueError:
        return "bad_user_id"
    try:
        amount = float(amount_raw)
    except ValueError:
        return "bad_amount"
    if not (amount == amount):  # NaN guard
        return "bad_amount"
    if amount in (float("inf"), float("-inf")):
        return "bad_amount"
    if amount <= 0:
        return "bad_amount"
    reason = reason.strip()
    if not reason:
        return "missing_reason"
    return user_id, amount, reason


_PARSE_ERR_TEXT = {
    "missing": (
        "❌ Usage: `/admin_credit <user_id> <usd> <reason>` — "
        "all four parts required."
    ),
    "bad_user_id": (
        "❌ user_id must be an integer Telegram id."
    ),
    "bad_amount": (
        "❌ amount must be a positive number in USD."
    ),
    "missing_reason": (
        "❌ A reason is required (it gets logged in the ledger). "
        "Anything beyond `<usd>` is treated as the reason."
    ),
}


def _format_balance_summary(summary: dict) -> str:
    user_label = (
        f"@{summary['username']}"
        if summary.get("username")
        else f"id={summary['telegram_id']}"
    )
    lines = [
        f"💼 *Wallet for {user_label}* (`{summary['telegram_id']}`)",
        "",
        f"• Balance: *${summary['balance_usd']:.4f}*",
        f"• Free messages left: {summary['free_messages_left']}",
        f"• Active model: `{summary['active_model']}`",
        f"• Language: `{summary['language_code']}`",
        f"• Total credited (lifetime): ${summary['total_credited_usd']:.4f}",
        f"• Total spent (lifetime): ${summary['total_spent_usd']:.4f}",
    ]
    txs = summary.get("recent_transactions") or []
    if txs:
        lines.append("")
        lines.append("📜 *Last 5 transactions*")
        for r in txs:
            sign = "+" if r["amount_usd"] >= 0 else "−"
            amount_abs = abs(r["amount_usd"])
            note = r.get("notes")
            note_suffix = f" — _{note}_" if note else ""
            lines.append(
                f"  • #{r['id']} `{r['gateway']}` "
                f"{sign}${amount_abs:.4f} ({r['status']}){note_suffix}"
            )
    return "\n".join(lines)


@router.message(Command("admin_balance"))
async def admin_balance(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op
    parts = (message.text or "").strip().split(None, 1)
    if len(parts) < 2:
        await message.answer("❌ Usage: `/admin_balance <user_id>`")
        return
    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("❌ user_id must be an integer Telegram id.")
        return
    try:
        summary = await db.get_user_admin_summary(user_id)
    except Exception:
        log.exception("admin_balance: get_user_admin_summary failed")
        await message.answer("❌ DB query failed — see logs.")
        return
    if summary is None:
        await message.answer(f"❌ No user with id `{user_id}`.")
        return
    await message.answer(
        _format_balance_summary(summary), parse_mode="Markdown"
    )


async def _handle_balance_op(
    message: Message, *, sign: int
) -> None:
    """Shared body of ``/admin_credit`` and ``/admin_debit``.

    ``sign`` is +1 for credit, -1 for debit.
    """
    parsed = parse_balance_args(message.text or "")
    if isinstance(parsed, str):
        await message.answer(_PARSE_ERR_TEXT[parsed])
        return
    user_id, amount, reason = parsed
    delta = sign * amount

    try:
        result = await db.admin_adjust_balance(
            telegram_id=user_id,
            delta_usd=delta,
            reason=reason,
            admin_telegram_id=message.from_user.id,
        )
    except Exception:
        log.exception("admin_adjust_balance failed")
        await message.answer("❌ DB write failed — see logs.")
        return

    if result is None:
        # Either user does not exist OR (for debit) insufficient funds.
        # Disambiguate via a follow-up summary fetch — costs one round
        # trip but only on the error path.
        summary = await db.get_user_admin_summary(user_id)
        if summary is None:
            await message.answer(f"❌ No user with id `{user_id}`.")
        else:
            await message.answer(
                f"❌ Refused — debit of ${amount:.4f} would take user "
                f"`{user_id}` below zero "
                f"(current balance: ${summary['balance_usd']:.4f})."
            )
        return

    sign_label = "Credited" if sign > 0 else "Debited"
    log.info(
        "admin_adjust_balance: admin=%s user=%s delta=$%.4f tx=%d reason=%r",
        message.from_user.id, user_id, delta,
        result["transaction_id"], reason,
    )
    await message.answer(
        f"✅ {sign_label} `{user_id}` ${amount:.4f}.\n"
        f"New balance: *${result['new_balance']:.4f}*\n"
        f"Tx id: `{result['transaction_id']}`\n"
        f"Reason: _{reason}_",
        parse_mode="Markdown",
    )


@router.message(Command("admin_credit"))
async def admin_credit(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    await _handle_balance_op(message, sign=+1)


@router.message(Command("admin_debit"))
async def admin_debit(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    await _handle_balance_op(message, sign=-1)
