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


# ---------------------------------------------------------------------
# Markdown-escape helper.
#
# Telegram's legacy ``parse_mode="Markdown"`` treats ``_`` ``*`` `` ` ``
# ``[`` as formatting markers and rejects the *entire message* with
# 400 BadRequest if they're unbalanced. Free-form admin-typed text
# (``reason`` on credit/debit, persisted ``notes`` on the wallet
# snapshot) used to land in those messages unescaped — Devin Review
# caught this on PR #50: a reason like ``stuck_invoice`` would crash
# the success confirmation **after** the DB write had already
# committed, so the admin would retry and double-adjust the balance.
#
# Escape, don't strip — admins should see exactly what they typed,
# not a sanitized variant. Escape via prefix-backslash, which legacy
# Markdown honours for these characters.
# ---------------------------------------------------------------------

_MD_RESERVED = "_*`["


def _escape_md(s: str | None) -> str:
    r"""Escape Telegram legacy-Markdown reserved characters in *s*.

    ``None`` or empty input returns ``""``. The four characters
    ``_ * ` [`` are prefixed with a backslash so the parser treats
    them as literals. We don't escape ``\`` itself: the only way one
    would land in admin-typed text is if the admin literally typed
    a backslash, in which case rendering it as-is is the obvious
    behaviour. (Telegram's legacy Markdown has no escape for ``\\``
    anyway — it just renders as ``\``.)
    """
    if not s:
        return ""
    return "".join("\\" + c if c in _MD_RESERVED else c for c in s)


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
    "• `/admin_promo_create <CODE> <pct%|$amt> [max_uses] [days]` — new promo\n"
    "• `/admin_promo_list` — list promo codes (newest 20)\n"
    "• `/admin_promo_revoke <CODE>` — soft-delete a promo code\n"
    "• `/admin_broadcast [--active=N] <text>` — send `<text>` to every "
    "user (or only users active in the last `N` days)"
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
            # Escape free-form note text — Markdown-special chars
            # in a stored note (`_`, `*`, `` ` ``, `[`) would
            # otherwise crash the whole admin reply with 400 Bad
            # Request, hiding the wallet snapshot from the admin.
            note_suffix = f" — _{_escape_md(note)}_" if note else ""
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
        # Escape free-form reason — without this, a reason like
        # ``stuck_invoice`` (admin's natural shorthand) would crash
        # this confirmation with 400 BadRequest **after** the DB
        # write had already committed. The admin would retry and
        # double-adjust the user's balance. Reported by Devin Review
        # on PR #50.
        f"Reason: _{_escape_md(reason)}_",
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


# ---------------------------------------------------------------------
# Promo creation / list / revoke
# ---------------------------------------------------------------------


def parse_promo_create_args(text: str) -> dict | str:
    """Parse ``/admin_promo_create <CODE> <pct%|$amt> [max_uses] [days]``.

    Returns a dict shaped::

        {
          "code": "WELCOME20",
          "discount_percent": 20,            # XOR with discount_amount
          "discount_amount": None,           # XOR with discount_percent
          "max_uses": 100 | None,
          "expires_in_days": 30 | None,
        }

    Returns a string error key on failure: ``"missing"``,
    ``"bad_code"``, ``"bad_discount"``, ``"bad_max_uses"``,
    ``"bad_days"``.

    Discount syntax:
      * ``20%``       → percent
      * ``$2.50``     → fixed USD
      * ``2.5``       → fixed USD (bare number assumed dollars)
    """
    parts = text.strip().split()
    if len(parts) < 3:
        return "missing"
    code = parts[1].upper()
    if not code or len(code) > 64 or not all(
        c.isalnum() or c in "_-" for c in code
    ):
        return "bad_code"

    raw_disc = parts[2]
    discount_percent: int | None = None
    discount_amount: float | None = None
    if raw_disc.endswith("%"):
        try:
            pct = int(raw_disc[:-1])
        except ValueError:
            return "bad_discount"
        if not (1 <= pct <= 100):
            return "bad_discount"
        discount_percent = pct
    else:
        try:
            amount = float(raw_disc.lstrip("$"))
        except ValueError:
            return "bad_discount"
        if not (amount == amount) or amount in (
            float("inf"), float("-inf")
        ) or amount <= 0:
            return "bad_discount"
        discount_amount = amount

    max_uses: int | None = None
    if len(parts) >= 4:
        try:
            max_uses = int(parts[3])
        except ValueError:
            return "bad_max_uses"
        if max_uses <= 0:
            return "bad_max_uses"

    expires_in_days: int | None = None
    if len(parts) >= 5:
        try:
            expires_in_days = int(parts[4])
        except ValueError:
            return "bad_days"
        if expires_in_days <= 0:
            return "bad_days"

    return {
        "code": code,
        "discount_percent": discount_percent,
        "discount_amount": discount_amount,
        "max_uses": max_uses,
        "expires_in_days": expires_in_days,
    }


_PROMO_CREATE_ERR_TEXT = {
    "missing": (
        "❌ Usage: `/admin_promo_create <CODE> <pct%|$amount> "
        "[max_uses] [days]`\n"
        "Examples:\n"
        "  `/admin_promo_create WELCOME20 20% 100 30`\n"
        "  `/admin_promo_create WINTER $5 50`\n"
        "  `/admin_promo_create FIVEOFF $5`"
    ),
    "bad_code": (
        "❌ Code must be alphanumeric (plus `_`/`-`), 1-64 chars."
    ),
    "bad_discount": (
        "❌ Discount must be `<int>%` (1-100) or `$<num>` "
        "(positive USD)."
    ),
    "bad_max_uses": (
        "❌ max_uses must be a positive integer (or omit it for "
        "unlimited)."
    ),
    "bad_days": (
        "❌ days-until-expiry must be a positive integer (or omit "
        "it for no expiry)."
    ),
}


def _format_promo_row(r: dict) -> str:
    if r.get("discount_percent") is not None:
        disc = f"{r['discount_percent']}%"
    elif r.get("discount_amount") is not None:
        disc = f"${r['discount_amount']:.2f}"
    else:
        disc = "?"
    used = r.get("used_count", 0)
    cap = r.get("max_uses")
    used_label = f"{used}/{cap}" if cap is not None else f"{used}/∞"
    state = "active" if r.get("is_active") else "*revoked*"
    expiry = r.get("expires_at")
    expiry_label = f" exp={expiry[:10]}" if expiry else ""
    return (
        f"`{r['code']}` — {disc} — {used_label}{expiry_label} — {state}"
    )


@router.message(Command("admin_promo_create"))
async def admin_promo_create(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    parsed = parse_promo_create_args(message.text or "")
    if isinstance(parsed, str):
        await message.answer(_PROMO_CREATE_ERR_TEXT[parsed])
        return

    expires_at = None
    if parsed["expires_in_days"] is not None:
        from datetime import datetime, timedelta, timezone
        expires_at = datetime.now(timezone.utc) + timedelta(
            days=parsed["expires_in_days"]
        )

    try:
        ok = await db.create_promo_code(
            code=parsed["code"],
            discount_percent=parsed["discount_percent"],
            discount_amount=parsed["discount_amount"],
            max_uses=parsed["max_uses"],
            expires_at=expires_at,
        )
    except ValueError as exc:
        # Defensive — parse_promo_create_args already enforces the
        # XOR / range invariants, so create_promo_code should not
        # raise. Surface anyway in case the contract drifts.
        await message.answer(f"❌ {exc}")
        return
    except Exception:
        log.exception("admin_promo_create: DB write failed")
        await message.answer("❌ DB write failed — see logs.")
        return

    if not ok:
        await message.answer(
            f"❌ Code `{parsed['code']}` already exists. Pick another or "
            f"use `/admin_promo_revoke {parsed['code']}` first."
        )
        return

    if parsed["discount_percent"] is not None:
        disc_label = f"{parsed['discount_percent']}%"
    else:
        disc_label = f"${parsed['discount_amount']:.2f}"
    cap = parsed["max_uses"]
    cap_label = f"{cap} uses" if cap is not None else "unlimited uses"
    exp_label = (
        f", expires in {parsed['expires_in_days']} days"
        if parsed["expires_in_days"] is not None else ", no expiry"
    )
    log.info(
        "admin_promo_create: admin=%s code=%s disc=%s cap=%s",
        message.from_user.id, parsed["code"], disc_label, cap,
    )
    await message.answer(
        f"✅ Created promo `{parsed['code']}`: {disc_label}, "
        f"{cap_label}{exp_label}.",
        parse_mode="Markdown",
    )


@router.message(Command("admin_promo_list"))
async def admin_promo_list(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    try:
        rows = await db.list_promo_codes(limit=20)
    except Exception:
        log.exception("admin_promo_list: DB read failed")
        await message.answer("❌ DB query failed — see logs.")
        return
    if not rows:
        await message.answer("_No promo codes yet._", parse_mode="Markdown")
        return
    lines = ["🎁 *Promo codes* (newest 20)", ""]
    for r in rows:
        lines.append(f"• {_format_promo_row(r)}")
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("admin_promo_revoke"))
async def admin_promo_revoke(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    parts = (message.text or "").strip().split(None, 1)
    if len(parts) < 2:
        await message.answer("❌ Usage: `/admin_promo_revoke <CODE>`")
        return
    code = parts[1].strip().upper()
    if not code:
        await message.answer("❌ Code is required.")
        return
    try:
        revoked = await db.revoke_promo_code(code)
    except Exception:
        log.exception("admin_promo_revoke: DB write failed")
        await message.answer("❌ DB write failed — see logs.")
        return
    if revoked:
        log.info(
            "admin_promo_revoke: admin=%s code=%s",
            message.from_user.id, code,
        )
        await message.answer(
            f"✅ Revoked `{code}`. Existing redemptions are kept; "
            f"new validations of this code will fail with `inactive`.",
            parse_mode="Markdown",
        )
    else:
        await message.answer(
            f"❌ `{code}` does not exist or is already revoked.",
            parse_mode="Markdown",
        )


# ---------------------------------------------------------------------
# /admin_broadcast — fan-out a text message to every (or recently
# active) user, throttled below Telegram's documented per-bot send
# rate (30 msg/s to *different* chats; we sit at ~25/s for headroom).
#
# Block-list / blocked-bot / dead-chat sends are caught and counted
# rather than aborting the broadcast. We also catch ``TelegramRetryAfter``
# (HTTP 429) and honour the server's ``retry_after`` window before
# resuming, so a transient surge doesn't kill the whole broadcast.
#
# Progress is reported by editing a single status message every
# ``_BROADCAST_PROGRESS_EVERY`` deliveries — chat-flooding the admin
# with one update per recipient would itself trip rate limits.
# ---------------------------------------------------------------------

import asyncio
import re

# Telegram doc: "30 messages per second to different users". Sit at
# 25/s = 0.04s between sends so we never crowd the limit. A burst
# allowance of 30 is documented but we'd rather pace conservatively.
_BROADCAST_DELAY_S = 0.04
_BROADCAST_PROGRESS_EVERY = 25
# Cap to avoid letting an admin DoS Telegram via a broadcast text
# longer than a single Telegram message can carry.
_BROADCAST_MAX_TEXT_LEN = 3500
# Upper bound on ``--active=N`` / ``only_active_days=``. PostgreSQL's
# ``interval`` stores days in a 32-bit int; an admin typing
# ``--active=9999999999`` (ten digits) would overflow the
# ``f"{N} days"`` string we format in
# :meth:`Database.iter_broadcast_recipients`, crashing the query with
# an opaque "DB query failed" banner instead of a friendly validation
# error up-front. 36_500 days (≈100 years) matches the bound already
# in place for promo/gift-code expiry — no real admin has "active in
# the last century" as a meaningful filter and the cap keeps the
# interval well clear of the PG overflow surface.
_BROADCAST_ACTIVE_DAYS_MAX = 36_500


def parse_broadcast_args(text: str) -> dict | str:
    """Parse ``/admin_broadcast [--active=N] <text>``.

    Returns either::

        {"only_active_days": int | None, "text": str}

    on success, or a string error key on failure: ``"missing"``
    (no body), ``"bad_active"`` (``--active`` parse failed),
    ``"active_too_large"`` (``--active`` > ``_BROADCAST_ACTIVE_DAYS_MAX``,
    which would otherwise overflow PG's interval column downstream),
    ``"too_long"`` (body > ``_BROADCAST_MAX_TEXT_LEN``).

    The body is everything after the command (and after the optional
    ``--active=N`` flag). Newlines are preserved so the admin can
    send formatted multi-line announcements. Leading/trailing
    whitespace is stripped.
    """
    # Drop the leading slash-command token.
    after = text.split(None, 1)
    if len(after) < 2 or not after[1].strip():
        return "missing"
    body = after[1]

    only_active_days: int | None = None
    m = re.match(r"\s*--active=(\S+)\s*", body)
    if m:
        try:
            only_active_days = int(m.group(1))
        except ValueError:
            return "bad_active"
        if only_active_days <= 0:
            return "bad_active"
        if only_active_days > _BROADCAST_ACTIVE_DAYS_MAX:
            return "active_too_large"
        body = body[m.end():]

    body = body.strip()
    if not body:
        return "missing"
    if len(body) > _BROADCAST_MAX_TEXT_LEN:
        return "too_long"

    return {"only_active_days": only_active_days, "text": body}


_BROADCAST_ERR_TEXT = {
    "missing": (
        "❌ Usage: `/admin_broadcast [--active=N] <text>`\n"
        "Examples:\n"
        "  `/admin_broadcast Hello everyone! New feature shipped.`\n"
        "  `/admin_broadcast --active=30 Heads-up: scheduled maintenance...`"
    ),
    "bad_active": (
        "❌ `--active=N` must be a positive integer (days)."
    ),
    "active_too_large": (
        f"❌ `--active=N` must be ≤ {_BROADCAST_ACTIVE_DAYS_MAX:,} "
        "days (≈100 years)."
    ),
    "too_long": (
        f"❌ Broadcast body too long (limit "
        f"{_BROADCAST_MAX_TEXT_LEN} chars)."
    ),
}


async def _do_broadcast(
    bot,
    *,
    recipients: list[int],
    text: str,
    admin_id: int,
    progress_callback=None,
) -> dict:
    """Send *text* to each id in *recipients*, paced + error-counted.

    Returns a stats dict ``{sent, blocked, failed, total}``. Logs
    every failure for forensics. Calls *progress_callback* — an
    ``async (stats: dict) -> None`` — every
    ``_BROADCAST_PROGRESS_EVERY`` recipients (and once at the end)
    with a snapshot dict ``{i, total, sent, blocked, failed}`` so
    the caller can surface progress however it wants (Telegram
    ``edit_text``, web-panel in-memory job dict, structured log,
    …). Passing ``None`` disables progress reporting entirely.
    """
    # Lazy import so the ``aiogram.exceptions`` symbol load doesn't
    # happen at module import time (and so test code that patches
    # ``aiogram`` doesn't get tangled in admin.py's import order).
    from aiogram.exceptions import (
        TelegramBadRequest,
        TelegramForbiddenError,
        TelegramRetryAfter,
    )

    sent = 0
    blocked = 0
    failed = 0
    total = len(recipients)

    for i, chat_id in enumerate(recipients, 1):
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            sent += 1
        except TelegramForbiddenError:
            # User blocked the bot OR deleted their account. Expected
            # at scale; just count and move on.
            blocked += 1
        except TelegramRetryAfter as exc:
            # Honour the server's back-off window. After sleeping,
            # retry *this* recipient (don't lose them).
            log.warning(
                "broadcast: 429 from Telegram, retry_after=%ss "
                "(recipient %d of %d)",
                exc.retry_after, i, total,
            )
            await asyncio.sleep(exc.retry_after)
            try:
                await bot.send_message(chat_id=chat_id, text=text)
                sent += 1
            except Exception:
                failed += 1
                log.exception(
                    "broadcast: post-429 retry failed for chat_id=%d",
                    chat_id,
                )
        except TelegramBadRequest:
            # Chat not found, deactivated user, etc.
            failed += 1
            log.exception(
                "broadcast: bad_request for chat_id=%d", chat_id
            )
        except Exception:
            failed += 1
            log.exception(
                "broadcast: unexpected error for chat_id=%d", chat_id
            )

        if progress_callback is not None and (
            i % _BROADCAST_PROGRESS_EVERY == 0 or i == total
        ):
            try:
                await progress_callback({
                    "i": i, "total": total,
                    "sent": sent, "blocked": blocked, "failed": failed,
                })
            except Exception:
                # Progress callbacks are best-effort; never let one
                # failure abort the whole broadcast.
                log.debug(
                    "broadcast: progress callback raised (i=%d)", i,
                    exc_info=True,
                )

        # Pace below Telegram's per-bot rate cap. Skip the delay
        # on the very last recipient to shorten the visible duration.
        if i < total:
            await asyncio.sleep(_BROADCAST_DELAY_S)

    log.info(
        "broadcast: admin=%s sent=%d blocked=%d failed=%d total=%d",
        admin_id, sent, blocked, failed, total,
    )
    return {"sent": sent, "blocked": blocked, "failed": failed,
            "total": total}


@router.message(Command("admin_broadcast"))
async def admin_broadcast(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op
    parsed = parse_broadcast_args(message.text or "")
    if isinstance(parsed, str):
        await message.answer(_BROADCAST_ERR_TEXT[parsed])
        return

    try:
        recipients = await db.iter_broadcast_recipients(
            only_active_days=parsed["only_active_days"]
        )
    except Exception:
        log.exception("admin_broadcast: recipient query failed")
        await message.answer("❌ DB query failed — see logs.")
        return

    if not recipients:
        await message.answer(
            "❌ No recipients matched. "
            "(Try without `--active=N` to include everyone.)"
        )
        return

    eta_seconds = int(len(recipients) * _BROADCAST_DELAY_S) + 1
    progress = await message.answer(
        f"📣 Broadcasting to {len(recipients)} user(s) "
        f"(ETA ~{eta_seconds}s)…\n"
        f"Progress: 0/{len(recipients)}"
    )

    async def _edit_progress(stats: dict) -> None:
        await progress.edit_text(
            f"📣 Broadcasting…\n"
            f"Progress: {stats['i']}/{stats['total']}\n"
            f"Sent: {stats['sent']}  "
            f"Blocked: {stats['blocked']}  "
            f"Failed: {stats['failed']}"
        )

    stats = await _do_broadcast(
        message.bot,
        recipients=recipients,
        text=parsed["text"],
        admin_id=message.from_user.id,
        progress_callback=_edit_progress,
    )
    await message.answer(
        "✅ Broadcast complete.\n"
        f"Sent: *{stats['sent']}*  "
        f"Blocked: *{stats['blocked']}*  "
        f"Failed: *{stats['failed']}*  "
        f"Total: *{stats['total']}*",
        parse_mode="Markdown",
    )
