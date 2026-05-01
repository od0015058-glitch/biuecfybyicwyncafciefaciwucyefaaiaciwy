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

from admin_roles import (
    ROLE_OPERATOR,
    ROLE_SUPER,
    ROLE_VIEWER,
    VALID_ROLES,
    effective_role,
    normalize_role,
    role_at_least,
)
from database import db
from formatting import format_usd

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

    Tolerant: empty / None → empty set. Whitespace-only entries,
    non-integer entries, and **non-positive integer entries** are
    silently dropped (with a WARNING log) so a typo in the env
    doesn't crash the bot at startup.

    Why reject non-positive ids: Telegram never issues a 0 or
    negative user id. A typo (`123,-456`) or accidentally pasting a
    chat id into the env value would silently put a never-matchable
    row in the admin set; with Stage-15-Step-E #5 follow-up #3's
    auto-promote on top, a non-positive entry would also seed a
    bogus ``admin_roles`` row in the DB. Drop them at parse time so
    every downstream consumer sees a clean set.
    """
    if not raw:
        return frozenset()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError:
            log.warning(
                "ADMIN_USER_IDS: ignoring non-integer entry %r", part
            )
            continue
        if value <= 0:
            log.warning(
                "ADMIN_USER_IDS: ignoring non-positive entry %r "
                "(Telegram user ids are always >= 1)",
                part,
            )
            continue
        out.add(value)
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


def get_admin_user_ids() -> frozenset[int]:
    """Read-only accessor for the current admin set.

    Most callers historically reached into ``_ADMIN_USER_IDS``
    directly, which is fine inside this module but leaks the private
    attribute to outside modules (``model_discovery``, future admin
    notifiers). Expose a typed accessor so tests that use
    :func:`set_admin_user_ids` to reshape the set still flow through
    the gated predicate + this getter.
    """
    return _ADMIN_USER_IDS


# ---------------------------------------------------------------------
# Stage-15-Step-E #5 follow-up — role-aware command gating.
#
# The first slice of Step-E #5 (PR #123) shipped the role *table* +
# the role-CRUD commands but kept every other ``/admin_*`` handler
# gated on the flat env-list ``is_admin`` predicate. That meant a
# DB-tracked ``viewer`` actually had no surface — they could be granted
# the role, but :func:`is_admin` wouldn't match them, so every
# ``/admin_metrics`` / ``/admin_balance`` / etc. silently no-oped.
#
# This helper resolves the actor's *effective* role (DB row first,
# then env-list fallback) once per handler call so each handler can
# branch on a single, typed, async-safe value. Resolution is the
# same primitive the future ``/admin/roles`` web page will use, so
# the Telegram-side and web-side agree on what "viewer means here".
#
# Why DB-fetch on every call rather than caching: the role table is
# tiny (one row per admin), the lookup is a single primary-key SELECT
# on an indexed integer column, and an admin's role can be revoked
# in real-time. A stale-by-five-minutes cache would let a just-revoked
# operator credit a wallet during their grace period — too dangerous
# for the leverage saved.
# ---------------------------------------------------------------------


async def _resolve_actor_role(message: Message) -> str | None:
    """Return the effective admin role for the actor of *message*.

    Walks the same resolution order as :func:`admin_roles.effective_role`:

    1. ``Database.get_admin_role`` — DB-tracked role wins when set.
    2. Env-list ``ADMIN_USER_IDS`` — backward-compat fallback; legacy
       admins keep ``super`` access without forcing an op-by-op DB seed.
    3. ``None`` — not an admin in either layer.

    Returns ``None`` for messages without a ``from_user`` (a service
    message we somehow received as an admin command — defence in
    depth, the dispatcher shouldn't deliver these here).

    DB-fetch failures fall through to the env-list result. We'd
    rather a transient pool error keep the legacy admin's surface
    working than silently downgrade them to ``None`` mid-incident.
    """
    if message.from_user is None:
        return None
    telegram_id = message.from_user.id
    is_env_admin = is_admin(telegram_id)
    db_role: str | None = None
    try:
        db_role = await db.get_admin_role(telegram_id)
    except Exception:
        log.exception(
            "admin: get_admin_role failed for telegram_id=%s; "
            "falling through to env-list",
            telegram_id,
        )
    return effective_role(
        telegram_id, db_role, is_env_admin=is_env_admin
    )


async def _require_role(message: Message, required: str) -> bool:
    """Return ``True`` iff the actor of *message* has at least the
    *required* role; otherwise silent no-op + ``False``.

    Same fail-closed contract :func:`is_admin` enforces: a non-admin,
    a viewer querying a super-only command, or a malformed message
    all return ``False`` and produce no user-visible output. The
    handler is expected to short-circuit on a ``False`` return so we
    don't leak the existence of the admin namespace to a curious
    user poking at the bot.
    """
    role = await _resolve_actor_role(message)
    if role_at_least(role, required):
        return True
    log.info(
        "admin: %s denied — telegram_id=%s effective_role=%s "
        "required=%s",
        getattr(message, "text", "<no text>")[:40],
        getattr(message.from_user, "id", None),
        role,
        required,
    )
    return False


# ---------------------------------------------------------------------
# /admin   →  hub message
# ---------------------------------------------------------------------

# Hub-message lines, grouped by minimum role required to use the
# command. The hub is rendered with only the rows the actor's
# effective role can actually drive; surfacing a command in the menu
# that the gate would silently deny is exactly the discoverability
# trap Step-E #5 was meant to close.
_HUB_LINES_VIEWER: tuple[str, ...] = (
    "• `/admin` — this menu",
    "• `/admin_metrics` — system stats (users, revenue, top models)",
    "• `/admin_balance <user_id>` — view a user's wallet + last 5 txs",
)
_HUB_LINES_OPERATOR: tuple[str, ...] = (
    "• `/admin_broadcast [--active=N] <text>` — send `<text>` to every "
    "user (or only users active in the last `N` days)",
)
_HUB_LINES_SUPER: tuple[str, ...] = (
    "• `/admin_credit <user_id> <usd> <reason>` — add USD to wallet",
    "• `/admin_debit <user_id> <usd> <reason>` — subtract USD from wallet",
    "• `/admin_promo_create <CODE> <pct%|$amt> [max_uses] [days]` — new promo",
    "• `/admin_promo_list` — list promo codes (newest 20)",
    "• `/admin_promo_revoke <CODE>` — soft-delete a promo code",
)
# Role-CRUD commands stay env-list-only — surface them in the hub
# only for an env-list admin so a DB-tracked super (who can't promote
# themselves out of super) doesn't see commands they can't run.
_HUB_LINES_ENV_ONLY: tuple[str, ...] = (
    "• `/admin_role_grant <user_id> <viewer|operator|super>` — record "
    "a graduated role for a Telegram id",
    "• `/admin_role_revoke <user_id>` — drop the DB-tracked role row",
    "• `/admin_role_list` — list every DB-tracked admin role",
)


def _render_admin_hub(role: str | None, *, is_env_admin: bool) -> str:
    """Render the role-filtered admin hub message.

    Each section is included only if the actor's *role* satisfies the
    section's minimum. The role-CRUD section is gated on
    *is_env_admin* (not *role*) because the role table itself is the
    source of truth those commands manage — letting a DB-tracked super
    promote themselves out of the role table would defeat the gate.
    """
    lines: list[str] = ["🛠 *Admin hub*", "", "Available commands:"]
    if role_at_least(role, ROLE_VIEWER):
        lines.extend(_HUB_LINES_VIEWER)
    if role_at_least(role, ROLE_OPERATOR):
        lines.extend(_HUB_LINES_OPERATOR)
    if role_at_least(role, ROLE_SUPER):
        lines.extend(_HUB_LINES_SUPER)
    if is_env_admin:
        lines.extend(_HUB_LINES_ENV_ONLY)
    return "\n".join(lines)


# Kept for backward-compat with any external import; the live hub
# now uses :func:`_render_admin_hub` to filter by role.
_ADMIN_HUB_TEXT = _render_admin_hub(ROLE_SUPER, is_env_admin=True)


@router.message(Command("admin"))
async def admin_hub(message: Message) -> None:
    role = await _resolve_actor_role(message)
    if not role_at_least(role, ROLE_VIEWER):
        log.info(
            "non-admin /admin attempt by telegram_id=%s",
            getattr(message.from_user, "id", None),
        )
        return  # silent no-op
    is_env = is_admin(
        message.from_user.id if message.from_user else None
    )
    await message.answer(
        _render_admin_hub(role, is_env_admin=is_env),
        parse_mode="Markdown",
    )


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
    pending_count = rows.get("pending_payments_count", 0)
    if pending_count:
        oldest = rows.get("pending_payments_oldest_age_hours")
        if oldest is not None:
            lines.append(
                f"⏳ Pending payments: *{pending_count:,}* "
                f"(oldest {oldest:.1f}h)"
            )
        else:
            lines.append(f"⏳ Pending payments: *{pending_count:,}*")
        # Stage-15-Step-D #5 bundled fix: surface the
        # ``pending_payments_over_threshold_count`` sub-line so the
        # Telegram-side ``/admin_metrics`` digest matches what the
        # web dashboard already shows. Stage-12-Step-B added the
        # over-threshold count to the DB shape and wired it into
        # ``dashboard.html`` but missed this consumer, so an
        # operator running ``/admin_metrics`` saw the raw count
        # (e.g. "5 pending") with no signal that 3 of those 5 were
        # already past the proactive-DM threshold and should have
        # caused a separate alert. Showing both keeps the two admin
        # surfaces in sync.
        over_threshold = rows.get("pending_payments_over_threshold_count")
        threshold_h = rows.get("pending_alert_threshold_hours")
        if over_threshold and threshold_h:
            lines.append(
                f"  ↳ {over_threshold:,} over {threshold_h}h"
            )
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
    # Read-only digest — viewer is the floor.
    if not await _require_role(message, ROLE_VIEWER):
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
        f"• Balance: *{format_usd(summary['balance_usd'])}*",
        f"• Free messages left: {summary['free_messages_left']}",
        f"• Active model: `{summary['active_model']}`",
        f"• Language: `{summary['language_code']}`",
        f"• Total credited (lifetime): {format_usd(summary['total_credited_usd'])}",
        f"• Total spent (lifetime): {format_usd(summary['total_spent_usd'])}",
    ]
    txs = summary.get("recent_transactions") or []
    if txs:
        lines.append("")
        lines.append("📜 *Last 5 transactions*")
        for r in txs:
            sign = "+" if r["amount_usd"] >= 0 else ""
            note = r.get("notes")
            # Escape free-form note text — Markdown-special chars
            # in a stored note (`_`, `*`, `` ` ``, `[`) would
            # otherwise crash the whole admin reply with 400 Bad
            # Request, hiding the wallet snapshot from the admin.
            note_suffix = f" — _{_escape_md(note)}_" if note else ""
            lines.append(
                f"  • #{r['id']} `{r['gateway']}` "
                f"{sign}{format_usd(r['amount_usd'])} ({r['status']}){note_suffix}"
            )
    return "\n".join(lines)


@router.message(Command("admin_balance"))
async def admin_balance(message: Message) -> None:
    # Read-only wallet snapshot — viewer is the floor (the actual
    # credit/debit handlers below escalate to super).
    if not await _require_role(message, ROLE_VIEWER):
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
                f"❌ Refused — debit of {format_usd(amount)} would take user "
                f"`{user_id}` below zero "
                f"(current balance: {format_usd(summary['balance_usd'])})."
            )
        return

    sign_label = "Credited" if sign > 0 else "Debited"
    log.info(
        "admin_adjust_balance: admin=%s user=%s delta=$%.4f tx=%d reason=%r",
        message.from_user.id, user_id, delta,
        result["transaction_id"], reason,
    )
    await message.answer(
        f"✅ {sign_label} `{user_id}` {format_usd(amount)}.\n"
        f"New balance: *{format_usd(result['new_balance'])}*\n"
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
    # Wallet writes — super only. A viewer/operator runs against this
    # gate and the silent no-op kicks in (same surface every other
    # denied admin command produces).
    if not await _require_role(message, ROLE_SUPER):
        return
    await _handle_balance_op(message, sign=+1)


@router.message(Command("admin_debit"))
async def admin_debit(message: Message) -> None:
    # Wallet writes — super only.
    if not await _require_role(message, ROLE_SUPER):
        return
    await _handle_balance_op(message, sign=-1)


# ---------------------------------------------------------------------
# Promo creation / list / revoke
# ---------------------------------------------------------------------


# Upper bound on the ``max_uses`` argument of ``/admin_promo_create``.
# Pre-fix this was unbounded — typing
# ``/admin_promo_create FOO 10% 2147483648`` would overflow PostgreSQL's
# INTEGER column on insert and the asyncpg driver would raise
# ``NumericValueOutOfRangeError``, surfacing as the generic
# ``"DB write failed — see logs."`` reply with no hint at the cause.
# The web admin already has the equivalent cap; this mirrors it on
# the Telegram-side parser. 1M is well clear of the 2.1B PG INT max
# and already implausibly large for any real promo.
_PROMO_MAX_USES_CAP = 1_000_000
# Upper bound on ``[days]`` for the same reason — PostgreSQL's
# ``interval`` arithmetic (``NOW() + ($N || ' days')::interval``) tops
# out at ≈68 years before the underlying ``int32`` for the days
# component overflows. 36_500 days (≈100 years) matches the cap the
# web admin and broadcast filter already use; anything beyond that
# is almost certainly a typo.
_PROMO_EXPIRES_IN_DAYS_CAP = 36_500


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
    ``"max_uses_too_large"``, ``"bad_days"``, ``"days_too_large"``.

    Discount syntax:
      * ``20%``       → percent
      * ``$2.50``     → fixed USD
      * ``2.5``       → fixed USD (bare number assumed dollars)
    """
    parts = text.strip().split()
    if len(parts) < 3:
        return "missing"
    code = parts[1].upper()
    # ASCII-only: Telegram-side equivalent of the ``parse_promo_form``
    # guard in web_admin. ``str.isalnum`` returns True for Unicode
    # digits ("۱") and letters (Cyrillic homoglyphs of Latin chars),
    # which would store fine but never match the DB row a user types
    # on a standard keyboard later. Constrain to ASCII so the
    # Telegram-DM and web admin code-creation flows stay in lock-step.
    if not code or len(code) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
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
        if max_uses > _PROMO_MAX_USES_CAP:
            return "max_uses_too_large"

    expires_in_days: int | None = None
    if len(parts) >= 5:
        try:
            expires_in_days = int(parts[4])
        except ValueError:
            return "bad_days"
        if expires_in_days <= 0:
            return "bad_days"
        if expires_in_days > _PROMO_EXPIRES_IN_DAYS_CAP:
            return "days_too_large"

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
    "max_uses_too_large": (
        f"❌ max_uses must be at most {_PROMO_MAX_USES_CAP:,} "
        f"(DB INTEGER limit)."
    ),
    "bad_days": (
        "❌ days-until-expiry must be a positive integer (or omit "
        "it for no expiry)."
    ),
    "days_too_large": (
        f"❌ days-until-expiry must be at most {_PROMO_EXPIRES_IN_DAYS_CAP:,} "
        f"(≈100 years)."
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
    # Promo CRUD touches the live discount table — super only,
    # matching the wallet credit/debit gates.
    if not await _require_role(message, ROLE_SUPER):
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
    # Promo data is sensitive (revenue calc + active discounts the
    # bot honours) — super only, matching the rest of the
    # ``/admin_promo_*`` family per the role-gate contract.
    if not await _require_role(message, ROLE_SUPER):
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
    # Soft-deletes a promo code — super only.
    if not await _require_role(message, ROLE_SUPER):
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
# Stage-9-Step-6 bug-fix bundle: cap the ``TelegramRetryAfter``
# back-off so a misbehaving Telegram response (or an outage spike
# returning ``retry_after=3600``) can't pin the broadcast worker
# for an hour per affected recipient. Pre-fix, ``await
# asyncio.sleep(exc.retry_after)`` was uncapped — the operator
# would see an apparently-stuck broadcast and no log line
# explaining why. Now we sleep at most this many seconds and log
# a WARNING when the server-supplied window exceeds the cap so
# ops can spot prolonged degradation.
_BROADCAST_RETRY_AFTER_MAX_S = 60.0
# How finely we slice a long ``TelegramRetryAfter`` sleep when a
# ``should_cancel`` predicate is wired in (see ``_do_broadcast``).
# Pre-fix, the retry-after sleep was a single ``asyncio.sleep(60)``
# call — a cancel arriving mid-sleep was honoured only after the
# full window elapsed AND the post-sleep retry attempt completed.
# Slicing the sleep into ~1 s chunks bounds cancel latency to one
# slice while preserving the back-off semantics. We deliberately
# keep the fast path (no ``should_cancel``) on a single sleep so the
# cap-enforcement test in ``test_web_admin.py`` still observes the
# canonical "sleep == cap" call.
_BROADCAST_RETRY_AFTER_SLICE_S = 1.0
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
    should_cancel=None,
) -> dict:
    """Send *text* to each id in *recipients*, paced + error-counted.

    Returns a stats dict ``{sent, blocked, failed, total, cancelled}``.
    Logs every failure for forensics. Calls *progress_callback* —
    an ``async (stats: dict) -> None`` — every
    ``_BROADCAST_PROGRESS_EVERY`` recipients (and once at the end)
    with a snapshot dict ``{i, total, sent, blocked, failed}`` so
    the caller can surface progress however it wants (Telegram
    ``edit_text``, web-panel in-memory job dict, structured log,
    …). Passing ``None`` disables progress reporting entirely.

    *should_cancel* is an optional zero-arg callable (``() -> bool``)
    polled at the top of every loop iteration. The callable is
    intentionally synchronous so an in-memory flag flip from the
    web admin's cancel endpoint takes effect within one tick of
    the pacing sleep, even if the underlying flag-store has no
    asyncio integration. When it returns truthy the loop exits
    cleanly; the returned stats dict carries ``cancelled=True``
    so the caller can mark the job ``cancelled`` rather than
    ``completed``.
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
    cancelled = False
    total = len(recipients)

    for i, chat_id in enumerate(recipients, 1):
        # Stage-9-Step-6: soft-cancel check at the *top* of the loop
        # so a cancel arriving during the previous iteration's pacing
        # sleep is honoured before we hit Telegram one more time.
        # Swallow any exception from the predicate — it's a flag
        # read; nothing it raises is worth aborting a broadcast for.
        if should_cancel is not None:
            try:
                if should_cancel():
                    cancelled = True
                    log.info(
                        "broadcast: cancel requested at recipient %d "
                        "of %d (sent=%d blocked=%d failed=%d)",
                        i, total, sent, blocked, failed,
                    )
                    break
            except Exception:
                log.debug(
                    "broadcast: should_cancel predicate raised "
                    "(treating as not-cancelled)",
                    exc_info=True,
                )

        try:
            await bot.send_message(chat_id=chat_id, text=text)
            sent += 1
        except TelegramForbiddenError:
            # User blocked the bot OR deleted their account. Expected
            # at scale; just count and move on.
            blocked += 1
        except TelegramRetryAfter as exc:
            # Honour the server's back-off window — but cap it so a
            # misbehaving response can't pin the worker for an hour.
            raw_retry = float(exc.retry_after) if exc.retry_after else 0.0
            sleep_for = max(0.0, min(raw_retry, _BROADCAST_RETRY_AFTER_MAX_S))
            if raw_retry > _BROADCAST_RETRY_AFTER_MAX_S:
                log.warning(
                    "broadcast: 429 from Telegram, retry_after=%ss "
                    "(capped to %ss; recipient %d of %d)",
                    raw_retry, sleep_for, i, total,
                )
            else:
                log.warning(
                    "broadcast: 429 from Telegram, retry_after=%ss "
                    "(recipient %d of %d)",
                    raw_retry, i, total,
                )
            # Cancel-aware sleep: if a ``should_cancel`` predicate was
            # wired in, slice the (potentially long) back-off into
            # ~1 s chunks so a cancel arriving mid-sleep is honoured
            # within ``_BROADCAST_RETRY_AFTER_SLICE_S`` instead of
            # after the full ``_BROADCAST_RETRY_AFTER_MAX_S`` window.
            # Without a predicate (legacy / Telegram-driven callers
            # like ``admin_broadcast``) we keep the original single
            # ``asyncio.sleep(sleep_for)`` so the cap-enforcement
            # test in ``test_web_admin.py`` continues to observe the
            # canonical "sleep == cap" call.
            cancelled_during_sleep = False
            if should_cancel is None:
                await asyncio.sleep(sleep_for)
            else:
                remaining = sleep_for
                while remaining > 0:
                    try:
                        if should_cancel():
                            cancelled = True
                            cancelled_during_sleep = True
                            log.info(
                                "broadcast: cancel requested during "
                                "retry-after sleep at recipient %d of "
                                "%d (sent=%d blocked=%d failed=%d)",
                                i, total, sent, blocked, failed,
                            )
                            break
                    except Exception:
                        log.debug(
                            "broadcast: should_cancel predicate raised "
                            "during retry-after sleep "
                            "(treating as not-cancelled)",
                            exc_info=True,
                        )
                    slice_s = min(_BROADCAST_RETRY_AFTER_SLICE_S, remaining)
                    await asyncio.sleep(slice_s)
                    remaining -= slice_s
            if cancelled_during_sleep:
                # ``cancelled`` is set; drop out of the for-loop so the
                # final summary stats reflect the abort.
                break

            # Retry attempt — preserve the same Telegram-exception
            # categorization the parent handler uses, instead of the
            # pre-fix ``except Exception`` that lumped a "blocked the
            # bot during retry" or "rate-limited again" outcome into
            # the generic ``failed`` bucket and emitted a noisy stack
            # trace for what should be a quiet ``blocked`` increment.
            try:
                await bot.send_message(chat_id=chat_id, text=text)
                sent += 1
            except TelegramForbiddenError:
                blocked += 1
            except TelegramRetryAfter:
                # A second 429 inside the same recipient is rare but
                # possible (Telegram occasionally returns a fresh
                # back-off window mid-burst). We don't recurse — one
                # back-off per recipient is enough to keep the
                # broadcast moving — and surface the rare event at
                # WARNING so ops can correlate with Telegram-side
                # incidents.
                failed += 1
                log.warning(
                    "broadcast: second 429 on retry for chat_id=%d; "
                    "recording as failed and moving on",
                    chat_id,
                )
            except TelegramBadRequest:
                failed += 1
                log.exception(
                    "broadcast: post-429 bad_request for chat_id=%d",
                    chat_id,
                )
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
        "broadcast: admin=%s sent=%d blocked=%d failed=%d total=%d "
        "cancelled=%s",
        admin_id, sent, blocked, failed, total, cancelled,
    )
    return {"sent": sent, "blocked": blocked, "failed": failed,
            "total": total, "cancelled": cancelled}


@router.message(Command("admin_broadcast"))
async def admin_broadcast(message: Message) -> None:
    # Broadcast doesn't touch the wallet table or the role table —
    # operator suffices. Lets the team scale support broadcasts to
    # someone other than the wallet super-admin.
    if not await _require_role(message, ROLE_OPERATOR):
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


# ---------------------------------------------------------------------
# Stage-15-Step-E #5: admin role grant / revoke / list
# ---------------------------------------------------------------------
#
# Env-list admins (the only admins today, plus any DB-tracked admins
# graduated via the commands below) manage DB-tracked roles via three
# new Telegram commands. The role hierarchy itself is documented in
# ``admin_roles.py``; this module is just the user-facing surface.
# The Stage-15-Step-E #5 *follow-up* PR (this one) wired
# ``role_at_least`` into every other ``/admin_*`` handler so a
# DB-tracked viewer/operator now sees a real reduced surface — the
# role record finally drives the gate, not just the audit log.
#
# Still on the wishlist for a future PR:
# * Add a /admin/roles web page mirroring this CLI.
# * Wire role gates into the web admin panel (currently
#   single-password access — every web operator is a de-facto super).
#
# We keep the gate on `is_admin` (env-list) for these commands so a
# DB-tracked viewer can't promote themselves to super by virtue of
# having a row in the table.

_ROLE_GRANT_USAGE = (
    "❌ Usage: `/admin_role_grant <user_id> <role> [notes…]`\n"
    "Roles: `viewer` (read-only), `operator` (broadcasts, promo, gift), "
    "`super` (full access, default for legacy env-list admins)."
)


def _format_role_row(r: dict) -> str:
    """Render one ``Database.list_admin_roles`` row for the Telegram
    list view. Markdown-formatted; user-supplied ``notes`` are escaped
    via :func:`_escape_md` so a free-form ``stuck_invoice``-style
    string can't break the message render the way PR #50 documented.
    """
    when = (r.get("granted_at") or "")[:19].replace("T", " ")
    granted_by = r.get("granted_by")
    by_label = f" by `{granted_by}`" if granted_by else ""
    notes = r.get("notes") or ""
    notes_label = f" — _{_escape_md(notes)}_" if notes else ""
    return (
        f"• `{r['telegram_id']}` → *{r['role']}*"
        f" — granted {when}{by_label}{notes_label}"
    )


@router.message(Command("admin_role_grant"))
async def admin_role_grant(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op for non-admins
    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 3:
        await message.answer(_ROLE_GRANT_USAGE, parse_mode="Markdown")
        return
    raw_user_id = parts[1].strip()
    raw_role = parts[2].strip()
    notes = parts[3].strip() if len(parts) >= 4 else None

    try:
        user_id = int(raw_user_id)
    except ValueError:
        await message.answer(
            f"❌ `{_escape_md(raw_user_id)}` is not a valid Telegram id.",
            parse_mode="Markdown",
        )
        return

    role = normalize_role(raw_role)
    if role is None:
        await message.answer(
            f"❌ `{_escape_md(raw_role)}` is not a valid role. "
            f"Choose one of: {', '.join(sorted(VALID_ROLES))}.",
            parse_mode="Markdown",
        )
        return

    try:
        stored = await db.set_admin_role(
            user_id,
            role,
            granted_by=message.from_user.id if message.from_user else None,
            notes=notes,
        )
    except ValueError as exc:
        # ``Database.set_admin_role`` validates again (defence in
        # depth). Surface the validator's message verbatim so the
        # admin sees the offending value.
        await message.answer(f"❌ {exc}")
        return
    except Exception:
        log.exception("admin_role_grant: DB write failed")
        await message.answer("❌ DB write failed — see logs.")
        return

    # Best-effort audit. Don't block the success message on a write
    # failure to ``admin_audit_log``.
    try:
        await db.record_admin_audit(
            actor=str(message.from_user.id) if message.from_user else "tg",
            action="role_grant",
            target=f"user:{user_id}",
            outcome="ok",
            meta={"role": stored, "notes": notes},
        )
    except Exception:
        log.exception("admin_role_grant: audit insert failed")

    log.info(
        "admin_role_grant: admin=%s target=%s role=%s",
        message.from_user.id if message.from_user else None, user_id, stored,
    )
    await message.answer(
        f"✅ Granted role *{stored}* to `{user_id}`.",
        parse_mode="Markdown",
    )


@router.message(Command("admin_role_revoke"))
async def admin_role_revoke(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op for non-admins
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "❌ Usage: `/admin_role_revoke <user_id>`",
            parse_mode="Markdown",
        )
        return
    raw_user_id = parts[1].strip()
    try:
        user_id = int(raw_user_id)
    except ValueError:
        await message.answer(
            f"❌ `{_escape_md(raw_user_id)}` is not a valid Telegram id.",
            parse_mode="Markdown",
        )
        return

    try:
        deleted = await db.delete_admin_role(user_id)
    except Exception:
        log.exception("admin_role_revoke: DB write failed")
        await message.answer("❌ DB write failed — see logs.")
        return

    try:
        await db.record_admin_audit(
            actor=str(message.from_user.id) if message.from_user else "tg",
            action="role_revoke",
            target=f"user:{user_id}",
            outcome="ok" if deleted else "noop",
            meta={"deleted": bool(deleted)},
        )
    except Exception:
        log.exception("admin_role_revoke: audit insert failed")

    if deleted:
        log.info(
            "admin_role_revoke: admin=%s target=%s",
            message.from_user.id if message.from_user else None, user_id,
        )
        await message.answer(
            f"✅ Revoked DB-tracked role for `{user_id}`. "
            f"(If they remain in `ADMIN_USER_IDS`, they keep super "
            f"access via the env list.)",
            parse_mode="Markdown",
        )
    else:
        await message.answer(
            f"ℹ️ No DB-tracked role row for `{user_id}` — nothing to revoke.",
            parse_mode="Markdown",
        )


@router.message(Command("admin_role_list"))
async def admin_role_list(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return  # silent no-op for non-admins
    try:
        rows = await db.list_admin_roles(limit=200)
    except Exception:
        log.exception("admin_role_list: DB read failed")
        await message.answer("❌ DB query failed — see logs.")
        return
    if not rows:
        await message.answer(
            "_No DB-tracked admin roles yet. Legacy env-list admins "
            "(`ADMIN_USER_IDS`) still have full access._",
            parse_mode="Markdown",
        )
        return
    lines = ["🛡 *Admin roles* (DB-tracked, newest first)", ""]
    for r in rows:
        lines.append(_format_role_row(r))
    await message.answer("\n".join(lines), parse_mode="Markdown")
