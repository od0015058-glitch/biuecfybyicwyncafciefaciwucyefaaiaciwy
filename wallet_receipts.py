"""wallet_receipts: render the user-facing top-up history.

Stage-12-Step-C. The wallet is denominated in USD; every gateway
ultimately credits the user in USD. But the user thinks in either
crypto (NowPayments → "I sent 0.0001 BTC") or rial (TetraPay → "I
paid 4,200,000 تومان"). A single "$5.00 — completed 2024-03-12"
line is unsatisfying — the user wants to verify against their bank
or wallet history.

This module owns the per-row formatting for the new "🧾 Recent
top-ups" wallet sub-screen. The data layer is
:meth:`Database.list_user_transactions` (status whitelist
``{"SUCCESS", "PARTIAL", "REFUNDED"}``); the bot handler in
:mod:`handlers` reads the env-driven page size, calls the DB method,
and asks this module to format each row.

Rendering rules:

* USD figure is the credit (``amount_usd_credited``) — what landed
  on their balance, not what they sent. For PARTIAL rows that's
  the partial credit, which is the genuinely useful number; for
  REFUNDED rows it's the original credit (matching what we debited
  during the refund).
* Crypto rows show the *currency they paid in* (BTC, USDT-TRC20…)
  alongside the USD figure. We don't render the on-chain amount
  (``amount_crypto_or_rial``) because users habitually think in USD
  on the Telegram side and the chain amount is rarely useful here.
* TetraPay rows show the rial-equivalent locked at order-creation
  time (``gateway_locked_rate_toman_per_usd × amount_usd``) — *not*
  the live snapshot. The locked rate is what the user actually
  paid; showing the live rate would mislead them into thinking
  they overpaid / underpaid.
* Status badge: ✅ for SUCCESS, ⚠️ for PARTIAL (partial credit, the
  user paid less than the invoiced amount but we still credited
  what landed), 🔄 for REFUNDED.
* Date is the most-relevant timestamp for the status — for SUCCESS
  / PARTIAL it's ``completed_at`` (when the credit landed); for
  REFUNDED it's ``refunded_at`` (when the refund went through).
  Both fall back to ``created_at`` if the more-specific timestamp
  is null (legacy rows).

We deliberately don't render the ``gateway_invoice_id`` — it's an
internal cursor, not something a user verifies against. Admin
panels still surface it for forensics.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Iterable

from strings import t


log = logging.getLogger("bot.wallet_receipts")


# ---------------------------------------------------------------------
# env-driven page size
# ---------------------------------------------------------------------

# Defaults documented in HANDOFF Stage-12-Step-C and surfaced via
# ``.env.example``. Default 5 — the wallet sub-screen is a
# Telegram message, not a full page; 5 receipts fit comfortably
# under the keyboard without scrolling. Cap at 20 to stay well
# under Telegram's 4 KB message limit even with the longest
# rendered line (TetraPay rows with locked-rate Toman annotations
# are the longest).
RECEIPTS_PAGE_SIZE_DEFAULT: int = 5
RECEIPTS_PAGE_SIZE_MAX: int = 20


def get_receipts_page_size() -> int:
    """Read ``RECEIPTS_PAGE_SIZE`` env, clamp to ``[1, MAX]``.

    A deploy-time typo logs and falls back to the default rather
    than crashing the wallet handler. Mirrors the helper pattern in
    :mod:`pending_alert` / :mod:`pending_expiration`.
    """
    raw = os.getenv("RECEIPTS_PAGE_SIZE", "").strip()
    if not raw:
        return RECEIPTS_PAGE_SIZE_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        log.error(
            "RECEIPTS_PAGE_SIZE=%r is not an integer; using default %d",
            raw, RECEIPTS_PAGE_SIZE_DEFAULT,
        )
        return RECEIPTS_PAGE_SIZE_DEFAULT
    return max(1, min(value, RECEIPTS_PAGE_SIZE_MAX))


# ---------------------------------------------------------------------
# rendering helpers
# ---------------------------------------------------------------------

# Status → status-badge string-table key. Ordering of the dict is
# deterministic for test pinning.
_STATUS_KEYS: dict[str, str] = {
    "SUCCESS": "receipts_status_success",
    "PARTIAL": "receipts_status_partial",
    "REFUNDED": "receipts_status_refunded",
}


def _format_receipt_date(row: dict, lang: str | None) -> str:
    """Pick the most-relevant timestamp and format it.

    For SUCCESS / PARTIAL: ``completed_at`` is the credit time.
    For REFUNDED: ``refunded_at`` is when the refund landed.
    Both fall back to ``created_at`` for legacy rows where the
    more-specific timestamp is null.

    The string is the ISO-8601 date portion (``YYYY-MM-DD``) — the
    user's local time-of-day depends on their device, and for a
    receipt list the day is what matters. We deliberately don't
    pull pytz / babel for this; the receipt is read-only and the
    extra dependency surface isn't worth the locale precision.
    """
    status = row.get("status") or ""
    iso: str | None
    if status == "REFUNDED":
        iso = (
            row.get("refunded_at")
            or row.get("completed_at")
            or row.get("created_at")
        )
    else:
        iso = row.get("completed_at") or row.get("created_at")
    if not iso:
        return "—"
    # ``isoformat`` outputs ``YYYY-MM-DDTHH:MM:SS[…]``. The first 10
    # chars are always the date.
    return iso[:10]


def _format_gateway_label(row: dict) -> str:
    """Gateway-friendly label for the receipt's secondary line.

    NowPayments rows surface the *crypto currency* the user actually
    paid in (``USDT-TRC20``, ``BTC``, …); the underlying string
    table doesn't have a per-currency translation so we render the
    upper-cased token as-is. TetraPay rows render as ``TetraPay``
    (the Iranian Shaparak / rial gateway). ``admin`` and ``gift``
    rows are surfaced in the wallet receipts feed for transparency
    when an admin manually credited or a gift-code redemption
    landed — they show ``Manual credit`` / ``Gift code``.
    """
    gateway = (row.get("gateway") or "").lower()
    currency = row.get("currency") or ""
    if gateway == "nowpayments":
        # E.g. "USDT-TRC20" — the user-facing crypto token.
        return currency.upper() if currency else "Crypto"
    if gateway == "tetrapay":
        return "TetraPay"
    if gateway == "admin":
        return "Manual credit"
    if gateway == "gift":
        return "Gift code"
    # Fallback for a future gateway addition: render the raw token.
    return gateway or "—"


def _format_toman_for_tetrapay(row: dict) -> str | None:
    """Render the rial-equivalent for a TetraPay row at the locked rate.

    *Not* the live snapshot rate. The ``gateway_locked_rate_toman_per_usd``
    is captured at order-creation time and is what the user
    actually paid. Showing the live rate on a months-old receipt
    would mislead the reader.

    Returns the formatted ``"≈ 412,500 TMN"`` block (no leading
    newline — the caller decides how to compose lines), or ``None``
    when the locked rate is missing (legacy TetraPay rows pre
    Stage-11-Step-C alembic 0011) or non-finite.
    """
    rate = row.get("gateway_locked_rate_toman_per_usd")
    amount = row.get("amount_usd")
    if not isinstance(rate, (int, float)) or not isinstance(
        amount, (int, float)
    ):
        return None
    if not math.isfinite(rate) or not math.isfinite(amount):
        return None
    if rate <= 0 or amount < 0:
        return None
    toman = amount * rate
    if not math.isfinite(toman):
        return None
    return f"≈ {toman:,.0f} TMN"


def format_receipt_line(row: dict, lang: str | None) -> str:
    """Render a single receipt as a one-or-two-line bullet.

    Shape (English locale)::

        ✅ $5.00 — USDT-TRC20 — 2024-03-12

    For TetraPay rows, the locked rial equivalent is appended in
    parentheses::

        ✅ $5.00 — TetraPay (≈ 412,500 TMN) — 2024-03-12

    Defensive against malformed rows: a missing / NULL / non-finite
    amount renders as ``$0.00`` (matches :mod:`wallet_display`'s
    NaN-defense policy); a missing status renders without a badge.
    """
    status = row.get("status") or ""
    badge_key = _STATUS_KEYS.get(status)
    badge = t(lang, badge_key) if badge_key else ""

    amount = row.get("amount_usd")
    if not isinstance(amount, (int, float)) or not math.isfinite(amount):
        amount_str = "$0.00"
    else:
        amount_str = f"${amount:.2f}"

    gateway_label = _format_gateway_label(row)
    if (row.get("gateway") or "").lower() == "tetrapay":
        toman = _format_toman_for_tetrapay(row)
        if toman:
            gateway_label = f"{gateway_label} ({toman})"

    date_str = _format_receipt_date(row, lang)

    parts = [p for p in (badge, amount_str, gateway_label, date_str) if p]
    return " — ".join(parts)


def format_receipts_page(
    rows: Iterable[dict], lang: str | None
) -> str:
    """Render an iterable of rows as a newline-joined block of bullets.

    Empty iterable → empty string; the caller is responsible for
    emitting the empty-state message instead.
    """
    return "\n".join(format_receipt_line(r, lang) for r in rows)
