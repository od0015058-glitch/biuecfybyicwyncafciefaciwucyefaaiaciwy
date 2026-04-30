"""referral: env-var config + payment-side wiring for the referral
codes feature.

Stage-13-Step-C. The DB-layer primitives live in ``database.py`` (look
for the ``Stage-13-Step-C: referral codes`` section). This module
sits one layer up and owns:

* ``REFERRAL_BONUS_PERCENT`` / ``REFERRAL_BONUS_MAX_USD`` env-var
  parsing with sensible defaults (10 % of the triggering top-up,
  capped at $5 per side). Defensive: NaN / Inf / negative values
  fall back to the default; sub-zero percentages would otherwise
  silently disable the feature.

* :func:`grant_referral_after_credit` — thin wrapper called from
  inside the open ``finalize_payment`` / ``finalize_partial_payment``
  transactions. Forwards the env-var bonus config so the DB function
  is config-free (easier to test).

* :func:`parse_start_payload` — pulls a ``ref_<code>`` payload out
  of a ``/start`` message text. Returns the bare code or ``None``
  for any malformed input. Stage-13-Step-C also fixes the
  pre-existing latent bug that ``cmd_start`` ignored ``/start
  <payload>`` entirely; this is the parser the handler now consults.

* :func:`build_share_url` — builds the deep-link a user pastes
  to friends (``https://t.me/<bot_username>?start=ref_<code>``).
  ``BOT_USERNAME`` env var is the canonical source; we don't call
  ``bot.get_me()`` at handler time because that adds a network
  round-trip on every wallet-screen render.
"""

from __future__ import annotations

import logging
import math
import os
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg  # noqa: F401

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# env-var config
# ----------------------------------------------------------------------


_DEFAULT_REFERRAL_BONUS_PERCENT = 10.0
_DEFAULT_REFERRAL_BONUS_MAX_USD = 5.0


def _safe_float_env(name: str, default: float) -> float:
    """Read *name* from ``os.environ`` as float, falling back to
    *default* on missing / malformed / non-finite / non-positive
    values. Logs a WARNING on the fallback so misconfigured deploys
    surface in ops logs instead of silently disabling the feature.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        log.warning(
            "%s is not a valid float (%r); falling back to %s",
            name, raw, default,
        )
        return default
    if not math.isfinite(value) or value <= 0:
        log.warning(
            "%s must be finite + positive (got %r); falling back to %s",
            name, value, default,
        )
        return default
    return value


def get_referral_bonus_percent() -> float:
    """Lazy env-var read so tests can monkeypatch ``os.environ``
    between cases without re-importing the module.
    """
    return _safe_float_env(
        "REFERRAL_BONUS_PERCENT", _DEFAULT_REFERRAL_BONUS_PERCENT
    )


def get_referral_bonus_max_usd() -> float:
    return _safe_float_env(
        "REFERRAL_BONUS_MAX_USD", _DEFAULT_REFERRAL_BONUS_MAX_USD
    )


# ----------------------------------------------------------------------
# /start <payload> parser  (Stage-13-Step-C bundled bug fix)
# ----------------------------------------------------------------------


# Telegram deep-link payloads are base64url-style (alphanumeric +
# ``-`` / ``_``). Our referral codes live inside the curated
# ``Database.REFERRAL_CODE_ALPHABET`` (uppercase ASCII alphanumeric
# minus visually-ambiguous characters) but other payload prefixes may
# carry richer characters — we accept the broad alphabet here and
# lean on the DB lookup to reject unknowns.
_REFERRAL_PAYLOAD_RE = re.compile(r"^ref_([A-Za-z0-9_-]{1,64})$")


def parse_start_payload(text: str | None) -> str | None:
    """Return the bare ``<arg>`` from a ``/start <arg>`` message text,
    or ``None`` if there is no payload.

    Telegram delivers the deep-link payload as the second
    whitespace-separated token of the ``/start`` message body. Empty
    payload, missing payload, or whitespace-only payload all return
    ``None``. Length-bounded to 64 chars: Telegram itself caps the
    deep-link payload at 64 base64url chars, so anything longer is
    junk we should not round-trip to the DB.
    """
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    if not payload or len(payload) > 64:
        return None
    return payload


def parse_referral_payload(text: str | None) -> str | None:
    """Extract a referral *code* from a ``/start ref_<code>`` payload,
    or ``None`` if the payload is missing / malformed / not a referral
    payload. Other payload shapes (e.g. a future ``promo_<code>``)
    return ``None`` here so unrelated handlers can match independently.
    """
    payload = parse_start_payload(text)
    if payload is None:
        return None
    match = _REFERRAL_PAYLOAD_RE.match(payload)
    if match is None:
        return None
    return match.group(1)


# ----------------------------------------------------------------------
# share URL builder
# ----------------------------------------------------------------------


def get_bot_username() -> str | None:
    """Bot's ``@username`` without the leading ``@``, read from
    ``BOT_USERNAME`` env var. Returns ``None`` if unset — callers
    fall back to a copy-paste-only flow (display the code without a
    deep link).

    Why an env var instead of ``bot.get_me()``? The wallet-screen
    render path is sync-ish (one DB read + a template format) and we
    don't want to add a Telegram round-trip for a value that never
    changes for the lifetime of the deployment. Setting the env var
    is documented in ``.env.example``.
    """
    raw = os.getenv("BOT_USERNAME", "").strip()
    if not raw:
        return None
    if raw.startswith("@"):
        raw = raw[1:]
    if not raw:
        return None
    return raw


def build_share_url(code: str) -> str | None:
    """Build the ``https://t.me/<bot>?start=ref_<code>`` deep link, or
    ``None`` if ``BOT_USERNAME`` is not configured.
    """
    username = get_bot_username()
    if username is None:
        return None
    return f"https://t.me/{username}?start=ref_{code}"


# ----------------------------------------------------------------------
# payment-side wiring — called from inside the open finalize TX
# ----------------------------------------------------------------------


async def grant_referral_after_credit(
    db,
    connection: "asyncpg.Connection",
    *,
    invitee_telegram_id: int,
    amount_usd: float,
    transaction_id: int | None,
) -> dict | None:
    """If *invitee* has a PENDING referral grant, flip it to PAID and
    credit both wallets inside the same DB transaction as the
    triggering top-up. Returns the credit info on success, or ``None``
    if there's nothing to do (no pending grant, non-finite amount, or
    bonus rounds to zero).

    Wraps :meth:`Database._grant_referral_in_tx` with the env-var
    config; the DB primitive is config-free so it stays trivially
    testable. Errors propagate — the open TX is the one that's
    crediting the original top-up, and we don't want to swallow a
    DB error here that would leave the grant in a half-flipped state
    against a successfully credited wallet.
    """
    return await db._grant_referral_in_tx(
        connection,
        invitee_telegram_id=invitee_telegram_id,
        amount_usd=amount_usd,
        transaction_id=transaction_id,
        bonus_percent=get_referral_bonus_percent(),
        bonus_max_usd=get_referral_bonus_max_usd(),
    )


__all__ = [
    "build_share_url",
    "get_bot_username",
    "get_referral_bonus_max_usd",
    "get_referral_bonus_percent",
    "grant_referral_after_credit",
    "parse_referral_payload",
    "parse_start_payload",
]
