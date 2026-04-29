"""TetraPay (Rial card / Shaparak) gateway integration.

Stage-11-Step-C. TetraPay is an Iranian card-payment PSP that accepts
IRR via Shaparak (the country's interbank network) and posts a
callback to our webhook on settlement. Two HTTP endpoints, both
JSON, both keyed off a per-merchant ``ApiKey``:

* ``POST https://tetra98.com/api/create_order``

  Request body (JSON, application/json)::

      {
          "ApiKey":      "<merchant key>",
          "Hash_id":     "<merchant-issued unique order id>",
          "Amount":      <integer, IRR>,
          "Description": "<order description>",
          "Email":       "<optional>",
          "Mobile":      "<optional>",
          "CallbackURL": "https://<our host>/tetrapay-webhook"
      }

  Response (JSON)::

      {
          "status":          "100",
          "payment_url_web": "https://tetra98.com/pay/<token>",
          "Authority":       "<gateway-issued opaque token>",
          "tracking_id":     "<gateway-issued tracking id>"
      }

  ``status == "100"`` is success; anything else is an error.

* ``POST https://tetra98.com/api/verify``

  Request body (JSON)::

      { "ApiKey": "<merchant key>", "Authority": "<from create_order>" }

  Response (JSON)::

      { "status": "100", "RefID": "<bank reference id>" }

  ``status == "100"`` confirms the order was settled. We MUST call
  this from the webhook handler before crediting; the user's browser
  callback alone is not authoritative (a malicious user can craft a
  POST to our webhook with a guessed Authority — verify is the
  authoritative settlement check, same defensive pattern as the
  NowPayments IPN signature path).

Webhook payload (POST to our CallbackURL on settlement)::

    { "status": "100", "hash_id": "<our Hash_id>", "authority": "<...>" }

Replay / idempotency: dedupe on ``Authority`` (TetraPay's stable
per-order id, mapped to ``transactions.gateway_invoice_id`` with the
same ``UNIQUE`` constraint that protects the NowPayments path).

Money invariant: the credit amount is the USD equivalent locked at
order-creation time, NOT recomputed at settlement. This protects the
user from rial-rate moves between entry and settlement (Iranian banks
regularly take multiple minutes for Shaparak 3DS round-trips). The
locked rate is recorded in
``transactions.gateway_locked_rate_toman_per_usd`` for audit; the
locked USD figure already lives in ``transactions.amount_usd_credited``
and is what ``database.finalize_payment`` credits.

The conversion math at order-creation time is::

    amount_irr = round(amount_toman * 10)            # IRR = 10 * Toman
    amount_toman = round(amount_usd * locked_rate)   # toman per USD

Rounding to integer rial *before* sending to TetraPay matches what
Shaparak settlements actually do (no fractional rials exist).

Configuration (env):

* ``TETRAPAY_API_KEY`` — merchant API key from the TetraPay panel.
  Without it, :func:`create_order` refuses (returns ``None``) the
  same way the NowPayments path does for a missing
  ``NOWPAYMENTS_API_KEY``.
* ``WEBHOOK_BASE_URL`` — reused from :mod:`payments`. The callback
  URL is ``${WEBHOOK_BASE_URL}/tetrapay-webhook``.
* ``TETRAPAY_API_BASE`` — optional override of
  ``https://tetra98.com``. Useful for staging or for swapping
  endpoints if TetraPay rotates their API host.
* ``TETRAPAY_REQUEST_TIMEOUT_SECONDS`` — optional float, default
  ``10``. Same shape as the NowPayments timeout.

This module is import-safe with the env vars unset; we resolve them
lazily inside each function so tests can patch them via
``monkeypatch.setenv`` without re-importing the module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import secrets
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiohttp import web

from database import db
from strings import t


log = logging.getLogger("bot.tetrapay")


# Per-process drop counters mirroring the NowPayments IPN handler's
# pattern (see ``payments._IPN_DROP_COUNTERS``). Increments are also
# logged at error level so a misconfigured TetraPay merchant panel
# pointing at the prod webhook is visible without grep-fu.
_TETRAPAY_DROP_COUNTERS: dict[str, int] = {
    "bad_json": 0,
    "missing_authority": 0,
    "non_success_callback": 0,
    "unknown_invoice": 0,
    "verify_failed": 0,
    "replay": 0,
}


def _bump_tetrapay_drop_counter(reason: str) -> None:
    _TETRAPAY_DROP_COUNTERS[reason] = _TETRAPAY_DROP_COUNTERS.get(reason, 0) + 1


def get_tetrapay_drop_counters() -> dict[str, int]:
    """Snapshot copy of the TetraPay webhook drop counters."""
    return dict(_TETRAPAY_DROP_COUNTERS)


_DEFAULT_API_BASE = "https://tetra98.com"
_DEFAULT_TIMEOUT_SECONDS = 10.0
_SUCCESS_STATUS = "100"


def _api_base() -> str:
    return os.getenv("TETRAPAY_API_BASE", _DEFAULT_API_BASE).rstrip("/")


def _api_key() -> str:
    return os.getenv("TETRAPAY_API_KEY", "")


def _timeout_seconds() -> float:
    raw = os.getenv("TETRAPAY_REQUEST_TIMEOUT_SECONDS", "")
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        log.warning(
            "TETRAPAY_REQUEST_TIMEOUT_SECONDS=%r is not a float; "
            "falling back to %ss",
            raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    # Reject zero / negative so a typo can't accidentally disable
    # the timeout (an aiohttp ClientTimeout(total=0) means "no
    # timeout", which lets a hung TetraPay endpoint stall the
    # invoice creation indefinitely).
    if value <= 0 or not math.isfinite(value):
        log.warning(
            "TETRAPAY_REQUEST_TIMEOUT_SECONDS=%r is not positive; "
            "falling back to %ss",
            raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    return value


def _callback_url() -> str:
    base = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
    if not base:
        return ""
    return f"{base}/tetrapay-webhook"


def generate_hash_id() -> str:
    """Generate a fresh per-order ``Hash_id`` (merchant-side unique id).

    TetraPay treats this as opaque; we use 24 hex chars from
    :func:`secrets.token_hex` (96 bits of entropy) — enough that
    even at a million orders/sec the collision probability stays
    negligible. The function is its own module-level helper so
    tests can monkeypatch it for deterministic Hash_id values.
    """
    return secrets.token_hex(12)


@dataclass(frozen=True)
class TetraPayOrder:
    """Result of a successful :func:`create_order` call.

    Attributes:
        authority: Gateway-issued opaque token that uniquely
            identifies the order. Stored on the
            ``transactions.gateway_invoice_id`` column; the unique
            constraint there is what de-dupes webhook replays.
        payment_url: The browser-facing redirect URL the user must
            visit to complete the card payment.
        hash_id: The merchant-side ``Hash_id`` we sent. Echoed back
            in the webhook so we can correlate independently of
            ``Authority``.
        tracking_id: TetraPay-side tracking id. Stored verbatim in
            logs / admin views; not used for routing.
        amount_irr: The integer rial figure we sent.
        locked_rate_toman_per_usd: The USD→Toman rate we used to
            compute ``amount_irr``. Stored on
            ``transactions.gateway_locked_rate_toman_per_usd`` for
            audit.
        amount_usd: The locked USD figure that will be credited on
            settlement. Stored on ``transactions.amount_usd_credited``.
    """

    authority: str
    payment_url: str
    hash_id: str
    tracking_id: str
    amount_irr: int
    locked_rate_toman_per_usd: float
    amount_usd: float


class TetraPayError(Exception):
    """Base exception for TetraPay create / verify failures.

    Carries the gateway-reported ``status`` (``"100"``-style code) and
    the raw response body for diagnostics. Caller decides whether to
    retry or refuse — for now we treat every non-``"100"`` as a
    refusal (no retries) since TetraPay's status codes are not
    publicly documented and a retry on a deterministic rejection is
    just noise.
    """

    def __init__(self, status: str | None, message: str, *, body: Any = None):
        self.status = status
        self.body = body
        super().__init__(message)


def usd_to_irr_amount(amount_usd: float, rate_toman_per_usd: float) -> int:
    """Convert a USD figure to an integer rial amount at *rate_toman_per_usd*.

    Rounding rule: round to the nearest integer rial. Shaparak doesn't
    settle fractional rials, so any sub-rial precision would be lost
    on the gateway side anyway. Rounding *here* (rather than at the
    aiohttp layer) makes the audit trail explicit:
    ``transactions.amount_crypto_or_rial`` matches what we sent.

    Refuses non-finite / non-positive inputs (raises ``ValueError``);
    callers should not be passing those, but the same defense-in-depth
    guard that protects ``finalize_payment`` and
    ``create_pending_transaction`` lives here too so a corrupted FSM
    state can't slip through.
    """
    if not math.isfinite(amount_usd) or amount_usd <= 0:
        raise ValueError(
            f"amount_usd must be finite and positive (got {amount_usd!r})"
        )
    if not math.isfinite(rate_toman_per_usd) or rate_toman_per_usd <= 0:
        raise ValueError(
            "rate_toman_per_usd must be finite and positive "
            f"(got {rate_toman_per_usd!r})"
        )
    # Toman → Rial: ×10. We round at the Toman step first to avoid
    # carrying USD-precision noise into the rial figure.
    amount_toman = round(amount_usd * rate_toman_per_usd)
    return int(amount_toman) * 10


async def create_order(
    *,
    amount_usd: float,
    rate_toman_per_usd: float,
    description: str,
    user_id: int,
    email: str | None = None,
    mobile: str | None = None,
    hash_id: str | None = None,
) -> TetraPayOrder:
    """Create a TetraPay order. Returns a :class:`TetraPayOrder` on success.

    Raises :class:`TetraPayError` on any non-``"100"`` response or
    missing API key. Raises ``aiohttp.ClientError`` /
    ``asyncio.TimeoutError`` on transport failure (caller decides
    whether to surface the error to the user or retry).

    The ``user_id`` is stashed in the order ``Description`` for ops
    visibility ("which user paid this 4 000 000 IRR?") — TetraPay's
    own dashboard has no notion of our Telegram user ids otherwise.
    """
    api_key = _api_key()
    if not api_key:
        raise TetraPayError(
            None,
            "TETRAPAY_API_KEY is not set; refusing to create order.",
        )
    callback_url = _callback_url()
    if not callback_url:
        raise TetraPayError(
            None,
            "WEBHOOK_BASE_URL is not set; refusing to create TetraPay order "
            "(gateway would have nowhere to deliver the settlement callback).",
        )

    amount_irr = usd_to_irr_amount(amount_usd, rate_toman_per_usd)
    hash_id = hash_id or generate_hash_id()
    payload = {
        "ApiKey": api_key,
        "Hash_id": hash_id,
        "Amount": amount_irr,
        "Description": f"{description} (user={user_id})",
        "Email": email or "",
        "Mobile": mobile or "",
        "CallbackURL": callback_url,
    }

    url = f"{_api_base()}/api/create_order"
    timeout = aiohttp.ClientTimeout(total=_timeout_seconds())
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=timeout) as response:
            try:
                data = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                body_text = await response.text()
                raise TetraPayError(
                    None,
                    f"TetraPay create_order returned non-JSON "
                    f"(status {response.status}): {body_text[:200]!r}",
                    body=body_text,
                )

    status = str(data.get("status", "")).strip()
    if status != _SUCCESS_STATUS:
        # Don't log the api_key. Do log the Hash_id so we can correlate
        # with TetraPay support if needed.
        log.warning(
            "TetraPay create_order non-success: status=%r hash_id=%r body=%r",
            status, hash_id, data,
        )
        raise TetraPayError(
            status,
            f"TetraPay create_order failed (status={status!r})",
            body=data,
        )

    authority = str(data.get("Authority", "")).strip()
    payment_url = str(data.get("payment_url_web", "")).strip()
    tracking_id = str(data.get("tracking_id", "")).strip()
    if not authority or not payment_url:
        log.error(
            "TetraPay create_order returned status=100 but missing fields: "
            "authority=%r payment_url=%r body=%r",
            authority, payment_url, data,
        )
        raise TetraPayError(
            status,
            "TetraPay create_order succeeded but response is missing "
            "authority / payment_url",
            body=data,
        )

    return TetraPayOrder(
        authority=authority,
        payment_url=payment_url,
        hash_id=hash_id,
        tracking_id=tracking_id,
        amount_irr=amount_irr,
        locked_rate_toman_per_usd=float(rate_toman_per_usd),
        amount_usd=float(amount_usd),
    )


async def verify_payment(authority: str) -> dict[str, Any]:
    """Confirm a TetraPay order was settled. Returns the parsed verify body.

    Raises :class:`TetraPayError` on any non-``"100"`` response or
    missing API key. Raises ``aiohttp.ClientError`` /
    ``asyncio.TimeoutError`` on transport failure.

    This is the AUTHORITATIVE settlement check. The user-facing
    redirect callback alone is NOT trustworthy — a malicious user
    could POST to our webhook with a guessed Authority. We always
    call ``verify_payment`` before crediting.
    """
    api_key = _api_key()
    if not api_key:
        raise TetraPayError(
            None,
            "TETRAPAY_API_KEY is not set; refusing to verify order.",
        )
    if not authority:
        raise TetraPayError(
            None,
            "verify_payment requires a non-empty authority",
        )

    payload = {"ApiKey": api_key, "Authority": authority}
    url = f"{_api_base()}/api/verify"
    timeout = aiohttp.ClientTimeout(total=_timeout_seconds())
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=timeout) as response:
            try:
                data = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                body_text = await response.text()
                raise TetraPayError(
                    None,
                    f"TetraPay verify returned non-JSON "
                    f"(status {response.status}): {body_text[:200]!r}",
                    body=body_text,
                )

    status = str(data.get("status", "")).strip()
    if status != _SUCCESS_STATUS:
        log.warning(
            "TetraPay verify non-success: status=%r authority=%r body=%r",
            status, authority, data,
        )
        raise TetraPayError(
            status,
            f"TetraPay verify failed (status={status!r})",
            body=data,
        )
    return data


async def tetrapay_webhook(request: web.Request) -> web.Response:
    """Handle a TetraPay settlement callback.

    The flow:

    1. Parse the JSON body. A malformed body is logged + counter-bumped
       and returns 200 (we never give the gateway a reason to retry —
       a malformed body won't get better on a retry).
    2. Pull ``authority`` (required) and ``status``. Missing
       ``authority`` is unrecoverable — we have no way to correlate
       the callback to one of our PENDING rows; log + drop.
    3. Replay-dedupe via ``record_payment_status_transition`` keyed on
       ``(authority, status)``, same table the NowPayments IPN uses.
       A duplicate delivery returns ``None`` and we bail before
       calling :func:`verify_payment`.
    4. If ``status != "100"`` (user cancelled, card declined, etc.):
       drop with 200; we leave the PENDING row alone — the reaper
       (:mod:`pending_expiration`) sweeps it after 24h. We
       deliberately do NOT mark the row FAILED here because TetraPay
       sometimes sends a non-success callback that is later followed
       by a success on a successful retry of the same payment.
    5. Look up the locked USD figure on our PENDING / PARTIAL row via
       :meth:`Database.get_pending_invoice_amount_usd`. Unknown
       invoices (no PENDING row) drop with 200 — same defense as the
       NowPayments path: a webhook for an invoice we never created
       cannot mint money.
    6. Call :func:`verify_payment` — the AUTHORITATIVE settlement
       check. The user-facing callback alone is not trustworthy;
       only TetraPay's own ``/api/verify`` endpoint can confirm
       Shaparak settled the order. A verify failure drops with 200
       (so TetraPay won't retry) and bumps the counter.
    7. Call :meth:`Database.finalize_payment` with the locked USD
       figure — same idempotent finalize the NowPayments path uses.
       The wallet credit + status flip happen in one DB transaction.
    8. Best-effort Telegram notification to the user. Failures here
       must not propagate (the wallet is already credited; the
       callback won't be retried).

    Always returns HTTP 200 except on bona-fide signature / parse
    failures — TetraPay retries on non-2xx, and we want every retry
    storm to be triggered by us actually being down (5xx) not by us
    legitimately rejecting a bad callback.
    """
    try:
        raw_body = await request.read()
        try:
            data = json.loads(raw_body)
        except (ValueError, TypeError):
            _bump_tetrapay_drop_counter("bad_json")
            log.error(
                "TetraPay webhook body is not valid JSON "
                "(remote=%s, len=%d); ignoring",
                request.remote, len(raw_body),
            )
            return web.Response(status=200, text="OK")

        if not isinstance(data, dict):
            _bump_tetrapay_drop_counter("bad_json")
            log.error(
                "TetraPay webhook body is JSON but not an object "
                "(remote=%s, type=%s); ignoring",
                request.remote, type(data).__name__,
            )
            return web.Response(status=200, text="OK")

        # TetraPay's docs aren't clear on capitalisation — accept both.
        authority = (
            data.get("authority") or data.get("Authority") or ""
        ).strip()
        status = str(data.get("status", "")).strip()
        hash_id = (
            data.get("hash_id") or data.get("Hash_id") or ""
        ).strip()

        if not authority:
            _bump_tetrapay_drop_counter("missing_authority")
            log.error(
                "TetraPay webhook missing authority "
                "(remote=%s, hash_id=%r, status=%r, body=%r); ignoring",
                request.remote, hash_id, status, data,
            )
            return web.Response(status=200, text="OK")

        # User cancelled / declined — gateway will not deliver "100"
        # for this same authority later, but we still record the
        # transition for audit. PENDING row stays so the reaper can
        # sweep it after 24h. Don't gate this branch on dedupe — TetraPay
        # may legitimately re-deliver the same non-success status.
        if status != _SUCCESS_STATUS:
            _bump_tetrapay_drop_counter("non_success_callback")
            log.info(
                "TetraPay webhook non-success status=%r for authority=%s "
                "(hash_id=%r); leaving PENDING row alone for reaper / "
                "user retry",
                status, authority, hash_id,
            )
            try:
                await db.record_payment_status_transition(
                    authority, status, outcome="noop",
                    meta={
                        "remote": request.remote,
                        "gateway": "tetrapay",
                        "hash_id": hash_id,
                    },
                )
            except Exception:
                log.exception(
                    "TetraPay non-success transition record failed for "
                    "authority=%s; ignoring (non-critical audit miss)",
                    authority,
                )
            return web.Response(status=200, text="OK")

        # Look up the locked USD figure on our ledger. ``None`` means
        # one of: (a) no PENDING/PARTIAL row for this authority (forged
        # callback or pre-creation race), or (b) the row already moved
        # to a terminal status — i.e. we already credited a previous
        # webhook delivery. Both are safe drops with 200; in case (b)
        # ``finalize_payment`` would return None anyway via FOR UPDATE +
        # status-check, but bailing here saves a useless ``verify_payment``
        # round-trip on every TetraPay retry of an already-credited order.
        locked_usd = await db.get_pending_invoice_amount_usd(authority)
        if locked_usd is None:
            _bump_tetrapay_drop_counter("unknown_invoice")
            log.warning(
                "TetraPay webhook for unknown / terminal invoice "
                "authority=%s (hash_id=%r); ignoring",
                authority, hash_id,
            )
            return web.Response(status=200, text="OK")

        # AUTHORITATIVE settlement check. A user could craft a forged
        # callback to this endpoint with a guessed authority; only
        # TetraPay's verify endpoint can confirm the order is settled.
        #
        # Two failure shapes here, deliberately distinguished:
        #
        # 1. **Deterministic gateway rejection** — ``TetraPayError`` with
        #    a non-None ``status`` field means TetraPay's ``/api/verify``
        #    explicitly told us the authority is not settled. We log,
        #    record the transition for audit, and return **200** (no
        #    retry; the gateway has spoken).
        #
        # 2. **Transient infrastructure failure** —
        #    ``asyncio.TimeoutError``, ``aiohttp.ClientError`` (DNS,
        #    connection reset, etc.), or ``TetraPayError`` with
        #    ``status=None`` (non-JSON response) all indicate we have no
        #    ground truth on settlement. We bump the counter and return
        #    **500** so TetraPay retries. Crucially we do **NOT** record
        #    a ``payment_status_transitions`` row here: the dedupe table
        #    must not block a future retry that could legitimately credit
        #    the user. A timeout that recorded the (authority, "100")
        #    transition would otherwise lock the user out of their own
        #    paid invoice forever.
        try:
            await verify_payment(authority)
        except TetraPayError as exc:
            _bump_tetrapay_drop_counter("verify_failed")
            if exc.status is None:
                # Non-JSON / no status → treat as transient.
                log.error(
                    "TetraPay verify ambiguous failure for authority=%s "
                    "(hash_id=%r): %s; returning 500 to trigger retry",
                    authority, hash_id, exc,
                )
                return web.Response(status=500, text="verify failed")
            log.error(
                "TetraPay verify rejected authority=%s (hash_id=%r): "
                "status=%r %s",
                authority, hash_id, exc.status, exc,
            )
            try:
                await db.record_payment_status_transition(
                    authority, status, outcome="rejected",
                    meta={
                        "remote": request.remote,
                        "gateway": "tetrapay",
                        "hash_id": hash_id,
                        "verify_status": exc.status,
                    },
                )
            except Exception:
                log.exception(
                    "TetraPay verify-rejected transition record failed for "
                    "authority=%s; ignoring (non-critical audit miss)",
                    authority,
                )
            return web.Response(status=200, text="OK")
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            # Transient — return 500 so TetraPay retries.
            _bump_tetrapay_drop_counter("verify_failed")
            log.error(
                "TetraPay verify transport failure for authority=%s "
                "(hash_id=%r): %s; returning 500 to trigger retry",
                authority, hash_id, exc,
            )
            return web.Response(status=500, text="verify failed")

        row = await db.finalize_payment(authority, locked_usd)
        if row is None:
            log.info(
                "TetraPay finalize_payment ignored authority=%s "
                "(unknown or already finalized — likely a race with "
                "another webhook delivery)",
                authority,
            )
            return web.Response(status=200, text="OK")

        # Audit-only transition record, written AFTER successful finalize.
        # Critically NOT pre-finalize: a pre-finalize record would lock a
        # transient verify failure into permanent uncreditability (the
        # retry would drop on dedupe before reaching ``finalize_payment``
        # again). ``finalize_payment`` is itself idempotent via FOR
        # UPDATE + status-check, so it's the canonical correctness gate;
        # this row is purely for ``/admin/transactions`` history and ops.
        try:
            await db.record_payment_status_transition(
                authority, status, outcome="applied",
                meta={
                    "remote": request.remote,
                    "body_len": len(raw_body),
                    "gateway": "tetrapay",
                    "hash_id": hash_id,
                },
            )
        except Exception:
            log.exception(
                "TetraPay applied transition record failed for "
                "authority=%s; ignoring (non-critical audit miss; "
                "wallet was already credited)",
                authority,
            )

        telegram_id = row["telegram_id"]
        delta_credited = float(row["delta_credited"])
        bonus_credited = float(row.get("promo_bonus_credited") or 0.0)

        # Best-effort user notification — see NowPayments path for
        # rationale on why a Telegram failure must NOT cause a 500.
        # Mirror the NowPayments path's promo-bonus rendering: append
        # a separate ``pay_promo_bonus`` line when a bonus was credited
        # so the user sees both the base credit and the promo on top
        # of it (the credit notification template only shows the base
        # amount).
        bot: Bot = request.app["bot"]
        try:
            lang = await db.get_user_language(telegram_id)
            msg = t(
                lang or "fa",
                "tetrapay_credit_notification",
                amount=delta_credited,
            )
            if bonus_credited > 0:
                msg = msg + "\n\n" + t(
                    lang or "fa",
                    "pay_promo_bonus",
                    bonus=bonus_credited,
                )
            await bot.send_message(
                telegram_id, msg, parse_mode="Markdown",
            )
        except (TelegramForbiddenError, TelegramBadRequest):
            # User blocked the bot or the chat is gone. Wallet is
            # already credited; nothing to do.
            log.info(
                "TetraPay credit notification undeliverable for user=%s "
                "(authority=%s)",
                telegram_id, authority,
            )
        except Exception:
            log.exception(
                "TetraPay credit notification failed for user=%s "
                "(authority=%s); wallet already credited",
                telegram_id, authority,
            )

        return web.Response(status=200, text="OK")
    except Exception:
        # Outer guard so an unexpected crash returns 500 (TetraPay
        # will retry — that's what we want for a real bug). The
        # narrower handlers above return 200 for *expected* refusals.
        log.exception("TetraPay webhook crashed unexpectedly")
        return web.Response(status=500, text="Server error")


__all__ = [
    "TetraPayError",
    "TetraPayOrder",
    "create_order",
    "generate_hash_id",
    "get_tetrapay_drop_counters",
    "tetrapay_webhook",
    "usd_to_irr_amount",
    "verify_payment",
]
