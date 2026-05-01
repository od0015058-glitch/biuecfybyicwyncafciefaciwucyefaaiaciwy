"""Zarinpal (Iranian Shaparak / Rial card) gateway integration.

Stage-15-Step-E #8 first slice. Zarinpal is the largest Iranian card
PSP — an alternative to :mod:`tetrapay` that some merchants prefer
for its better OEM coverage and v4 API ergonomics. Two HTTP endpoints,
both JSON, both keyed off a per-merchant ``merchant_id`` (a 36-char
GUID-shaped string from the Zarinpal merchant panel):

* ``POST https://payment.zarinpal.com/pg/v4/payment/request.json``

  Request body (JSON, application/json)::

      {
          "merchant_id":  "<36-char merchant key>",
          "amount":       <integer, IRR>,
          "currency":     "IRR",
          "description":  "<order description>",
          "callback_url": "https://<our host>/zarinpal-callback",
          "metadata":     {"mobile": "...", "email": "..."}  # optional
      }

  Success response (HTTP 200, JSON)::

      {
          "data":   {"code": 100, "message": "Success", "authority": "A0000...",
                     "fee_type": "Merchant", "fee": 0},
          "errors": []
      }

  ``data.code == 100`` is success; anything else (including a non-empty
  ``errors`` array) is an error. ``authority`` is a 36-char opaque
  token — the same value Zarinpal will return in the user-redirect
  callback below, and what we store on
  ``transactions.gateway_invoice_id``.

* ``POST https://payment.zarinpal.com/pg/v4/payment/verify.json``

  Request body (JSON)::

      {"merchant_id": "...", "authority": "...", "amount": <integer IRR>}

  Response (JSON)::

      {
          "data":   {"code": 100, "message": "Verified",
                     "ref_id": 12345678, "card_pan": "5022-29**-****-1234",
                     "card_hash": "...", "fee_type": "Merchant", "fee": 0},
          "errors": []
      }

  ``code == 100`` confirms the order was settled. ``code == 101``
  means the order was already verified previously — also a SUCCESS
  (idempotency: Zarinpal lets you call verify twice safely). Any
  other code is a refusal. We MUST call this from the callback
  handler before crediting; the user-side redirect alone is not
  authoritative (a malicious user can craft a ``GET`` to our
  callback endpoint with a guessed Authority — verify is the
  authoritative settlement check, same defensive pattern as the
  NowPayments IPN signature path and the TetraPay verify path).

Browser-redirect callback (NOT a server-to-server webhook)::

    GET ${WEBHOOK_BASE_URL}/zarinpal-callback?Authority=A0000...&Status=OK

Zarinpal redirects the *user's browser* back to our callback URL
after they pay, with ``Authority`` and ``Status`` (``OK`` or ``NOK``)
as query parameters. There is no separate server-to-server webhook
— this differs materially from the NowPayments and TetraPay paths
where the gateway POSTs a JSON body. The handler here therefore:

  1. reads query params (not a JSON body),
  2. calls the authoritative verify,
  3. finalizes the wallet credit on success,
  4. returns a tiny HTML page that nudges the user back to Telegram
     (so they see immediate feedback rather than a blank tab).

Replay / idempotency: dedupe on ``Authority`` (Zarinpal's stable
per-order id, mapped to ``transactions.gateway_invoice_id`` with the
same ``UNIQUE`` constraint that protects the NowPayments and TetraPay
paths). The same ``payment_status_transitions(authority, status)``
dedupe table guards against a curious user refreshing the callback
URL repeatedly: only the first delivery applies, the rest no-op.

Money invariant: the credit amount is the USD equivalent locked at
order-creation time, NOT recomputed at settlement. Same rationale as
TetraPay — Iranian banks regularly take multiple minutes for Shaparak
3DS round-trips and the rial can move materially in that window.
The locked rate is recorded in
``transactions.gateway_locked_rate_toman_per_usd`` for audit; the
locked USD figure already lives in ``transactions.amount_usd_credited``
and is what ``database.finalize_payment`` credits.

The conversion math at order-creation time is identical to TetraPay::

    amount_irr = round(amount_toman * 10)            # IRR = 10 * Toman
    amount_toman = round(amount_usd * locked_rate)   # toman per USD

Configuration (env):

* ``ZARINPAL_MERCHANT_ID`` — 36-char merchant id from the Zarinpal
  panel. Without it, :func:`create_order` refuses (returns a
  :class:`ZarinpalError`) the same way the TetraPay path does for a
  missing ``TETRAPAY_API_KEY``.
* ``WEBHOOK_BASE_URL`` — reused from :mod:`payments`. The callback
  URL is ``${WEBHOOK_BASE_URL}/zarinpal-callback``.
* ``ZARINPAL_API_BASE`` — optional override of
  ``https://payment.zarinpal.com/pg``. Useful for staging or for
  Zarinpal's sandbox (``https://sandbox.zarinpal.com/pg``).
* ``ZARINPAL_REQUEST_TIMEOUT_SECONDS`` — optional float, default
  ``10``. Same shape as the TetraPay timeout.

This module is import-safe with the env vars unset; we resolve them
lazily inside each function so tests can patch them via
``monkeypatch.setenv`` without re-importing the module.

What's deliberately deferred to the next AI (per HANDOFF §11
"first-slice" working pattern):

* User-facing entry point in ``handlers.py`` (the ``💳 پرداخت با
  زرین‌پال`` button alongside the existing TetraPay button). Today
  the module is fully wired in ``main.start_webhook_server`` and
  the integration tests exercise the create / verify / callback
  triple end-to-end, but no Telegram FSM state routes a user to
  ``create_order`` yet. The next slice should mirror the existing
  ``tetrapay_topup_*`` handlers.
* Backfill reaper for users who close the browser before the
  redirect lands. Zarinpal can settle an order whose user never
  comes back — without a backfill we'd never credit them. The
  pending-expiration reaper would currently EXPIRE the row after
  24h. A small periodic task that calls ``verify_payment`` for any
  PENDING Zarinpal row older than ~5 minutes would close that gap.
* A user-facing "payment confirmed" / "payment failed" HTML page
  that's nicer than the bare-bones one this slice ships. The
  current page is intentionally minimal because the user is
  expected to flip back to Telegram for the canonical confirmation;
  a designer pass would help.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiohttp import web

from database import db
from strings import t


log = logging.getLogger("bot.zarinpal")


# Per-process drop counters mirroring the NowPayments / TetraPay
# pattern. Increments are also logged at error level so a misconfigured
# Zarinpal merchant panel pointing at the prod callback URL is visible
# without grep-fu.
_ZARINPAL_DROP_COUNTERS: dict[str, int] = {
    "missing_authority": 0,
    "non_success_callback": 0,
    "unknown_invoice": 0,
    "verify_failed": 0,
    "replay": 0,
}


def _bump_zarinpal_drop_counter(reason: str) -> None:
    _ZARINPAL_DROP_COUNTERS[reason] = _ZARINPAL_DROP_COUNTERS.get(reason, 0) + 1


def get_zarinpal_drop_counters() -> dict[str, int]:
    """Snapshot copy of the Zarinpal callback drop counters."""
    return dict(_ZARINPAL_DROP_COUNTERS)


_DEFAULT_API_BASE = "https://payment.zarinpal.com/pg"
_DEFAULT_TIMEOUT_SECONDS = 10.0
# Zarinpal v4 success codes. 100 == settled now; 101 == already
# verified previously (a duplicate verify call). Both are SUCCESS
# from our perspective: in the 101 case we still want to credit if
# we haven't yet, so the same ``finalize_payment`` path runs and is
# idempotent. Zarinpal documents 101 explicitly to make verify safe
# to call twice.
_VERIFY_SUCCESS_CODES: frozenset[int] = frozenset({100, 101})
_REQUEST_SUCCESS_CODE = 100


def _api_base() -> str:
    return os.getenv("ZARINPAL_API_BASE", _DEFAULT_API_BASE).rstrip("/")


def _merchant_id() -> str:
    return os.getenv("ZARINPAL_MERCHANT_ID", "")


def _timeout_seconds() -> float:
    raw = os.getenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "")
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        log.warning(
            "ZARINPAL_REQUEST_TIMEOUT_SECONDS=%r is not a float; "
            "falling back to %ss",
            raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    # Reject zero / negative so a typo can't accidentally disable
    # the timeout (an aiohttp ClientTimeout(total=0) means "no
    # timeout", which lets a hung Zarinpal endpoint stall the
    # invoice creation indefinitely). Same defense as the TetraPay
    # path (see ``tetrapay._timeout_seconds``).
    if value <= 0 or not math.isfinite(value):
        log.warning(
            "ZARINPAL_REQUEST_TIMEOUT_SECONDS=%r is not positive/finite; "
            "falling back to %ss",
            raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    return value


def _callback_url() -> str:
    base = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
    if not base:
        return ""
    return f"{base}/zarinpal-callback"


@dataclass(frozen=True)
class ZarinpalOrder:
    """Result of a successful :func:`create_order` call.

    Attributes:
        authority: 36-char gateway-issued opaque token that uniquely
            identifies the order. Stored on the
            ``transactions.gateway_invoice_id`` column; the unique
            constraint there is what de-dupes callback replays.
        payment_url: The browser-facing redirect URL the user must
            visit to complete the card payment.
            ``https://payment.zarinpal.com/pg/StartPay/<authority>``.
        amount_irr: The integer rial figure we sent to Zarinpal.
            We MUST pass the same value back to ``verify_payment``
            (Zarinpal compares it server-side and rejects mismatches
            as a defense against a tampered redirect).
        locked_rate_toman_per_usd: The USD→Toman rate we used to
            compute ``amount_irr``. Stored on
            ``transactions.gateway_locked_rate_toman_per_usd`` for
            audit.
        amount_usd: The locked USD figure that will be credited on
            settlement. Stored on
            ``transactions.amount_usd_credited``.
        fee_type: Who pays the gateway fee — ``"Merchant"`` or
            ``"Payer"``. Echoed for ops visibility / reconciliation.
        fee: Integer rial fee, as Zarinpal reports it. Echoed for ops.
    """

    authority: str
    payment_url: str
    amount_irr: int
    locked_rate_toman_per_usd: float
    amount_usd: float
    fee_type: str = ""
    fee: int = 0


class ZarinpalError(Exception):
    """Base exception for Zarinpal create / verify failures.

    Carries the gateway-reported integer ``code`` (100-style) and
    the raw response body for diagnostics. Caller decides whether
    to retry or refuse — for now we treat every non-success code as
    a refusal (no retries) since Zarinpal's error codes are
    deterministic per-order (a wrong merchant_id will not "fix
    itself" on a retry).
    """

    def __init__(self, code: int | None, message: str, *, body: Any = None):
        self.code = code
        self.body = body
        super().__init__(message)


def usd_to_irr_amount(amount_usd: float, rate_toman_per_usd: float) -> int:
    """Convert a USD figure to an integer rial amount at *rate_toman_per_usd*.

    Rounding rule: round to the nearest integer rial. Shaparak
    doesn't settle fractional rials, so any sub-rial precision would
    be lost on the gateway side anyway. Rounding *here* (rather than
    at the aiohttp layer) makes the audit trail explicit:
    ``transactions.amount_crypto_or_rial`` matches what we sent.

    Identical math to :func:`tetrapay.usd_to_irr_amount` so a future
    refactor that pulls the helper into a shared ``money.py`` module
    can collapse them without touching either gateway's tests.

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
) -> ZarinpalOrder:
    """Create a Zarinpal order. Returns a :class:`ZarinpalOrder` on success.

    Raises :class:`ZarinpalError` on any non-success response or
    missing merchant id. Raises ``aiohttp.ClientError`` /
    ``asyncio.TimeoutError`` on transport failure (caller decides
    whether to surface the error to the user or retry).

    The ``user_id`` is stashed in the order ``description`` for ops
    visibility ("which user paid this 4 000 000 IRR?") — Zarinpal's
    own dashboard has no notion of our Telegram user ids otherwise.
    """
    merchant_id = _merchant_id()
    if not merchant_id:
        raise ZarinpalError(
            None,
            "ZARINPAL_MERCHANT_ID is not set; refusing to create order.",
        )
    callback_url = _callback_url()
    if not callback_url:
        raise ZarinpalError(
            None,
            "WEBHOOK_BASE_URL is not set; refusing to create Zarinpal "
            "order (gateway would have nowhere to redirect the user "
            "back to after payment).",
        )

    amount_irr = usd_to_irr_amount(amount_usd, rate_toman_per_usd)
    metadata: dict[str, str] = {}
    if email:
        metadata["email"] = email
    if mobile:
        metadata["mobile"] = mobile
    payload: dict[str, Any] = {
        "merchant_id": merchant_id,
        "amount": amount_irr,
        "currency": "IRR",
        "description": f"{description} (user={user_id})",
        "callback_url": callback_url,
    }
    if metadata:
        payload["metadata"] = metadata

    url = f"{_api_base()}/v4/payment/request.json"
    timeout = aiohttp.ClientTimeout(total=_timeout_seconds())
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=timeout) as response:
            try:
                data = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                body_text = await response.text()
                raise ZarinpalError(
                    None,
                    f"Zarinpal create_order returned non-JSON "
                    f"(status {response.status}): {body_text[:200]!r}",
                    body=body_text,
                )

    if not isinstance(data, dict):
        raise ZarinpalError(
            None,
            f"Zarinpal create_order returned non-object body: {data!r}",
            body=data,
        )

    payload_data = data.get("data") if isinstance(data.get("data"), dict) else {}
    code_raw = payload_data.get("code") if payload_data else None
    try:
        code = int(code_raw) if code_raw is not None else None
    except (TypeError, ValueError):
        code = None

    if code != _REQUEST_SUCCESS_CODE:
        # Don't log the merchant_id. Do log the user_id so we can
        # correlate with Zarinpal support if needed.
        log.warning(
            "Zarinpal create_order non-success: code=%r user_id=%r body=%r",
            code, user_id, data,
        )
        raise ZarinpalError(
            code,
            f"Zarinpal create_order failed (code={code!r})",
            body=data,
        )

    authority = str(payload_data.get("authority", "")).strip()
    fee_type = str(payload_data.get("fee_type", "")).strip()
    fee_raw = payload_data.get("fee", 0)
    try:
        fee = int(fee_raw)
    except (TypeError, ValueError):
        fee = 0
    if not authority:
        log.error(
            "Zarinpal create_order returned code=100 but missing authority: "
            "body=%r",
            data,
        )
        raise ZarinpalError(
            code,
            "Zarinpal create_order succeeded but response is missing "
            "authority",
            body=data,
        )

    # Zarinpal's redirect URL is `<base>/StartPay/<authority>`. The
    # base for the user-facing redirect is the SAME host as the API
    # base by convention (so a sandbox API base implies a sandbox
    # StartPay too); we use ``urllib.parse.quote`` defensively even
    # though authorities are only [A-Za-z0-9].
    payment_url = f"{_api_base()}/StartPay/{quote(authority, safe='')}"

    return ZarinpalOrder(
        authority=authority,
        payment_url=payment_url,
        amount_irr=amount_irr,
        locked_rate_toman_per_usd=float(rate_toman_per_usd),
        amount_usd=float(amount_usd),
        fee_type=fee_type,
        fee=fee,
    )


async def verify_payment(authority: str, amount_irr: int) -> dict[str, Any]:
    """Confirm a Zarinpal order was settled. Returns the parsed verify body.

    Raises :class:`ZarinpalError` on any non-success response or
    missing merchant id. Raises ``aiohttp.ClientError`` /
    ``asyncio.TimeoutError`` on transport failure.

    This is the AUTHORITATIVE settlement check. The user-facing
    redirect callback alone is NOT trustworthy — a malicious user
    could craft a GET to our callback endpoint with a guessed
    Authority. We always call ``verify_payment`` before crediting.

    Both ``code == 100`` and ``code == 101`` are treated as success:
    100 means "settled now", 101 means "settled previously" (a
    duplicate verify call). Either way the order is confirmed paid.
    """
    merchant_id = _merchant_id()
    if not merchant_id:
        raise ZarinpalError(
            None,
            "ZARINPAL_MERCHANT_ID is not set; refusing to verify order.",
        )
    if not authority:
        raise ZarinpalError(
            None,
            "verify_payment requires a non-empty authority",
        )
    if not isinstance(amount_irr, int) or amount_irr <= 0:
        raise ZarinpalError(
            None,
            f"verify_payment requires a positive integer amount_irr "
            f"(got {amount_irr!r})",
        )

    payload = {
        "merchant_id": merchant_id,
        "authority": authority,
        "amount": amount_irr,
    }
    url = f"{_api_base()}/v4/payment/verify.json"
    timeout = aiohttp.ClientTimeout(total=_timeout_seconds())
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=timeout) as response:
            try:
                data = await response.json(content_type=None)
            except aiohttp.ContentTypeError:
                body_text = await response.text()
                raise ZarinpalError(
                    None,
                    f"Zarinpal verify returned non-JSON "
                    f"(status {response.status}): {body_text[:200]!r}",
                    body=body_text,
                )

    if not isinstance(data, dict):
        raise ZarinpalError(
            None,
            f"Zarinpal verify returned non-object body: {data!r}",
            body=data,
        )

    payload_data = data.get("data") if isinstance(data.get("data"), dict) else {}
    code_raw = payload_data.get("code") if payload_data else None
    try:
        code = int(code_raw) if code_raw is not None else None
    except (TypeError, ValueError):
        code = None

    if code not in _VERIFY_SUCCESS_CODES:
        log.warning(
            "Zarinpal verify non-success: code=%r authority=%r body=%r",
            code, authority, data,
        )
        raise ZarinpalError(
            code,
            f"Zarinpal verify failed (code={code!r})",
            body=data,
        )
    return data


# Tiny HTML pages for the user's browser. We deliberately keep these
# bare-bones — the canonical confirmation lands as a Telegram DM
# from :func:`zarinpal_callback`. The page just gives the user
# something to read while they flip back to the bot.
_HTML_SUCCESS = (
    "<!doctype html><html lang=\"fa\" dir=\"rtl\"><head>"
    "<meta charset=\"utf-8\"><title>Payment confirmed</title></head>"
    "<body style=\"font-family:sans-serif;text-align:center;"
    "padding:48px 16px\">"
    "<h1>✅ پرداخت تأیید شد</h1>"
    "<p>کیف پول شما شارژ شد. می‌توانید به ربات بازگردید.</p>"
    "</body></html>"
)
_HTML_FAILURE = (
    "<!doctype html><html lang=\"fa\" dir=\"rtl\"><head>"
    "<meta charset=\"utf-8\"><title>Payment failed</title></head>"
    "<body style=\"font-family:sans-serif;text-align:center;"
    "padding:48px 16px\">"
    "<h1>❌ پرداخت ناموفق</h1>"
    "<p>پرداخت تکمیل نشد. می‌توانید مجدداً تلاش کنید یا از ربات کمک بخواهید.</p>"
    "</body></html>"
)


def _success_response() -> web.Response:
    return web.Response(status=200, text=_HTML_SUCCESS, content_type="text/html")


def _failure_response() -> web.Response:
    return web.Response(status=200, text=_HTML_FAILURE, content_type="text/html")


async def zarinpal_callback(request: web.Request) -> web.Response:
    """Handle a Zarinpal redirect callback.

    Unlike :func:`tetrapay.tetrapay_webhook`, Zarinpal's callback is
    a USER-AGENT redirect, not a server-to-server webhook — the
    user's browser hits our callback URL with ``?Authority=...&
    Status=OK|NOK`` query parameters. We:

      1. Read the query string. ``Authority`` missing is unrecoverable
         (no row to correlate to); log + drop with the failure HTML.
      2. ``Status != OK`` (user cancelled / declined): record the
         transition for audit and return the failure HTML. The
         PENDING row stays so the reaper sweeps it after 24h.
      3. Look up the locked USD figure on our ledger via
         :meth:`Database.get_pending_invoice_amount_usd` (same helper
         the TetraPay path uses). Unknown invoices drop with the
         failure HTML.
      4. Look up the locked rial figure too — Zarinpal requires us
         to send it on the verify call. We read it back from
         :meth:`Database.get_pending_invoice_amount_irr`.
      5. Call :func:`verify_payment` — the AUTHORITATIVE settlement
         check. A verify failure drops with the failure HTML and
         records the transition (so a refresh-loop on the same
         Authority no-ops).
      6. Call :meth:`Database.finalize_payment` with the locked USD
         figure. Same idempotent finalize the TetraPay path uses.
      7. Best-effort Telegram notification to the user. Failures
         here must not propagate (the wallet is already credited).
      8. Return the success HTML.

    The HTTP status is always 200 — Zarinpal does not retry the
    user-redirect, so a 4xx/5xx wouldn't trigger a re-delivery, it
    would just confuse the user with a broken-looking page.
    """
    try:
        # Zarinpal's docs use mixed casing for the query param names.
        # Accept both common spellings; ``request.query`` is
        # case-sensitive so we have to fan out manually.
        authority = (
            request.query.get("Authority")
            or request.query.get("authority")
            or ""
        ).strip()
        status = (
            request.query.get("Status")
            or request.query.get("status")
            or ""
        ).strip()

        if not authority:
            _bump_zarinpal_drop_counter("missing_authority")
            log.error(
                "Zarinpal callback missing Authority "
                "(remote=%s, status=%r); ignoring",
                request.remote, status,
            )
            return _failure_response()

        # User cancelled / declined — Status=NOK (or anything not
        # exactly "OK"). Record the transition for audit; PENDING
        # row stays so the reaper sweeps it after 24h.
        if status.upper() != "OK":
            _bump_zarinpal_drop_counter("non_success_callback")
            log.info(
                "Zarinpal callback non-success status=%r for authority=%s; "
                "leaving PENDING row alone for reaper / user retry",
                status, authority,
            )
            try:
                await db.record_payment_status_transition(
                    authority, status.upper() or "NOK", outcome="noop",
                    meta={
                        "remote": request.remote,
                        "gateway": "zarinpal",
                    },
                )
            except Exception:
                log.exception(
                    "Zarinpal non-success transition record failed for "
                    "authority=%s; ignoring (non-critical audit miss)",
                    authority,
                )
            return _failure_response()

        # Look up the locked USD figure on our ledger. ``None`` means
        # one of: (a) no PENDING/PARTIAL row for this authority
        # (forged callback or pre-creation race), or (b) the row
        # already moved to a terminal status — i.e. we already
        # credited a previous callback delivery (refresh-loop). Both
        # are safe drops; in case (b) ``finalize_payment`` would
        # return None anyway via FOR UPDATE + status-check, but
        # bailing here saves a useless ``verify_payment`` round-trip
        # on every refresh of an already-credited order.
        locked_usd = await db.get_pending_invoice_amount_usd(authority)
        if locked_usd is None:
            _bump_zarinpal_drop_counter("unknown_invoice")
            log.warning(
                "Zarinpal callback for unknown / terminal invoice "
                "authority=%s; ignoring",
                authority,
            )
            # If the row was already finalized we want to show success;
            # if it's truly unknown we want to show failure. We can't
            # tell from this code path without an extra DB read, so we
            # err on the side of "no progress" by showing the failure
            # page. The user will see their Telegram credit DM if they
            # already paid; the page is purely cosmetic at this point.
            return _failure_response()

        # Zarinpal requires us to send the original ``amount`` on the
        # verify call. Recovering it from the PENDING row keeps the
        # verify deterministic even if the in-memory FX cache rotated
        # between order-creation and callback (which is exactly the
        # scenario that motivated the locked-USD invariant for the
        # credit side). The rial figure is stored verbatim in
        # ``transactions.amount_crypto_or_rial`` for both gateways.
        locked_irr = await db.get_pending_invoice_amount_irr(authority)
        if locked_irr is None:
            _bump_zarinpal_drop_counter("unknown_invoice")
            log.error(
                "Zarinpal callback authority=%s has a PENDING USD figure "
                "but no rial figure; refusing to verify (would send the "
                "wrong amount and Zarinpal would reject the verify)",
                authority,
            )
            return _failure_response()

        # AUTHORITATIVE settlement check. A user could craft a forged
        # callback to this endpoint with a guessed authority; only
        # Zarinpal's verify endpoint can confirm the order is settled.
        try:
            await verify_payment(authority, locked_irr)
        except ZarinpalError as exc:
            _bump_zarinpal_drop_counter("verify_failed")
            log.error(
                "Zarinpal verify rejected authority=%s: code=%r %s",
                authority, exc.code, exc,
            )
            try:
                await db.record_payment_status_transition(
                    authority, "verify_failed", outcome="rejected",
                    meta={
                        "remote": request.remote,
                        "gateway": "zarinpal",
                        "verify_code": exc.code,
                    },
                )
            except Exception:
                log.exception(
                    "Zarinpal verify-rejected transition record failed "
                    "for authority=%s; ignoring (non-critical audit miss)",
                    authority,
                )
            return _failure_response()
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            # Transient — but the Zarinpal callback is a user-redirect,
            # not a retried server-to-server webhook. We can't ask the
            # user to come back; we have to accept that this delivery
            # is lost and rely on a future backfill reaper to credit
            # the user. Document this gap here so the next AI building
            # the reaper sees the test pin.
            _bump_zarinpal_drop_counter("verify_failed")
            log.error(
                "Zarinpal verify transport failure for authority=%s: %s; "
                "user will need to be credited via backfill reaper",
                authority, exc,
            )
            return _failure_response()

        row = await db.finalize_payment(authority, locked_usd)
        if row is None:
            log.info(
                "Zarinpal finalize_payment ignored authority=%s "
                "(unknown or already finalized — likely a refresh-loop "
                "on the callback URL)",
                authority,
            )
            _bump_zarinpal_drop_counter("replay")
            # The user already saw a credit DM on the first delivery;
            # show success again so they don't think the second tab
            # broke something.
            return _success_response()

        # Audit-only transition record, written AFTER successful
        # finalize. Same rationale as TetraPay: pre-finalize would
        # lock a transient verify failure into permanent
        # uncreditability. ``finalize_payment`` is itself idempotent
        # via FOR UPDATE + status-check.
        try:
            await db.record_payment_status_transition(
                authority, "OK", outcome="applied",
                meta={
                    "remote": request.remote,
                    "gateway": "zarinpal",
                },
            )
        except Exception:
            log.exception(
                "Zarinpal applied transition record failed for "
                "authority=%s; ignoring (non-critical audit miss; "
                "wallet was already credited)",
                authority,
            )

        telegram_id = row["telegram_id"]
        delta_credited = float(row["delta_credited"])
        bonus_credited = float(row.get("promo_bonus_credited") or 0.0)

        # Best-effort user notification — see TetraPay path for
        # rationale on why a Telegram failure must NOT cause an
        # error response (the wallet is already credited).
        bot: Bot = request.app["bot"]
        try:
            lang = await db.get_user_language(telegram_id)
            msg = t(
                lang or "fa",
                "zarinpal_credit_notification",
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
            log.info(
                "Zarinpal credit notification undeliverable for user=%s "
                "(authority=%s)",
                telegram_id, authority,
            )
        except Exception:
            log.exception(
                "Zarinpal credit notification failed for user=%s "
                "(authority=%s); wallet already credited",
                telegram_id, authority,
            )

        return _success_response()
    except Exception:
        # Outer guard so an unexpected crash returns the failure HTML.
        # Unlike TetraPay we do NOT return 500 — Zarinpal's callback
        # is a user-redirect, and a 5xx page just looks broken to the
        # user. Log loudly so an operator catches the bug.
        log.exception("Zarinpal callback crashed unexpectedly")
        return _failure_response()


# JSON-serialisability sanity-check helper — used only by the test
# suite to confirm we're never about to send a non-JSON-encodable
# value to Zarinpal. Module-level so it's importable without the
# overhead of constructing an aiohttp client. Not part of the public
# surface; exposed for tests only.
def _ensure_json_encodable(payload: dict[str, Any]) -> None:
    json.dumps(payload, ensure_ascii=False)


__all__ = [
    "ZarinpalError",
    "ZarinpalOrder",
    "create_order",
    "get_zarinpal_drop_counters",
    "usd_to_irr_amount",
    "verify_payment",
    "zarinpal_callback",
]
