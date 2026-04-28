import asyncio
import hashlib
import hmac
import json
import logging
import os

import aiohttp
from aiogram import Bot
from aiohttp import web

from database import db
from strings import t

log = logging.getLogger("bot.payments")

NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY")
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET")

# How long we trust a cached min-amount lookup before re-querying the
# NowPayments API. The minimums move with the underlying network
# fee + spot price, so a stale value can falsely accept an invoice
# that NowPayments will then reject. 1h is conservative.
_MIN_AMOUNT_CACHE_TTL_SECONDS = 3600
_min_amount_cache: dict[str, tuple[float | None, float]] = {}


class MinAmountError(Exception):
    """NowPayments rejected the invoice as below the per-currency minimum.

    Raised from :func:`create_crypto_invoice` when the API returns a
    400 with ``"amountTo is too small"`` (the only currency-specific
    BAD_REQUEST we can recover from cleanly).

    Carries the offending pay-currency symbol and, when we can fetch
    it, the current minimum in USD so the handler can render a precise
    user-facing message. ``min_usd`` is ``None`` when the min-amount
    lookup itself failed (network error, malformed response, etc.).
    """

    def __init__(self, currency: str, min_usd: float | None):
        self.currency = currency
        self.min_usd = min_usd
        msg = f"NowPayments min-amount not met for {currency!s}"
        if min_usd is not None:
            msg += f" (min ${min_usd:.2f})"
        super().__init__(msg)

# Public base URL where this bot is reachable (HTTPS in production).
# Example: https://bot.example.com  -> IPN posts to /nowpayments-webhook there.
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
if not WEBHOOK_BASE_URL:
    log.warning(
        "WEBHOOK_BASE_URL is not set; NowPayments will not be able to deliver "
        "IPN callbacks until it is configured."
    )
CALLBACK_URL = f"{WEBHOOK_BASE_URL}/nowpayments-webhook" if WEBHOOK_BASE_URL else ""


def _hmac_sha512_hex(secret: str, data: bytes) -> str:
    """Lowercase hex digest of HMAC-SHA512 over ``data`` keyed by ``secret``."""
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha512).hexdigest()


def _canonicalize_ipn_body(raw_body: bytes) -> bytes | None:
    """Re-serialize the IPN body in NowPayments' canonical form.

    Recursively sorted keys, no whitespace separators, and — critically —
    ``ensure_ascii=False`` so non-ASCII characters (e.g. the Persian
    ``order_description`` "شارژ کیف پول") stay as raw UTF-8 instead of
    being escaped into ``\\uXXXX``. The default ``ensure_ascii=True``
    inflated the canonical body by ~40 bytes vs. what NowPayments
    actually signed, which is the bug PR #39 fixed.

    Returns ``None`` if the body isn't valid JSON.
    """
    try:
        payload = json.loads(raw_body)
    except (ValueError, TypeError):
        return None
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def verify_ipn_signature(
    raw_body: bytes,
    signature_header: str | None,
    *,
    secret: str | None = None,
) -> bool:
    """Verify the x-nowpayments-sig HMAC-SHA512 signature.

    Two-pass verifier (defense in depth against canonicalization drift):

    1. **Raw-body pass.** HMAC the bytes exactly as they came off the
       wire. Mature webhook handlers (Stripe, GitHub, Paddle) all sign
       the raw request body so the receiver doesn't have to guess the
       sender's canonicalization rules. This is the path that should
       hit in production.

    2. **Canonicalized pass.** If the raw-body HMAC doesn't match, fall
       back to re-serializing the parsed JSON with sorted keys and no
       whitespace and HMAC that. This matches NowPayments' historical
       documented behaviour (their PHP example does ``ksort`` +
       ``json_encode`` before signing) and protects us if anything
       upstream of us — a reverse proxy, an HTTP client, an aiohttp
       quirk — rewrote whitespace or key order between NowPayments and
       us.

    If neither matches we log diagnostics (first/last 8 hex chars of
    each candidate digest plus body lengths — not enough to forge a
    signature, enough to tell secret-mismatch from canonicalization
    drift) and return False. The caller then 401s the request.

    The ``secret`` kwarg exists so tests can inject a known IPN secret
    without touching env vars; production callers should leave it
    unset and let it resolve to ``NOWPAYMENTS_IPN_SECRET``.
    """
    secret = secret if secret is not None else NOWPAYMENTS_IPN_SECRET
    if not secret:
        log.error("NOWPAYMENTS_IPN_SECRET is not set; refusing to process IPN.")
        return False
    if not signature_header:
        log.warning("IPN request had no x-nowpayments-sig header.")
        return False

    received = signature_header.lower()

    raw_digest = _hmac_sha512_hex(secret, raw_body)
    if hmac.compare_digest(raw_digest, received):
        return True

    canonical = _canonicalize_ipn_body(raw_body)
    canonical_digest = (
        _hmac_sha512_hex(secret, canonical) if canonical is not None else None
    )
    if canonical_digest is not None and hmac.compare_digest(
        canonical_digest, received
    ):
        return True

    # Diagnostics: which candidate were we computing, how do their
    # short prefixes compare to what NowPayments sent, and how long
    # were the bodies. Useful when the next deployment still mismatches
    # and we need to know whether it's a secret problem or a
    # canonicalization problem.
    log.warning(
        "IPN sig mismatch: raw=%s..%s canonical=%s received=%s..%s "
        "secret_len=%d body_len=%d canonical_len=%s",
        raw_digest[:8],
        raw_digest[-8:],
        f"{canonical_digest[:8]}..{canonical_digest[-8:]}"
        if canonical_digest
        else "n/a",
        received[:8],
        received[-8:],
        len(secret),
        len(raw_body),
        len(canonical) if canonical is not None else "n/a",
    )
    return False


# Backwards-compatible private alias retained for any callers that
# still reach for the old name. New code should use the public name.
_verify_ipn_signature = verify_ipn_signature


async def _query_min_amount(
    currency_from: str, currency_to: str
) -> float | None:
    """Single ``GET /v1/min-amount`` call returning ``fiat_equivalent`` (USD).

    Returns ``None`` on any failure (network error, non-2xx, malformed
    JSON, missing field). The caller decides what to do with that.
    """
    url = "https://api.nowpayments.io/v1/min-amount"
    params = {
        "currency_from": currency_from,
        "currency_to": currency_to,
        "fiat_equivalent": "usd",
    }
    headers = {"x-api-key": NOWPAYMENTS_API_KEY} if NOWPAYMENTS_API_KEY else {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    log.warning(
                        "min-amount lookup %s->%s returned %d: %s",
                        currency_from, currency_to, response.status, body,
                    )
                    return None
                data = await response.json()
    except (asyncio.TimeoutError, aiohttp.ClientError):
        log.warning(
            "min-amount lookup %s->%s timed out / network error",
            currency_from, currency_to,
        )
        return None
    except Exception:
        log.exception(
            "Unexpected error in min-amount lookup %s->%s",
            currency_from, currency_to,
        )
        return None

    fiat = data.get("fiat_equivalent")
    try:
        return float(fiat) if fiat is not None else None
    except (TypeError, ValueError):
        return None


async def get_min_amount_usd(
    pay_currency: str, *, attempted_usd: float | None = None
) -> float | None:
    """Best-effort USD floor for an invoice paid in *pay_currency*.

    NowPayments' ``/v1/min-amount`` is asymmetric and a bit
    misleading: ``<crypto> -> usd`` typically reflects the *conversion*
    floor (~tens of cents — "smallest crypto amount we'll convert to
    USD") and NOT the floor that ``POST /v1/payment`` enforces on the
    merchant settlement side. Asking the merchant-side question
    (``usd -> <crypto>``) gives us a more honest "smallest invoice
    this provider will accept" value.

    We try both directions and return the *larger* of the two as the
    user-facing minimum. If the API returns a value that's clearly
    inconsistent with the rejection we just saw — i.e. the floor is
    *less than* the amount the user actually attempted — we treat
    the lookup as untrustworthy and return ``None`` so the handler
    falls back to the generic "min not met for <currency>" message
    instead of misleading the user with e.g. "$0.16".

    The combined result is cached per pay_currency for
    ``_MIN_AMOUNT_CACHE_TTL_SECONDS`` so re-prompts after a rejection
    don't fan out two more HTTP calls.
    """
    pay_currency = pay_currency.lower()
    cached = _min_amount_cache.get(pay_currency)
    if cached is not None:
        value, ts = cached
        if (asyncio.get_event_loop().time() - ts) < _MIN_AMOUNT_CACHE_TTL_SECONDS:
            return value

    pay_side = await _query_min_amount(pay_currency, "usd")
    merchant_side = await _query_min_amount("usd", pay_currency)
    candidates = [v for v in (pay_side, merchant_side) if v is not None]
    value = max(candidates) if candidates else None

    # If the user's attempted amount was already above the value we
    # got back, then by definition this isn't the floor that
    # actually triggered the rejection — surfacing it would tell the
    # user e.g. "min $0.16" when their $5 invoice was rejected.
    # Suppress the number so the "unknown min" branch of the UI fires
    # (clear language, no false precision).
    if value is not None and attempted_usd is not None and value < attempted_usd:
        log.warning(
            "min-amount lookup for %s gave $%.2f which is below the rejected "
            "invoice amount $%.2f; suppressing as untrustworthy",
            pay_currency, value, attempted_usd,
        )
        value = None

    _min_amount_cache[pay_currency] = (value, asyncio.get_event_loop().time())
    return value


async def create_crypto_invoice(
    telegram_id: int,
    amount_usd: float,
    currency: str,
    max_retries: int = 3,
    promo_code: str | None = None,
    promo_bonus_usd: float = 0.0,
):
    url = "https://api.nowpayments.io/v1/payment"
    headers = {
        "x-api-key": NOWPAYMENTS_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "price_amount": amount_usd,
        "price_currency": "usd",
        "pay_currency": currency,
        "order_id": str(telegram_id),
        "order_description": "شارژ کیف پول",
        "ipn_callback_url": CALLBACK_URL,
        # We deliberately DO NOT set is_fee_paid_by_user. Per
        # NowPayments' own FAQ
        # (nowpayments.io/help/payments/api/...), enabling that
        # flag automatically pins the invoice to fixed-rate mode
        # and bakes the ~0.5% gateway service fee into the
        # quoted pay_amount. The combined effect raises the
        # effective per-currency floor past $5 even on cheap
        # chains like TRX / USDT-BEP20, which is exactly the
        # rejection users were hitting.
        #
        # By leaving the flag off the merchant absorbs the
        # gateway's 0.5% service fee (~$0.025 on a $5 top-up,
        # ~$0.50 on a $100 top-up). The model-call markup more
        # than covers that, and the alternative (a $5 invoice
        # that NowPayments rejects with "amountTo is too small")
        # is much worse for retention.
    }

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    # NowPayments returns 201 for created invoices.
                    if response.status in (200, 201):
                        data = await response.json()
                        if not (data.get("pay_address") and data.get("pay_amount")):
                            log.warning(
                                "NowPayments returned 2xx but missing fields: %r",
                                data,
                            )
                            return None

                        payment_id = data.get("payment_id")
                        if payment_id is None:
                            log.warning(
                                "NowPayments response missing payment_id; "
                                "refusing to issue invoice without an idempotency key."
                            )
                            return None

                        # Record a PENDING transaction so the IPN webhook can
                        # finalize idempotently. If we can't record PENDING,
                        # do NOT return the invoice — the webhook would
                        # refuse to credit it later.
                        try:
                            await db.create_pending_transaction(
                                telegram_id=telegram_id,
                                gateway="NowPayments",
                                currency_used=currency,
                                amount_crypto=float(data.get("pay_amount")),
                                amount_usd=float(amount_usd),
                                gateway_invoice_id=str(payment_id),
                                promo_code=promo_code,
                                promo_bonus_usd=float(promo_bonus_usd),
                            )
                        except Exception:
                            log.exception(
                                "Failed to record PENDING transaction for payment_id=%s",
                                payment_id,
                            )
                            return None

                        return data

                    error_text = await response.text()
                    log.error(
                        "NowPayments error (attempt %d/%d) for "
                        "pay_currency=%s amount_usd=$%.2f: status=%d body=%s",
                        attempt + 1,
                        max_retries,
                        currency,
                        float(amount_usd),
                        response.status,
                        error_text,
                    )

                    # Currency-specific minimum: don't waste retries on a
                    # 400 that's deterministic. Surface a structured
                    # error so the handler can show the user the actual
                    # minimum and suggest a different currency.
                    if (
                        response.status == 400
                        and "amountTo is too small" in error_text
                    ):
                        min_usd = await get_min_amount_usd(
                            currency, attempted_usd=float(amount_usd)
                        )
                        raise MinAmountError(currency=currency, min_usd=min_usd)

        except MinAmountError:
            raise
        except asyncio.TimeoutError:
            log.warning(
                "Timeout talking to NowPayments (attempt %d/%d)",
                attempt + 1,
                max_retries,
            )
        except Exception:
            log.exception(
                "Network error talking to NowPayments (attempt %d/%d)",
                attempt + 1,
                max_retries,
            )

        if attempt < max_retries - 1:
            await asyncio.sleep(2)

    return None


# IPN statuses we consider "in flight": the payment is still progressing,
# nothing to do but log. NowPayments may emit several of these per invoice.
_IN_FLIGHT_STATUSES = frozenset(
    {"waiting", "confirming", "confirmed", "sending"}
)

# IPN statuses that mean the payment will NOT settle. We mark the ledger
# with a terminal status (so retries are no-ops) and notify the user.
# Mapping: incoming IPN status -> ledger status we record.
_TERMINAL_FAILURE_STATUSES = {
    "expired": "EXPIRED",
    "failed": "FAILED",
    "refunded": "REFUNDED",
}


def _compute_actually_paid_usd(data: dict) -> float | None:
    """Convert the IPN's `actually_paid` (in pay_currency) to USD.

    NowPayments quotes the conversion rate at invoice time:
        pay_amount  <crypto>  ==  price_amount  USD
    so the proportional USD value of `actually_paid` is:
        actually_paid_usd = actually_paid / pay_amount * price_amount

    Using the quoted rate (rather than fetching a fresh spot price) is
    the right defensive choice: it's the rate the user agreed to when
    they generated the invoice, so they cannot game us by paying when
    the spot price has moved against us.

    Returns None if any required field is missing or non-positive,
    in which case the caller should refuse to credit and surface for
    manual reconciliation.
    """
    try:
        actually_paid = float(data["actually_paid"])
        pay_amount = float(data["pay_amount"])
        price_amount = float(data["price_amount"])
    except (KeyError, TypeError, ValueError):
        return None
    if pay_amount <= 0 or price_amount <= 0 or actually_paid <= 0:
        return None
    # Cap at price_amount as a defense-in-depth: NowPayments shouldn't fire
    # `partially_paid` for an over-payment, but if it ever did we don't want
    # to credit more than the user requested.
    usd = actually_paid / pay_amount * price_amount
    return min(usd, price_amount)

# Maps an IPN failure status -> (strings.py key for PENDING-row variant,
# strings.py key for PARTIAL-row variant). The actual translated text is
# looked up at notification time using the user's stored language.
_TERMINAL_FAILURE_MESSAGE_KEYS = {
    "expired": ("pay_expired_pending", "pay_expired_partial"),
    "failed": ("pay_failed_pending", "pay_failed_partial"),
    "refunded": ("pay_refunded_pending", "pay_refunded_partial"),
}


async def payment_webhook(request: web.Request):
    try:
        raw_body = await request.read()
        signature = request.headers.get("x-nowpayments-sig")

        if not verify_ipn_signature(raw_body, signature):
            log.warning(
                "IPN signature verification failed (remote=%s)", request.remote
            )
            return web.Response(status=401, text="Invalid signature")

        data = json.loads(raw_body)
        status = data.get("payment_status")
        payment_id = data.get("payment_id")
        if payment_id is None:
            log.warning("Webhook missing payment_id; ignoring (status=%s)", status)
            return web.Response(status=200, text="OK")

        bot: Bot = request.app["bot"]

        if status == "finished":
            # We need the full invoice price from the IPN (not the row's
            # amount_usd_credited, which is overwritten with the partial
            # already-credited amount when this payment first came in as
            # partially_paid). Without it we can't compute the remaining
            # delta to credit on a PARTIAL -> SUCCESS upgrade.
            try:
                full_price_usd = float(data["price_amount"])
            except (KeyError, TypeError, ValueError):
                full_price_usd = 0.0
            if full_price_usd <= 0:
                log.error(
                    "finished IPN for payment_id=%s missing/invalid "
                    "price_amount; refusing to credit (data=%r)",
                    payment_id,
                    data,
                )
                return web.Response(status=200, text="OK")

            # Atomic in one DB transaction: flip the row (PENDING or
            # PARTIAL) to SUCCESS, and credit the wallet by however much
            # of the full invoice price hasn't been credited yet. If
            # anything fails the row stays in its previous state so a
            # webhook retry can finalize it.
            row = await db.finalize_payment(str(payment_id), full_price_usd)
            if row is None:
                # Either we've never seen this payment_id (no PENDING row
                # was created on our side) or it was already in a terminal
                # state (SUCCESS / EXPIRED / FAILED / REFUNDED). Either
                # way: do NOT credit. The whole point of the transactions
                # ledger is that a replayed or unknown IPN cannot mint money.
                log.info(
                    "Webhook for payment_id=%s ignored (unknown or already finalized)",
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            delta_credited = float(row["delta_credited"])
            total_credited = float(row["amount_usd_credited"])
            bonus_credited = float(row.get("promo_bonus_credited") or 0.0)

            # Best-effort user notification. The wallet has already been
            # credited in finalize_payment; a Telegram error must not cause us
            # to return 500 and trigger a NowPayments retry (the retry would
            # be a no-op because the row is no longer PENDING/PARTIAL).
            lang = await db.get_user_language(telegram_id)
            if delta_credited > 0:
                # Either a fresh full payment, or the remainder after a
                # partially_paid earlier credited some.
                msg = t(lang, "pay_credited_full", delta=delta_credited)
            else:
                # We've already credited the full amount earlier (e.g.
                # partially_paid covered the full price). Just close the
                # loop with the user.
                msg = t(lang, "pay_credited_total_only", total=total_credited)
            if bonus_credited > 0:
                msg = msg + "\n\n" + t(
                    lang, "pay_promo_bonus", bonus=bonus_credited
                )
            try:
                await bot.send_message(chat_id=telegram_id, text=msg)
            except Exception:
                log.exception(
                    "Failed to notify user %d about credit of $%s "
                    "(delta=$%.4f, promo_bonus=$%.4f)",
                    telegram_id,
                    total_credited,
                    delta_credited,
                    bonus_credited,
                )

        elif status in _TERMINAL_FAILURE_STATUSES:
            # Close the ledger row (works for both PENDING and PARTIAL —
            # see mark_transaction_terminal docstring) and notify the user.
            # No balance change: PARTIAL rows keep the partial credit they
            # already received.
            target_status = _TERMINAL_FAILURE_STATUSES[status]
            row = await db.mark_transaction_terminal(str(payment_id), target_status)
            if row is None:
                log.info(
                    "Webhook %s for payment_id=%s ignored "
                    "(unknown or already in a different terminal state)",
                    status,
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            previous_status = row["previous_status"]
            # `amount_usd_credited` semantics differ by row state: for PENDING
            # rows it's the *intended* credit (set at invoice creation), for
            # PARTIAL rows it's the cumulative amount already credited. Treat
            # PENDING as $0 actually credited so the log line and the
            # `{credited}` template var both reflect reality.
            credited_so_far = (
                float(row["amount_usd_credited"]) if previous_status == "PARTIAL" else 0.0
            )
            log.info(
                "Marked payment_id=%s as %s for user %d (was %s, credited so far $%.4f)",
                payment_id,
                target_status,
                telegram_id,
                previous_status,
                credited_so_far,
            )
            pending_key, partial_key = _TERMINAL_FAILURE_MESSAGE_KEYS[status]
            string_key = partial_key if previous_status == "PARTIAL" else pending_key
            lang = await db.get_user_language(telegram_id)
            text = t(lang, string_key, credited=credited_so_far)
            try:
                await bot.send_message(chat_id=telegram_id, text=text)
            except Exception:
                log.exception(
                    "Failed to notify user %d about %s payment %s",
                    telegram_id,
                    target_status,
                    payment_id,
                )

        elif status == "partially_paid":
            # Under-payment: the user paid some crypto, but less than the
            # invoice required. Credit the proportional USD value derived
            # from `actually_paid` (NOT the originally requested
            # price_amount, which would over-credit and let users
            # intentionally underpay to drain margin).
            actually_paid_usd = _compute_actually_paid_usd(data)
            if actually_paid_usd is None:
                # Couldn't derive a credit amount from the IPN payload.
                # Refuse to credit and log loudly so the operator can
                # reconcile by hand. The row stays PENDING.
                log.error(
                    "partially_paid IPN for payment_id=%s missing/invalid "
                    "fields needed to convert actually_paid -> USD; leaving "
                    "row PENDING for manual review (data=%r)",
                    payment_id,
                    data,
                )
                return web.Response(status=200, text="OK")

            row = await db.finalize_partial_payment(
                str(payment_id), actually_paid_usd
            )
            if row is None:
                log.info(
                    "Webhook partially_paid for payment_id=%s ignored "
                    "(unknown or already in a non-PENDING/non-PARTIAL state)",
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            total_credited = float(row["amount_usd_credited"])
            delta = float(row["delta_credited"])
            log.info(
                "partially_paid for payment_id=%s user=%d: "
                "delta_credited=$%.4f total_credited=$%.4f",
                payment_id,
                telegram_id,
                delta,
                total_credited,
            )
            # Only notify when there was actually new money to credit, so a
            # replayed IPN with the same actually_paid doesn't re-spam the user.
            if delta > 0:
                lang = await db.get_user_language(telegram_id)
                try:
                    await bot.send_message(
                        chat_id=telegram_id,
                        text=t(lang, "pay_partial", delta=delta, total=total_credited),
                    )
                except Exception:
                    log.exception(
                        "Failed to notify user %d about partial credit of $%s",
                        telegram_id,
                        delta,
                    )

        elif status in _IN_FLIGHT_STATUSES:
            log.info(
                "In-flight IPN status=%s for payment_id=%s; no-op",
                status,
                payment_id,
            )

        else:
            log.info(
                "Unhandled IPN status=%s for payment_id=%s; no-op",
                status,
                payment_id,
            )

        return web.Response(status=200, text="OK")
    except Exception:
        log.exception("Webhook handler error")
        return web.Response(status=500, text="Error")
