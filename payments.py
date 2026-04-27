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

# Public base URL where this bot is reachable (HTTPS in production).
# Example: https://bot.example.com  -> IPN posts to /nowpayments-webhook there.
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
if not WEBHOOK_BASE_URL:
    log.warning(
        "WEBHOOK_BASE_URL is not set; NowPayments will not be able to deliver "
        "IPN callbacks until it is configured."
    )
CALLBACK_URL = f"{WEBHOOK_BASE_URL}/nowpayments-webhook" if WEBHOOK_BASE_URL else ""


def _verify_ipn_signature(raw_body: bytes, signature_header: str | None) -> bool:
    """Verify the x-nowpayments-sig HMAC-SHA512 signature.

    NowPayments signs the IPN payload with the IPN secret using the
    canonicalized JSON form (recursively sorted keys, no whitespace
    separators) and sends the lowercase hex digest in the
    'x-nowpayments-sig' header. We re-canonicalize the parsed body the
    same way and compare in constant time.
    """
    if not NOWPAYMENTS_IPN_SECRET:
        log.error("NOWPAYMENTS_IPN_SECRET is not set; refusing to process IPN.")
        return False
    if not signature_header:
        return False
    try:
        payload = json.loads(raw_body)
    except (ValueError, TypeError):
        return False
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    expected = hmac.new(
        NOWPAYMENTS_IPN_SECRET.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header.lower())


async def create_crypto_invoice(
    telegram_id: int,
    amount_usd: float,
    currency: str,
    max_retries: int = 3,
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
        "is_fee_paid_by_user": True,
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
                        "NowPayments error (attempt %d/%d): status=%d body=%s",
                        attempt + 1,
                        max_retries,
                        response.status,
                        error_text,
                    )

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

        if not _verify_ipn_signature(raw_body, signature):
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
            try:
                await bot.send_message(chat_id=telegram_id, text=msg)
            except Exception:
                log.exception(
                    "Failed to notify user %d about credit of $%s "
                    "(delta=$%.4f)",
                    telegram_id,
                    total_credited,
                    delta_credited,
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
