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

# User-facing notification text per terminal failure status.
_TERMINAL_FAILURE_MESSAGES = {
    "expired": (
        "⏰ مهلت پرداخت فاکتور شما به پایان رسید و وجهی دریافت نشد. "
        "اگر می‌خواهید شارژ کنید، لطفاً یک فاکتور جدید ایجاد کنید."
    ),
    "failed": (
        "❌ پرداخت شما ناموفق بود. "
        "اگر مبلغی از حساب شما کسر شده است، با پشتیبانی تماس بگیرید."
    ),
    "refunded": (
        "↩️ پرداخت شما بازگشت داده شد و به حساب شما اضافه نشد."
    ),
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
            # Atomic: flip the PENDING transaction to SUCCESS and credit the
            # user's wallet in a single DB transaction. If either the status
            # flip or the credit fails, the whole thing rolls back and the
            # row stays PENDING so a webhook retry can finalize it.
            row = await db.finalize_payment(str(payment_id))
            if row is None:
                # Either we've never seen this payment_id (no PENDING row
                # was created on our side) or it was already SUCCESS.
                # Either way: do NOT credit. The whole point of the
                # transactions ledger is that a replayed or unknown IPN
                # cannot mint money.
                log.info(
                    "Webhook for payment_id=%s ignored (unknown or already finalized)",
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            amount_usd = float(row["amount_usd_credited"])

            # Best-effort user notification. The wallet has already been
            # credited in finalize_payment; a Telegram error must not cause us
            # to return 500 and trigger a NowPayments retry (the retry would
            # be a no-op because the row is no longer PENDING).
            try:
                await bot.send_message(
                    chat_id=telegram_id,
                    text=(
                        f"✅ پرداخت تایید شد! مبلغ ${amount_usd} "
                        "به حساب شما اضافه شد."
                    ),
                )
            except Exception:
                log.exception(
                    "Failed to notify user %d about credit of $%s",
                    telegram_id,
                    amount_usd,
                )

        elif status in _TERMINAL_FAILURE_STATUSES:
            # Mark the ledger and notify. No balance change.
            target_status = _TERMINAL_FAILURE_STATUSES[status]
            row = await db.mark_transaction_terminal(str(payment_id), target_status)
            if row is None:
                log.info(
                    "Webhook %s for payment_id=%s ignored "
                    "(unknown or already finalized)",
                    status,
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            log.info(
                "Marked payment_id=%s as %s for user %d",
                payment_id,
                target_status,
                telegram_id,
            )
            try:
                await bot.send_message(
                    chat_id=telegram_id,
                    text=_TERMINAL_FAILURE_MESSAGES[status],
                )
            except Exception:
                log.exception(
                    "Failed to notify user %d about %s payment %s",
                    telegram_id,
                    target_status,
                    payment_id,
                )

        elif status == "partially_paid":
            # Under-payment: NowPayments accepted some funds but less than the
            # requested price_amount. Crediting the actually_paid USD value is
            # P1-C; for now leave the row PENDING and log so we can audit.
            log.warning(
                "partially_paid IPN for payment_id=%s; deferring to P1-C "
                "(no balance change, ledger row left PENDING)",
                payment_id,
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
