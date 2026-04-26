import aiohttp
import os
import asyncio
from aiohttp import web
from database import db
from aiogram import Bot

NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY")
CALLBACK_URL = "http://212.87.199.41:8080/nowpayments-webhook"  # ✅ مطابق با main.py 

async def create_crypto_invoice(telegram_id: int, amount_usd: float, currency: str, max_retries: int = 3):
    url = "https://api.nowpayments.io/v1/payment"
    headers = {
        "x-api-key": NOWPAYMENTS_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "price_amount": amount_usd,
        "price_currency": "usd",
        "pay_currency": currency,
        "order_id": telegram_id,
        "order_description": "شارژ کیف پول",
        "ipn_callback_url": CALLBACK_URL,
        "is_fee_paid_by_user": True
    }
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    # ✅ NowPayments با 201 جواب میده، نه 200
                    if response.status in [200, 201]:
                        data = await response.json()
                        if data.get('pay_address') and data.get('pay_amount'):
                            payment_id = data.get('payment_id')
                            if payment_id is not None:
                                # Record a PENDING transaction so the IPN
                                # webhook can finalize idempotently.
                                try:
                                    await db.create_pending_transaction(
                                        telegram_id=telegram_id,
                                        gateway='NowPayments',
                                        currency_used=currency,
                                        amount_crypto=float(data.get('pay_amount')),
                                        amount_usd=float(amount_usd),
                                        gateway_invoice_id=str(payment_id),
                                    )
                                except Exception as exc:
                                    # If we can't record PENDING, the webhook
                                    # will refuse to credit later. Surface it
                                    # to ops; do NOT silently return the
                                    # invoice as if everything is fine.
                                    print(
                                        f"❌ Failed to record PENDING transaction "
                                        f"for payment_id={payment_id}: {exc}"
                                    )
                                    return None
                            else:
                                print(
                                    "⚠️ NowPayments response missing payment_id; "
                                    "refusing to issue invoice without an idempotency key."
                                )
                                return None
                            return data
                        else:
                            print(f"⚠️ پاسخ ناقص از NowPayments: {data}")
                            return None
                    else:
                        error_text = await response.text()
                        print(f"❌ خطای NowPayments (تلاش {attempt + 1}/{max_retries}): status={response.status}, body={error_text}")
                        
        except asyncio.TimeoutError:
            print(f"⏱️ Timeout در ارتباط با NowPayments (تلاش {attempt + 1}/{max_retries})")
        except Exception as e:
            print(f"❌ خطای شبکه (تلاش {attempt + 1}/{max_retries}): {e}")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(2)
    
    return None


async def payment_webhook(request: web.Request):
    try:
        data = await request.json()
        if data.get('payment_status') == 'finished':
            payment_id = data.get('payment_id')
            if payment_id is None:
                print("⚠️ Webhook missing payment_id; ignoring")
                return web.Response(status=200, text="OK")

            row = await db.complete_transaction(str(payment_id))
            if row is None:
                # Either we've never seen this payment_id (no PENDING row
                # was created on our side) or it was already SUCCESS.
                # Either way: do NOT credit. The whole point of the
                # transactions ledger is that a replayed or unknown IPN
                # cannot mint money.
                print(
                    f"ℹ️ Webhook for payment_id={payment_id} ignored "
                    f"(unknown or already finalized)."
                )
                return web.Response(status=200, text="OK")

            telegram_id = row['telegram_id']
            amount_usd = float(row['amount_usd_credited'])

            await db.add_balance(telegram_id, amount_usd)

            bot: Bot = request.app['bot']
            await bot.send_message(
                chat_id=telegram_id,
                text=f"✅ پرداخت تایید شد! مبلغ ${amount_usd} به حساب شما اضافه شد.",
            )

        return web.Response(status=200, text="OK")
    except Exception as e:
        print(f"⚠️ خطای وب‌هوک: {e}")
        return web.Response(status=500, text="Error")
