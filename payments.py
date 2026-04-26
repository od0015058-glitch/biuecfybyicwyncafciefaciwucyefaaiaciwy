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
            telegram_id = int(data.get('order_id'))
            amount_usd = float(data.get('price_amount'))
            
            await db.add_balance(telegram_id, amount_usd)
            
            bot: Bot = request.app['bot']
            await bot.send_message(chat_id=telegram_id, text=f"✅ پرداخت تایید شد! مبلغ ${amount_usd} به حساب شما اضافه شد.")
            
        return web.Response(status=200, text="OK")
    except Exception as e:
        print(f"⚠️ خطای وب‌هوک: {e}")
        return web.Response(status=500, text="Error")
