import aiohttp
import os
import json
from database import db

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

async def chat_with_model(telegram_id: int, user_prompt: str) -> str:
    # 1. Fetch user data and check limits
    user = await db.get_user(telegram_id)
    if not user:
        return "❌ حساب کاربری شما یافت نشد. لطفا ابتدا ربات را /start کنید."

    free_msgs = user['free_messages_left']
    balance = float(user['balance_usd'])
    active_model = user['active_model']

    # 2. Hard block if they are out of free messages and out of money
    if free_msgs <= 0 and balance < 0.05:
        return "⚠️ اعتبار شما کافی نیست. لطفا از منوی کیف پول، حساب خود را شارژ کنید."

    # 3. Call OpenRouter API
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": active_model,
        "messages": [{"role": "user", "content": user_prompt}]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload) as response:
                if response.status != 200:
                    return f"❌ خطای سرور OpenRouter: {response.status}"
                
                data = await response.json()
                reply_text = data['choices'][0]['message']['content']
                prompt_tokens = data['usage']['prompt_tokens']
                completion_tokens = data['usage']['completion_tokens']
                
                # 4. Economic Settlement
                if free_msgs > 0:
                    await db.decrement_free_message(telegram_id)
                else:
                    # MVP Math: Assuming a flat rate for testing (e.g., $1 per 1M tokens)
                    # You will map exact model prices here later.
                    cost = (prompt_tokens + completion_tokens) * 0.000001
                    deducted = await db.deduct_balance(telegram_id, cost)
                    if not deducted:
                        # Balance was sufficient at the pre-check but a
                        # concurrent request already drained it. Log usage
                        # anyway so we have a cost record; the next call is
                        # blocked by the pre-check at the top of this function.
                        print(
                            f"⚠️ Insufficient balance at settlement for user "
                            f"{telegram_id} (cost ${cost:.6f}); usage logged anyway."
                        )
                    await db.log_usage(telegram_id, active_model, prompt_tokens, completion_tokens, cost)
                    
                return reply_text
                
    except Exception as e:
        return f"❌ خطای ارتباطی: {str(e)}"