import logging
import os

import aiohttp

from database import db
from pricing import calculate_cost_async
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, t

log = logging.getLogger("bot.ai_engine")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

async def chat_with_model(telegram_id: int, user_prompt: str) -> str:
    # 1. Fetch user data and check limits
    user = await db.get_user(telegram_id)
    if not user:
        # No row yet -> user never /started. Greet in the default locale.
        return t(DEFAULT_LANGUAGE, "ai_no_account")

    free_msgs = user['free_messages_left']
    balance = float(user['balance_usd'])
    active_model = user['active_model']
    lang = user['language_code'] if user['language_code'] in SUPPORTED_LANGUAGES else DEFAULT_LANGUAGE

    # 2. Hard block if they are out of free messages and out of money
    if free_msgs <= 0 and balance < 0.05:
        return t(lang, "ai_insufficient_balance")

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
                    body = await response.text()
                    log.error(
                        "OpenRouter HTTP %d for user %d model=%s: %s",
                        response.status, telegram_id, active_model, body,
                    )
                    return t(lang, "ai_provider_unavailable")
                
                data = await response.json()
                reply_text = data['choices'][0]['message']['content']
                prompt_tokens = data['usage']['prompt_tokens']
                completion_tokens = data['usage']['completion_tokens']
                
                # 4. Economic Settlement
                if free_msgs > 0:
                    await db.decrement_free_message(telegram_id)
                else:
                    cost = await calculate_cost_async(
                        active_model, prompt_tokens, completion_tokens
                    )
                    deducted = await db.deduct_balance(telegram_id, cost)
                    if not deducted:
                        # Balance was sufficient at the pre-check but a
                        # concurrent request already drained it. Record the
                        # usage with cost_deducted_usd=0 so SUM(cost) on
                        # usage_logs still reconciles with actual balance
                        # changes; the next call is blocked by the pre-check.
                        log.warning(
                            "Insufficient balance at settlement for user %d "
                            "(would-be cost $%.6f); logging at $0.00.",
                            telegram_id, cost,
                        )
                    charged = cost if deducted else 0.0
                    await db.log_usage(telegram_id, active_model, prompt_tokens, completion_tokens, charged)
                    
                return reply_text
                
    except Exception:
        log.exception("Unexpected error in chat_with_model for user %d", telegram_id)
        return t(lang, "ai_transient_error")