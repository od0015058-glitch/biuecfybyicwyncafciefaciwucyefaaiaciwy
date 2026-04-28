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
    # P3-5: pull the toggle from the same row so we don't fire a second
    # SELECT for what's effectively a one-byte flag. Defaults to False
    # if the column is missing (pre-migration DB), keeping the bot
    # operational while the DBA applies the migration.
    memory_enabled = bool(user["memory_enabled"]) if "memory_enabled" in user else False

    # 2. Hard block if they are out of free messages and out of money
    if free_msgs <= 0 and balance < 0.05:
        return t(lang, "ai_insufficient_balance")

    # 3. Build the messages payload. With memory disabled this is just
    #    [user_prompt]; with memory enabled we prepend the user's last
    #    N turns so the model can carry context across messages. The
    #    cost grows roughly linearly with conversation length — the
    #    hub UI explains this and offers a "🆕 New chat" reset.
    messages: list[dict] = []
    if memory_enabled:
        messages.extend(await db.get_recent_messages(telegram_id))
    messages.append({"role": "user", "content": user_prompt})

    # 4. Call OpenRouter API
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": active_model,
        "messages": messages,
    }

    # Hard timeout on the upstream call. Without this, an OpenRouter
    # stall pins the user's coroutine forever — the user thinks the
    # bot is dead, no error is logged, and we leak event-loop slots
    # under sustained slowness. 60s total, with separate connect /
    # read budgets so a stuck TCP handshake is reported separately
    # from a stuck stream.
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=50)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload) as response:
                if response.status != 200:
                    body = await response.text()
                    log.error(
                        "OpenRouter HTTP %d for user %d model=%s: %s",
                        response.status, telegram_id, active_model, body,
                    )
                    # 429 from OpenRouter most often comes from a free
                    # model whose upstream provider (Google AI Studio,
                    # Cerebras, etc.) is rate-limiting that specific
                    # slug — usually the ":free" tier. Generic
                    # "provider unavailable" obscures the actionable
                    # advice ("pick a paid model or wait"). Detect the
                    # ":free" suffix to give a more honest message.
                    if response.status == 429:
                        if active_model.endswith(":free"):
                            return t(lang, "ai_rate_limited_free")
                        return t(lang, "ai_rate_limited")
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

                # 5. Persist the turn for memory-enabled users. We do
                #    this AFTER the settlement so a free / paid call
                #    that already deducted balance doesn't get recorded
                #    twice if the response parse fails. Persisting both
                #    sides of the turn keeps the buffer balanced (so
                #    each fetch returns alternating user/assistant
                #    pairs in chronological order).
                if memory_enabled:
                    await db.append_conversation_message(telegram_id, "user", user_prompt)
                    await db.append_conversation_message(telegram_id, "assistant", reply_text)

                return reply_text
                
    except Exception:
        log.exception("Unexpected error in chat_with_model for user %d", telegram_id)
        return t(lang, "ai_transient_error")