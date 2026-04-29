import logging
import math
import os

import aiohttp

from database import db
from pricing import calculate_cost_async
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, t

log = logging.getLogger("bot.ai_engine")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Schema default from ``alembic/versions/0001_baseline.py``:
#     active_model VARCHAR(255) DEFAULT 'openai/gpt-3.5-turbo'
# Mirrored here so we can self-heal a row whose ``active_model``
# column is somehow ``NULL`` / empty / whitespace at chat time. The
# column is nullable (no ``NOT NULL`` constraint) and direct DB
# writes (an operator running raw SQL, a bad migration backfill, a
# legacy tool that bypassed ``set_active_model``) can leave it
# blank. Without this fallback the bot would send
# ``{"model": null, ...}`` to OpenRouter, get a 400, and reply
# ``ai_provider_unavailable`` for *every* subsequent chat from
# that user — no actionable hint, no recovery path. The 429 branch
# below also crashes outright (``None.endswith(":free")``) and
# surfaces as ``ai_transient_error``. Falling back to a known-good
# id keeps the user productive while the row is repaired.
_ACTIVE_MODEL_FALLBACK = "openai/gpt-3.5-turbo"


def _resolve_active_model(raw: object) -> str:
    """Return a non-empty model id, falling back to the schema
    default when ``raw`` is ``None`` / empty / whitespace.

    Coerces to ``str`` defensively so a row that somehow stored a
    non-string (e.g. via a future schema migration accident) still
    routes through the fallback rather than blowing up at
    ``endswith`` / ``.lower()`` later in the function.
    """
    if raw is None:
        return _ACTIVE_MODEL_FALLBACK
    coerced = str(raw).strip()
    if not coerced:
        return _ACTIVE_MODEL_FALLBACK
    return coerced

async def chat_with_model(telegram_id: int, user_prompt: str) -> str:
    # 1. Fetch user data and check limits
    user = await db.get_user(telegram_id)
    if not user:
        # No row yet -> user never /started. Greet in the default locale.
        return t(DEFAULT_LANGUAGE, "ai_no_account")

    free_msgs = user['free_messages_left']
    balance = float(user['balance_usd'])
    # Defense-in-depth: a non-finite ``balance_usd`` (NaN or
    # ``+Infinity``) silently bypasses the ``balance < 0.05``
    # insufficient-funds gate below — every comparison against NaN
    # returns False, and ``+Infinity < 0.05`` is also False (so
    # neither value is "less than 0.05"). A user with a poisoned
    # wallet (legacy row predating the write-side finite guards on
    # ``finalize_payment`` / ``finalize_partial_payment`` /
    # ``deduct_balance`` / ``redeem_gift_code`` /
    # ``admin_adjust_balance``, a manual SQL fix, a future migration
    # mishap, or any path that bypasses those callers) would
    # therefore pass the gate, hit OpenRouter on the bot's dime, then
    # have ``deduct_balance`` silently no-op (``balance_usd >= cost``
    # is False for NaN, and the existing ``_is_finite_amount`` guard
    # at the SQL layer refuses the +Infinity branch) and fall to the
    # ``cost=0`` ``log_usage`` branch — i.e. unlimited free chat at
    # the bot's expense. ``-Infinity`` correctly fails the gate
    # (``-Inf < 0.05`` is True) so it isn't part of this hole, but
    # we treat any non-finite value the same way for simplicity and
    # because ``-Inf`` is just as wrong to display in the hub UI.
    # Treat non-finite as $0 locally so the gate fires correctly;
    # log loud-and-once so ops can repair the row.
    if not math.isfinite(balance):
        log.error(
            "user %d has non-finite balance_usd=%r in DB; treating "
            "as $0 for gating. Investigate row corruption.",
            telegram_id, user['balance_usd'],
        )
        balance = 0.0
    raw_active_model = user['active_model']
    active_model = _resolve_active_model(raw_active_model)
    if active_model != raw_active_model:
        # Either the row was NULL / blank, or the value had stray
        # surrounding whitespace. Log loud-and-once so ops can
        # repair the row; we keep the chat moving rather than
        # surface the issue to the user.
        log.warning(
            "active_model fallback engaged for user %d: stored "
            "value %r → using %r",
            telegram_id, raw_active_model, active_model,
        )
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
                # OpenRouter occasionally returns a 200 with a body
                # shaped like ``{"error": {...}}`` (rate-limit info,
                # safety-policy block, model-specific provider error)
                # instead of the OpenAI-style chat completion shape.
                # Indexing ``data['choices'][0]['message']['content']``
                # on those bodies raises KeyError / IndexError and
                # bubbles up as a 'Run polling' crash visible only in
                # logs — the user sees nothing back. Guard explicitly
                # and surface the existing 'provider unavailable'
                # i18n message instead.
                try:
                    reply_text = data["choices"][0]["message"]["content"]
                    prompt_tokens = data["usage"]["prompt_tokens"]
                    completion_tokens = data["usage"]["completion_tokens"]
                except (KeyError, IndexError, TypeError):
                    log.error(
                        "OpenRouter 200 with unexpected body for user %d "
                        "model=%s: %.500r",
                        telegram_id, active_model, data,
                    )
                    return t(lang, "ai_provider_unavailable")
                
                # 4. Economic Settlement
                #
                # ``free_msgs`` was read from a stale ``users`` row at
                # the top of this function. Between that read and now
                # we made a (slow) HTTP call to OpenRouter, so several
                # concurrent prompts from the same user can all observe
                # the same pre-call snapshot and all enter the
                # ``free_msgs > 0`` branch.
                #
                # ``decrement_free_message`` is atomic — its WHERE
                # ``free_messages_left > 0`` only fires once per
                # remaining free message — so a concurrent racer
                # returns ``None`` instead of decrementing.
                # Pre-fix that ``None`` was silently swallowed and
                # the racer got a free reply with no settlement at
                # all (no decrement, no balance deduction, no
                # usage_log row). Five concurrent prompts with
                # ``free_messages_left=1`` therefore granted four
                # un-paid replies and the bot ate the OpenRouter cost.
                #
                # Fall through to the paid-settlement branch when the
                # decrement no-ops so the wallet is charged like any
                # other paid call. The pre-check at the top of this
                # function still gates whether the user is allowed to
                # call OpenRouter at all (``free_msgs <= 0`` AND
                # ``balance < 0.05``), so the worst case here is a
                # user with one free message and a paid balance who
                # fires N concurrent prompts: the first decrements
                # the free counter, the rest spend balance.
                settled_as_free = False
                if free_msgs > 0:
                    decremented = await db.decrement_free_message(telegram_id)
                    if decremented is not None:
                        settled_as_free = True
                    else:
                        log.info(
                            "free-message race for user %d: counter "
                            "already exhausted by a concurrent prompt; "
                            "falling back to paid settlement",
                            telegram_id,
                        )
                if not settled_as_free:
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