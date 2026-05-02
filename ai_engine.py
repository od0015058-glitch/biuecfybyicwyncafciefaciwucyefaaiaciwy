import logging
import math
import os
import time
from email.utils import parsedate_to_datetime

import aiohttp

from admin_toggles import is_model_disabled
from database import db
from openrouter_keys import (
    key_for_user,
    mark_key_rate_limited,
    record_key_usage,
)
from pricing import calculate_cost_async
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, t
from vision import (
    VisionError,
    build_multimodal_user_message,
    is_vision_capable_model,
)

log = logging.getLogger("bot.ai_engine")


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP ``Retry-After`` header value into a delta-seconds float.

    Per RFC 7231 §7.1.3, ``Retry-After`` can carry **either** a
    decimal delta-seconds count (``Retry-After: 60``) **or** an
    HTTP-date (``Retry-After: Wed, 21 Oct 2015 07:28:00 GMT``).
    The first slice of the per-key cooldown shipped only handled
    the delta-seconds form; an HTTP-date fell into the
    ``ValueError`` catch in :func:`chat_with_model` and the
    cooldown silently used the default 60s window. That's the
    Stage-15-Step-E #4 follow-up #4 **bundled bug fix**: many CDNs
    (Cloudflare, Akamai, AWS CloudFront — all of which can sit in
    front of OpenRouter's edge) emit HTTP-dates instead of
    delta-seconds, so the bot was throwing away a real upstream
    signal and waiting longer than necessary on short throttles
    (and shorter than necessary on long ones — both directions
    were wrong).

    Returns ``None`` when *value* is empty / unparseable / yields
    a non-positive delta. The caller treats ``None`` as "no
    Retry-After supplied" and uses
    :data:`openrouter_keys.DEFAULT_COOLDOWN_SECS`.

    The function is sync + side-effect-free so unit tests can
    exercise the parsing matrix directly without spinning up an
    aiohttp session.
    """
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    # 1. Try delta-seconds first — by far the common case.
    try:
        delta = float(stripped)
    except (TypeError, ValueError):
        delta = None
    if delta is not None:
        if math.isfinite(delta) and delta > 0.0:
            return delta
        # Non-positive delta-seconds: treat as "no usable header"
        # and let the caller fall back to the default cooldown.
        return None
    # 2. Fall through to HTTP-date. ``parsedate_to_datetime`` is
    # tolerant of all three RFC-acceptable date formats (RFC 1123,
    # RFC 850, asctime) and returns a tz-aware datetime; subtracting
    # ``time.time()`` gives the delta-seconds. A parse failure or
    # a date in the past (clock skew, stale upstream cache) yields
    # ``None`` so the caller falls back to the default cooldown
    # rather than locking out forever (past) or for a giant span
    # (parser returning a placeholder).
    try:
        dt = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    try:
        epoch = dt.timestamp()
    except (OverflowError, OSError, ValueError):
        return None
    delta_from_date = epoch - time.time()
    if not math.isfinite(delta_from_date) or delta_from_date <= 0.0:
        return None
    return delta_from_date

# Legacy single-key env var kept for reference in comments/tests.
# Actual key selection now routes through openrouter_keys.key_for_user.
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

async def chat_with_model(
    telegram_id: int,
    user_prompt: str,
    *,
    image_data_uris: list[str] | None = None,
) -> str:
    """Run one chat turn for ``telegram_id``.

    ``image_data_uris`` is the Stage-15-Step-E #10 vision integration
    surface. When non-empty the active model must be vision-capable
    (per :func:`vision.is_vision_capable_model`) — a non-vision model
    short-circuits with the ``ai_model_no_vision`` localised string
    *before* any wallet debit or OpenRouter spend, so the user can
    pick a vision-capable model and re-send rather than paying for
    a 400. The keyword is keyword-only on purpose: callers
    (``handlers.process_chat`` text path, ``handlers.process_photo``
    photo path) must opt in by name and the existing 19+ test
    callsites that pass positional args (``chat_with_model(42, "hi")``)
    keep working unchanged.

    Memory persistence stays text-only: the prompt text is
    persisted via ``db.append_conversation_message``, the image
    bytes are NOT (the schema is ``content TEXT NOT NULL``). A
    follow-up PR can extend ``conversation_messages`` to JSONB if
    we want full-fidelity replay; for now the trade-off is that
    a memory-enabled user's vision turn replays as text-only on
    the next turn (the model loses the visual context but keeps
    the conversational thread). HANDOFF.md documents this limit.
    """
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

    # Stage-14: admin can disable individual models at runtime.
    if is_model_disabled(active_model):
        return t(lang, "ai_model_disabled")

    # Stage-15-Step-E #10: vision-capability gate. Fires *before* the
    # insufficient-balance gate so a user with empty wallet trying to
    # send an image to a non-vision model gets the actionable error
    # (pick a vision model) rather than the generic "top up" — the
    # latter would have them top up uselessly because the next
    # attempt would still fail. Also fires *before* the OpenRouter
    # spend so a non-vision model never sees the image and never
    # 400s back at us with us holding the bag for a $0 reply.
    has_images = bool(image_data_uris)
    if has_images and not is_vision_capable_model(active_model):
        log.info(
            "vision rejected for user %d: model %r does not support images",
            telegram_id, active_model,
        )
        return t(lang, "ai_model_no_vision")

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
    if has_images:
        # Stage-15-Step-E #10: assemble the OpenAI/OpenRouter
        # multimodal user-message dict via the pure helper. A
        # ``VisionError`` here means the caller (handler) handed us
        # malformed inputs that survived its own validation — drop
        # the image cleanly and surface the same provider-unavailable
        # text we already use for unrecoverable input issues, so the
        # user sees a localised message rather than a poller-level
        # crash. Never charge or call OpenRouter in this branch.
        try:
            messages.append(
                build_multimodal_user_message(user_prompt, image_data_uris)
            )
        except VisionError:
            log.exception(
                "vision payload assembly failed for user %d "
                "(model=%r, image_count=%d, prompt_len=%d); "
                "investigate handler-side validation.",
                telegram_id, active_model,
                len(image_data_uris) if image_data_uris else 0,
                len(user_prompt or ""),
            )
            return t(lang, "ai_provider_unavailable")
    else:
        messages.append({"role": "user", "content": user_prompt})

    # 4. Call OpenRouter API
    # Stage-15-Step-E #4 follow-up #4: pass ``model=active_model``
    # so the picker treats slots cooled for *other* models on the
    # same key as still available — only slots cooled for the
    # user's active model are skipped.
    try:
        api_key = key_for_user(telegram_id, model=active_model)
    except RuntimeError:
        log.error("No OpenRouter API keys configured — cannot serve chat.")
        return t(lang, "ai_provider_unavailable")
    headers = {
        "Authorization": f"Bearer {api_key}",
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
                        # Stage-15-Step-E #4: put this key in
                        # cooldown so the next user routed to it
                        # falls through to a different pool member
                        # rather than retrying the same hot key.
                        # Stage-15-Step-E #4 follow-up #4: scope
                        # the cooldown to ``(api_key, active_model)``
                        # so other models on the same key keep
                        # serving — OpenRouter typically 429s a
                        # specific (often ``:free``) model rather
                        # than the API key as a whole, and a
                        # whole-key cooldown over-blocked paid
                        # traffic on the same key.
                        # ``_parse_retry_after`` handles both the
                        # delta-seconds form and the RFC HTTP-date
                        # form (bundled bug fix); the inner
                        # ``mark_key_rate_limited`` clamps + falls
                        # back if the value is still unusable.
                        # Wrapped in a broad except so a parsing
                        # quirk doesn't mask the user-facing reply.
                        try:
                            retry_after_secs = _parse_retry_after(
                                response.headers.get("Retry-After")
                            )
                            mark_key_rate_limited(
                                api_key,
                                retry_after_secs=retry_after_secs,
                                model=active_model,
                            )
                        except Exception:
                            log.exception(
                                "mark_key_rate_limited raised "
                                "unexpectedly for user %d; "
                                "continuing to user reply.",
                                telegram_id,
                            )
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

                # Stage-13-Step-B: ``content`` is allowed to be ``null``
                # in the OpenAI-compatible chat-completion spec — tool
                # calls return ``{"role": "assistant", "content": null,
                # "tool_calls": [...]}``, and upstream policy refusals
                # / safety blocks at OpenRouter sometimes surface as
                # 200s with ``content: null`` rather than the
                # ``{"error": ...}`` body shape the guard above
                # catches. Without this branch the bot would forward
                # ``None`` (or an empty string) to the handler, which
                # then hands it to Telegram and gets back ``Bad
                # Request: message text is empty``. Treat empty /
                # falsy reply text as the same provider-unavailable
                # condition the explicit error-body branch already
                # surfaces, so the user gets a useful message and we
                # don't bill them for a non-reply.
                if not reply_text or not str(reply_text).strip():
                    log.warning(
                        "OpenRouter 200 with empty/null content for user %d "
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
                # Stage-15-Step-E #4 follow-up #3: track per-key
                # 24h usage / cost on the operator panel. We compute
                # the *would-be* cost for both free and paid branches
                # so the panel's "24h spend" column reflects upstream
                # OpenRouter pressure for *this key* — a free user
                # routed through key #2 still spent OpenRouter quota
                # on key #2, even though we didn't bill the user.
                #
                # ``calculate_cost_async`` is a fast in-memory lookup
                # (catalog cache + markup arithmetic) so the extra
                # call on the free path is cheap.
                cost_for_key_tracker = await calculate_cost_async(
                    active_model, prompt_tokens, completion_tokens
                )
                if not settled_as_free:
                    cost = cost_for_key_tracker
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

                # Bump the per-key 24h usage tracker + bump
                # ``last_used_at`` on the DB-backed registry row
                # (when applicable). Wrapped in a broad except so a
                # transient failure on either path doesn't lose the
                # AI reply the user just paid for.
                try:
                    await record_key_usage(
                        api_key, cost_for_key_tracker, db=db,
                    )
                except Exception:
                    log.exception(
                        "record_key_usage failed for user %d after "
                        "successful settlement; continuing.",
                        telegram_id,
                    )

                # 5. Persist the turn for memory-enabled users. We do
                #    this AFTER the settlement so a free / paid call
                #    that already deducted balance doesn't get recorded
                #    twice if the response parse fails. Persisting both
                #    sides of the turn keeps the buffer balanced (so
                #    each fetch returns alternating user/assistant
                #    pairs in chronological order).
                #
                # Stage-15-Step-E #10 bundled fix: persistence is
                # best-effort. Pre-fix, an INSERT that raised
                # (``\\x00`` NUL byte in the prompt or reply —
                # Postgres TEXT rejects with "invalid byte sequence
                # for encoding UTF8: 0x00" — a transient connection
                # drop, a deadlock, an FK violation if the user row
                # was deleted between the chat starting and persist
                # time, etc.) would bubble out to the outer
                # ``except Exception`` at the bottom of this
                # function, the user would see ``ai_transient_error``,
                # and ``reply_text`` would be lost — even though the
                # wallet had ALREADY been debited at line ~293 and
                # the usage_log row had ALREADY been written at line
                # ~306. Re-prompting would re-charge them. Net
                # effect: silent double-billing whenever a memory-
                # enabled user happened to send a prompt or receive
                # a reply containing a NUL byte (which Telegram does
                # allow). Fix: catch the persistence failure
                # locally, log loud-and-once for ops, and still
                # return the AI reply to the user. Losing one turn
                # from the memory buffer is much better than
                # double-billing them; the next turn re-establishes
                # context naturally because the *current* prompt
                # they just paid for is the one that matters most.
                if memory_enabled:
                    # Stage-15-Step-E #10 follow-up #2: persist the
                    # image refs alongside the prompt text for vision
                    # turns. Pre-follow-up, the image was silently
                    # dropped from the persisted row, so the next
                    # memory replay surfaced as a text-only turn —
                    # the model kept the conversational thread but
                    # lost the visual context that drove the question.
                    # Empty / None ``image_data_uris`` keeps the
                    # text-only INSERT shape unchanged.
                    persisted_uris = (
                        list(image_data_uris) if image_data_uris else None
                    )
                    try:
                        await db.append_conversation_message(
                            telegram_id, "user", user_prompt,
                            image_data_uris=persisted_uris,
                        )
                        await db.append_conversation_message(
                            telegram_id, "assistant", reply_text,
                        )
                    except Exception:
                        log.exception(
                            "memory persist failed for user %d after "
                            "successful settlement; returning reply "
                            "anyway to avoid double-billing on retry.",
                            telegram_id,
                        )

                return reply_text
                
    except Exception:
        log.exception("Unexpected error in chat_with_model for user %d", telegram_id)
        return t(lang, "ai_transient_error")