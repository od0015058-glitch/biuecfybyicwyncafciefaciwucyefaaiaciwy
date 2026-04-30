import logging
import math

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ai_engine import chat_with_model
from amount_input import normalize_amount
from database import db
from force_join import (
    FORCE_JOIN_CHECK_CALLBACK,
    force_join_check_callback,
)
from fx_rates import get_usd_to_toman_snapshot
from wallet_display import format_toman_annotation
from wallet_receipts import (
    format_receipts_page,
    get_receipts_page_size,
)
from models_catalog import CatalogModel, get_catalog
from payments import (
    GLOBAL_MIN_TOPUP_USD,
    MinAmountError,
    create_crypto_invoice,
    effective_min_usd,
    find_cheaper_alternative,
    get_min_amount_usd,
)
from pricing import apply_markup_to_price
from rate_limit import (
    consume_chat_token,
    release_chat_slot,
    try_claim_chat_slot,
)
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, all_button_labels, t
from tetrapay import create_order as tetrapay_create_order

log = logging.getLogger("bot.handlers")

router = Router()


class UserStates(StatesGroup):
    waiting_custom_amount = State()
    # Stage-11-Step-B: a separate FSM state for the Toman entry flow.
    # Kept distinct from ``waiting_custom_amount`` so we don't have to
    # smuggle a "currency_mode" flag in FSM data and can wire different
    # error strings / prompts per entry path.
    waiting_toman_amount = State()
    waiting_promo_code = State()
    waiting_gift_code = State()


# Pre-compute the set of all main-keyboard button labels (across every
# supported language). aiogram dispatches handlers by F.text == "..."
# *before* we get a chance to look up the user's language from the DB,
# so we have to match against every locale's spelling of each button.
_KBD_KEYS = ("kbd_models", "kbd_wallet", "kbd_support", "kbd_language")
_ALL_KBD_LABELS: tuple[str, ...] = tuple(
    label for k in _KBD_KEYS for label in all_button_labels(k)
)
_MODEL_LABELS = all_button_labels("kbd_models")
_WALLET_LABELS = all_button_labels("kbd_wallet")
_SUPPORT_LABELS = all_button_labels("kbd_support")
_LANGUAGE_LABELS = all_button_labels("kbd_language")


# Currencies offered for crypto top-ups. The first element is what the
# user sees on the inline button, the second is the NowPayments ticker
# we send as `pay_currency` when creating the invoice.
#
# When adding a row here, also confirm the ticker is enabled in the
# NowPayments dashboard (Store Settings → Currencies). NowPayments
# rejects invoice creation for a disabled currency with HTTP 400.
SUPPORTED_PAY_CURRENCIES: tuple[tuple[str, str], ...] = (
    ("₿ Bitcoin", "btc"),
    ("Ξ Ethereum", "eth"),
    ("🔷 Litecoin", "ltc"),
    ("💎 TON", "ton"),
    ("⚡ TRON (TRX)", "trx"),
    ("💵 USDT (TRC20)", "usdttrc20"),
    ("💵 USDT (ERC20)", "usdterc20"),
    ("💵 USDT (BEP20)", "usdtbsc"),
    ("💵 USDT (TON)", "usdtton"),
)
# Layout: 3-wide grid for the 9 currencies above.
_CURRENCY_ROWS_LAYOUT = (3, 3, 3)


async def _get_user_language(telegram_id: int) -> str:
    """Look up the user's preferred language, falling back to the default.

    Used at the top of every handler. We keep it tiny on purpose; if the
    user row is missing (e.g. they never sent /start), we just default,
    matching the existing 'don't crash if not registered' behaviour
    elsewhere in this file.
    """
    lang = await db.get_user_language(telegram_id)
    if lang in SUPPORTED_LANGUAGES:
        return lang
    return DEFAULT_LANGUAGE


# ==========================================
# Legacy bottom reply keyboard (P3-3 deprecated this; kept only as a
# rendering helper so the legacy text handlers can still construct one
# if absolutely needed. The hub UI is now inline-only.)
# ==========================================
def get_main_keyboard(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=t(lang, "kbd_models")),
                KeyboardButton(text=t(lang, "kbd_wallet")),
            ],
            [
                KeyboardButton(text=t(lang, "kbd_support")),
                KeyboardButton(text=t(lang, "kbd_language")),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


# ==========================================
# Inline hub — single-message UI (P3-3)
# ==========================================
# The bot renders ONE persistent message — the "hub" — that the user
# navigates by tapping inline buttons attached to it. Every action
# (wallet, models, language, support, …) edits the same message in
# place, so the chat history stays clean: one bot bubble, the user's
# free-text messages, and the bot's AI replies. Subflows that take
# free-text input from the user (custom amount, promo code) inherently
# create extra bubbles — that's unavoidable, but they end with a fresh
# inline-keyboard message that becomes the local hub for that flow.
async def _hub_text_and_kb(
    telegram_id: int, lang: str
) -> tuple[str, InlineKeyboardBuilder]:
    """Render the hub: title text + 5-button inline keyboard.

    Pulls live state (active model, balance, current language,
    memory toggle) from the DB so the user always sees their
    settings without digging into sub-screens.
    """
    user = await db.get_user(telegram_id)
    active_model = (
        user["active_model"]
        if user and user.get("active_model")
        else t(lang, "hub_no_active_model")
    )
    # Stage-13-Step-A bundled bug fix: NaN-guard the balance figure
    # before format-string interpolation. ``hub_title`` formats the
    # value as ``${balance:.2f}`` directly, and ``f"${math.nan:.2f}"``
    # renders literally ``$nan`` — so a corrupted ``users.balance_usd``
    # row (legacy NaN, manual SQL fix gone wrong, etc.) would leak
    # ``$nan`` into the user's hub view. The same regression applies
    # to ``wallet_text`` (``hub_wallet_handler`` /
    # ``back_to_wallet_handler``) and ``redeem_ok``
    # (:func:`_redeem_code_for_user`); both call sites get the same
    # ``math.isfinite`` guard. ``$0.00`` is the closest sensible
    # rendering of "we don't know your balance" — the upstream that
    # handed us a NaN has a real bug, not a UI string.
    raw_balance = float(user["balance_usd"]) if user else 0.0
    balance = raw_balance if math.isfinite(raw_balance) else 0.0
    lang_label = t(lang, f"hub_lang_label_{lang}")
    # asyncpg.Record supports "key in record"; the migration may not
    # have run yet on the production DB so be defensive.
    memory_on = bool(user["memory_enabled"]) if user and "memory_enabled" in user else False
    memory_label = t(lang, "memory_state_on" if memory_on else "memory_state_off")

    text = t(
        lang,
        "hub_title",
        active_model=active_model,
        balance=balance,
        lang_label=lang_label,
        memory_label=memory_label,
    )

    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "hub_btn_wallet"), callback_data="hub_wallet")
    kb.button(text=t(lang, "hub_btn_models"), callback_data="hub_models")
    # Two distinct actions: tapping "🆕 New Chat" wipes the
    # conversation buffer immediately (free, no confirmation
    # screen). The "🧠 Memory: ON/OFF" button opens the memory
    # settings screen with the cost trade-off explanation. They
    # used to be a single "🆕 New Chat" button that opened the
    # memory screen — confusing because the user expected the
    # button to *do* something every time, not show a settings
    # page that does nothing when memory is OFF.
    kb.button(text=t(lang, "hub_btn_new_chat"), callback_data="hub_newchat")
    kb.button(
        text=t(lang, "hub_btn_memory", state=memory_label),
        callback_data="hub_memory",
    )
    kb.button(text=t(lang, "hub_btn_support"), callback_data="hub_support")
    kb.button(text=t(lang, "hub_btn_language"), callback_data="hub_language")
    kb.adjust(2, 2, 2)
    return text, kb


async def _send_hub(message: Message, lang: str, *, remove_kb: bool = False) -> None:
    """Send a fresh hub message. ``remove_kb=True`` strips the legacy
    bottom reply-keyboard from the user's client (one-shot — once the
    user has seen a ``ReplyKeyboardRemove`` they won't see the bottom
    keyboard again unless we explicitly send a new ReplyKeyboardMarkup).
    """
    text, kb = await _hub_text_and_kb(message.chat.id, lang)
    if remove_kb:
        # Strip the legacy bottom keyboard first as its own message —
        # ``answer`` with reply_markup=ReplyKeyboardRemove on the same
        # call would also strip the inline keyboard we want to attach.
        await message.answer("…", reply_markup=ReplyKeyboardRemove())
    await message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


async def _edit_to_hub(callback: CallbackQuery, lang: str) -> None:
    """Edit the calling callback's message back into the hub view.

    All "🏠 Back to menu" buttons funnel here. Idempotent: if the
    message text + keyboard are already the hub, Telegram's
    ``edit_text`` raises a ``TelegramBadRequest: message is not
    modified`` for that no-op, which is the only exception we want
    to silence.

    Pre-fix the bare ``except Exception`` here swallowed every
    exception including DB-pool drops bubbling out of an upstream
    ``edit_text`` retry layer, ``TelegramForbiddenError`` (the user
    blocked the bot — worth surfacing in logs as a real warning, not
    a debug line), ``TelegramRetryAfter`` (which we genuinely want
    to see so we can tune backoff), and unrelated aiohttp network
    blips. Tightening to ``TelegramBadRequest`` matches the same
    fix shipped for ``_render_memory_screen`` in Stage-9-Step-1.5.
    """
    text, kb = await _hub_text_and_kb(callback.from_user.id, lang)
    try:
        await callback.message.edit_text(
            text, reply_markup=kb.as_markup(), parse_mode="Markdown"
        )
    except TelegramBadRequest:
        # The legitimate "message is not modified" / parse-mode no-op
        # case. Not user-facing; everything else propagates so it
        # surfaces in logs / the dispatcher's error handler.
        log.debug("edit_to_hub: edit_text was a no-op", exc_info=True)


def _back_to_menu_button(kb: InlineKeyboardBuilder, lang: str) -> None:
    """Append the standard '🏠 Back to menu' button to a screen's keyboard."""
    kb.button(text=t(lang, "btn_back_to_menu"), callback_data="back_to_hub")


# ==========================================
# COMMAND: /start
# ==========================================
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    # Drop any in-flight FSM (custom-amount / promo input) — /start is
    # always a hard reset.
    await state.clear()
    # ``message.from_user`` is None for anonymous group admins and
    # channel-bot edge cases. ``UserUpsertMiddleware`` guards None
    # for the DB upsert but the handler itself was still touching
    # ``.id`` / ``.username`` / ``.first_name`` directly — pre-fix
    # this AttributeError'd and bubbled up as a poller-level crash.
    # Same defensive guard pattern that PR #51 added to ``process_chat``
    # and the cleanup PR added to ``process_promo_input`` /
    # ``process_custom_amount_input``.
    if message.from_user is None:
        return
    await db.create_user(message.from_user.id, message.from_user.username or "Unknown")
    lang = await _get_user_language(message.from_user.id)
    # Quick greeting first (one-shot bubble), then the hub. Greeting is
    # short and addressable so users feel acknowledged before the menu
    # appears. Also strips the legacy bottom keyboard from old clients.
    greeting = t(lang, "start_greeting", first_name=message.from_user.first_name or "")
    await message.answer(greeting, reply_markup=ReplyKeyboardRemove())
    await _send_hub(message, lang, remove_kb=False)


# ==========================================
# /redeem — gift code redemption (Stage-8-Part-3)
# ==========================================
@router.message(Command("redeem"))
async def cmd_redeem(message: Message, state: FSMContext):
    """Redeem a gift code: ``/redeem CODE``.

    Gift codes are admin-issued (via the web admin at /admin/gifts) and
    credit balance directly — no purchase required. Each user can
    redeem each code at most once. Code matching is case-insensitive
    on the user side; the DB normalises to uppercase.
    """
    await state.clear()
    if message.from_user is None:
        # Same defensive guard pattern used in cmd_start / process_chat.
        return
    user_id = message.from_user.id
    lang = await _get_user_language(user_id)

    # Parse the code arg. ``aiogram``'s Command filter doesn't auto-split,
    # so do it ourselves. Accept ``/redeem CODE`` and ``/redeem@bot CODE``.
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(t(lang, "redeem_usage"))
        return
    code_arg = parts[1].strip()
    reply = await _redeem_code_for_user(user_id, code_arg, lang)
    await message.answer(reply)


# Shared by ``cmd_redeem`` (slash command) and the wallet-menu redeem
# flow (FSM ``waiting_gift_code`` handler). Returns the localized text
# to send back to the user; never raises. ``redeem_bad_code`` covers
# both bad format and over-length input. The DB layer is the source
# of truth for status semantics — we just translate them.
_REDEEM_ERR_KEY_MAP: dict[str, str] = {
    "not_found": "redeem_not_found",
    "inactive": "redeem_inactive",
    "expired": "redeem_expired",
    "exhausted": "redeem_exhausted",
    "already_redeemed": "redeem_already_redeemed",
    "user_unknown": "redeem_user_unknown",
}


async def _redeem_code_for_user(
    user_id: int, code_arg: str, lang: str
) -> str:
    """Validate + redeem *code_arg* and return the localized response."""
    # Cap on length matches the DB column / parser bound — anything
    # longer is definitely junk, no need to round-trip the DB.
    # ASCII-only matches the admin-side ``parse_promo_form`` /
    # ``parse_gift_form`` validators (web_admin.py): codes are stored
    # as ASCII ``[A-Z0-9_-]`` and matching a user-typed string with
    # Unicode lookalikes (Persian "۱" vs ASCII "1", Cyrillic "А" vs
    # Latin "A") would always 404 in the DB anyway. Reject upstream
    # so the user gets ``redeem_bad_code`` (a clearer message than
    # the generic ``redeem_not_found`` they'd get from the DB miss).
    if len(code_arg) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code_arg
    ):
        return t(lang, "redeem_bad_code")

    try:
        result = await db.redeem_gift_code(code_arg, user_id)
    except Exception:
        log.exception("redeem_gift_code crashed")
        return t(lang, "redeem_error")

    status = result.get("status")
    if status == "ok":
        amount = float(result["amount_usd"])
        # Same NaN-guard as :func:`_hub_text_and_kb` — ``redeem_ok``
        # formats ``new_balance`` as ``${balance:.2f}`` directly, and
        # ``f"${math.nan:.2f}"`` renders literally ``$nan``. Defence in
        # depth: ``redeem_gift_code`` itself only credits a positive
        # amount onto a row that's already been rejected at every
        # write site that could mint a NaN, so the worst real-world
        # path is a legacy NaN row that never got swept. Falling back
        # to ``$0.00`` matches the hub view's policy: "we don't know
        # your balance" is a real bug upstream, not a UI string.
        raw_new_balance = float(result["new_balance_usd"])
        new_balance = (
            raw_new_balance if math.isfinite(raw_new_balance) else 0.0
        )
        log.info(
            "redeem ok telegram_id=%s code=%s amount=%s new_balance=%s",
            user_id, code_arg.upper(), amount, new_balance,
        )
        return t(lang, "redeem_ok", amount=amount, balance=new_balance)

    err_key = _REDEEM_ERR_KEY_MAP.get(status, "redeem_error")
    log.info(
        "redeem fail telegram_id=%s code=%s status=%s",
        user_id, code_arg.upper(), status,
    )
    return t(lang, err_key)


# ==========================================
# Top-level reply-keyboard handlers (matched across all languages)
# ==========================================
# Legacy reply-keyboard handlers. P3-3 replaced the bottom keyboard
# with an inline hub, but old Telegram clients may still have a cached
# ReplyKeyboardMarkup from before the deploy. Until the user sends
# /start (which clears it via ReplyKeyboardRemove), tapping a legacy
# button still hits these handlers — we route them to the hub +
# strip the legacy keyboard.
#
# Defensive FSM clear: same reason as before — these are reachable
# from inside flows like waiting_custom_amount or waiting_promo_code,
# and we don't want stale FSM state intercepting the user's next text
# message after they bounce back to the hub. `state.clear()` is a no-op
# when no state is set so it's always safe.
async def _route_legacy_text_to_hub(message: Message, state: FSMContext):
    await state.clear()
    # ``message.from_user`` is None for anonymous-group / channel-bot
    # edge cases. The legacy reply-keyboard buttons are matched by
    # text equality so a group admin posting (e.g.) "👛 کیف پول" as
    # an anonymous admin would route here without ``from_user``.
    # Same guard pattern as cmd_start / process_chat.
    if message.from_user is None:
        return
    lang = await _get_user_language(message.from_user.id)
    await _send_hub(message, lang, remove_kb=True)


@router.message(F.text.in_(_SUPPORT_LABELS))
async def support_text_handler(message: Message, state: FSMContext):
    await _route_legacy_text_to_hub(message, state)


@router.message(F.text.in_(_LANGUAGE_LABELS))
async def language_text_handler(message: Message, state: FSMContext):
    await _route_legacy_text_to_hub(message, state)


# ==========================================
# Hub callbacks — each top-level button on the hub edits the message
# in place into the chosen screen. Every screen has a "🏠 Back to menu"
# button that routes back via ``back_to_hub``.
# ==========================================
@router.callback_query(F.data == "back_to_hub")
async def back_to_hub_handler(callback: CallbackQuery, state: FSMContext):
    # Defensive FSM clear — reachable from inside the charge flows.
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    await _edit_to_hub(callback, lang)
    await callback.answer()


# Stage-13-Step-A: ``✅ I've joined`` button on the required-channel
# gate. The middleware in ``force_join.py`` skips this exact callback
# so the handler can re-check membership without an infinite loop.
# Registered here (the public router) — admins skip the gate entirely
# in the middleware so they'd never tap this button.
@router.callback_query(F.data == FORCE_JOIN_CHECK_CALLBACK)
async def _force_join_check_handler(callback: CallbackQuery):
    await force_join_check_callback(callback)


@router.callback_query(F.data == "hub_wallet")
async def hub_wallet_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    lang = await _get_user_language(user_id)
    user_data = await db.get_user(user_id)
    # NaN guard — ``wallet_text`` formats ``${balance:.2f}`` directly,
    # so a corrupted ``users.balance_usd`` row would otherwise leak
    # ``$nan`` into the wallet view. Same regression Stage-13-Step-A
    # fixed for ``hub_title`` (see :func:`_hub_text_and_kb`); the
    # comment there mistakenly claimed ``wallet_text`` was already
    # protected via ``format_balance_block``, but ``format_balance_block``
    # was never wired into this handler — the wallet template still
    # goes through ``strings.t`` with the raw float. Falling back to
    # ``$0.00`` matches the hub policy.
    raw_balance = float(user_data["balance_usd"]) if user_data else 0.0
    balance = raw_balance if math.isfinite(raw_balance) else 0.0
    builder = _build_wallet_keyboard(lang)
    # Stage-11-Step-D: append the live USD→Toman annotation to the
    # USD figure when an FX snapshot is cached. ``format_toman_annotation``
    # returns ``""`` on a cold cache so a fresh deploy still renders
    # the wallet — just without the Toman line.
    snap = await get_usd_to_toman_snapshot()
    toman_line = format_toman_annotation(lang, balance, snap)
    await callback.message.edit_text(
        t(lang, "wallet_text", balance=balance, toman_line=toman_line),
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data == "hub_models")
async def hub_models_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    user = await db.get_user(callback.from_user.id)
    active_model = user["active_model"] if user else "—"
    await _send_provider_list(callback, edit=True, lang=lang, active_model=active_model)
    await callback.answer()


@router.callback_query(F.data == "hub_support")
async def hub_support_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    _back_to_menu_button(builder, lang)
    await callback.message.edit_text(
        t(lang, "support_text"),
        parse_mode="Markdown",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data == "hub_language")
async def hub_language_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_lang_fa"), callback_data="set_lang_fa")
    builder.button(text=t(lang, "btn_lang_en"), callback_data="set_lang_en")
    _back_to_menu_button(builder, lang)
    builder.adjust(2, 1)
    await callback.message.edit_text(
        t(lang, "language_picker_title"), reply_markup=builder.as_markup()
    )
    await callback.answer()


async def _render_memory_screen(callback: CallbackQuery, lang: str) -> None:
    """Render the conversation-memory settings screen (P3-5).

    Shows the user's current memory state, a toggle, and — if memory
    is enabled — a "Reset conversation" button. The screen also
    surfaces the cost trade-off in plain language so the user can
    make an informed call before flipping it on.
    """
    enabled = await db.get_memory_enabled(callback.from_user.id)
    state_label = t(lang, "memory_state_on" if enabled else "memory_state_off")
    text = t(lang, "memory_screen", state=state_label)

    builder = InlineKeyboardBuilder()
    if enabled:
        builder.button(
            text=t(lang, "btn_memory_disable"), callback_data="mem_toggle"
        )
        # Only meaningful when memory is on — otherwise there's nothing
        # to reset. Keep the screen tidy when it's off.
        builder.button(
            text=t(lang, "btn_memory_reset"), callback_data="mem_reset"
        )
    else:
        builder.button(
            text=t(lang, "btn_memory_enable"), callback_data="mem_toggle"
        )
    _back_to_menu_button(builder, lang)
    builder.adjust(1)
    # Wipe ("🆕 New chat") followed by a re-render is the canonical
    # case where the new screen content is identical to the existing
    # one (toggle state didn't change). Telegram raises
    # ``TelegramBadRequest: Message is not modified`` for that, which
    # propagates up the dispatcher as an error log line. The toast was
    # already shown, so the UX is fine — just swallow the no-op.
    #
    # Bundled bug fix (Stage-9-Step-1.5): pre-fix this swallowed every
    # ``Exception`` here including DB drops, network blips on the
    # Telegram session, and bot-was-blocked errors (``TelegramForbiddenError``)
    # — masking real bugs as a single ``log.debug`` line. Tighten to
    # ``TelegramBadRequest`` so only the legitimate "message is not
    # modified" / parse-mode no-op cases are silenced.
    try:
        await callback.message.edit_text(
            text, parse_mode="Markdown", reply_markup=builder.as_markup()
        )
    except TelegramBadRequest:
        log.debug("memory screen edit_text was a no-op", exc_info=True)


@router.callback_query(F.data == "hub_newchat")
async def hub_newchat_handler(callback: CallbackQuery, state: FSMContext):
    """Wipe the user's conversation buffer immediately.

    Tapping "🆕 New Chat" on the hub used to open the memory settings
    screen — confusing because the button sounded like an action but
    behaved like navigation. Now it does the obvious thing: clear the
    conversation buffer (free), confirm via toast, leave the user on
    the hub. Memory toggling lives behind a separate "🧠 Memory:
    ON/OFF" hub button.
    """
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    deleted = await db.clear_conversation(callback.from_user.id)
    if deleted == 0:
        # Nothing to clear — surface that explicitly so the user
        # doesn't think the button is broken. Also nudge them toward
        # the memory toggle if they expected memory to be on.
        memory_on = await db.get_memory_enabled(callback.from_user.id)
        if memory_on:
            await callback.answer(t(lang, "memory_reset_empty"))
        else:
            await callback.answer(
                t(lang, "newchat_no_memory_hint"), show_alert=True
            )
        return
    await callback.answer(t(lang, "memory_reset_done", count=deleted))


@router.callback_query(F.data == "hub_memory")
async def hub_memory_handler(callback: CallbackQuery, state: FSMContext):
    """Open the memory settings screen with the cost trade-off explainer.

    Reachable from the dedicated "🧠 Memory: ON/OFF" hub button. The
    screen shows the user's current memory state, a toggle, and (when
    memory is on) a "Start new chat" button — the latter is also
    available directly from the hub via ``hub_newchat``.
    """
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    await _render_memory_screen(callback, lang)
    await callback.answer()


@router.callback_query(F.data == "mem_toggle")
async def memory_toggle_handler(callback: CallbackQuery, state: FSMContext):
    """Flip the per-user memory_enabled flag and re-render the screen."""
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    current = await db.get_memory_enabled(callback.from_user.id)
    new_value = not current
    updated = await db.set_memory_enabled(callback.from_user.id, new_value)
    if not updated:
        # Should be unreachable thanks to the upsert middleware, but
        # surface as alert rather than silently no-op'ing.
        await callback.answer(t(lang, "ai_no_account"), show_alert=True)
        return
    # Telegram alert toast confirms the flip — same pattern as the
    # language switcher. Then re-render the screen so the buttons
    # reflect the new state immediately.
    await callback.answer(
        t(lang, "memory_toggled_on" if new_value else "memory_toggled_off")
    )
    await _render_memory_screen(callback, lang)


@router.callback_query(F.data == "mem_reset")
async def memory_reset_handler(callback: CallbackQuery, state: FSMContext):
    """Wipe the user's conversation buffer ("🆕 New chat")."""
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    deleted = await db.clear_conversation(callback.from_user.id)
    # Toast tells the user how many turns were cleared. If the buffer
    # was already empty (deleted=0), the message still makes sense
    # ("conversation reset, 0 messages cleared").
    await callback.answer(t(lang, "memory_reset_done", count=deleted))
    await _render_memory_screen(callback, lang)


@router.callback_query(F.data.startswith("set_lang_"))
async def set_language_handler(callback: CallbackQuery, state: FSMContext):
    new_lang = callback.data.removeprefix("set_lang_")
    if new_lang not in SUPPORTED_LANGUAGES:
        await callback.answer("Unknown language", show_alert=True)
        return
    # Clear FSM defensively (see top-level handlers comment); the language
    # picker is reachable while in waiting_custom_amount.
    await state.clear()
    await db.set_language(callback.from_user.id, new_lang)
    # Telegram alert (the small toast at the top of the chat) confirms
    # the switch took effect, then we edit the message back to the hub
    # rendered in the new language. No fresh chat bubble needed — the
    # hub message carries every label, including the "Language: 🇮🇷 …"
    # row, so the user sees the change reflected everywhere.
    await callback.answer(t(new_lang, "language_changed"))
    await _edit_to_hub(callback, new_lang)


# ==========================================
# Model picker (P2-4)
# ==========================================
# Two-step UI: provider list -> paginated model list -> "set active".
#
# Callback data fits Telegram's 64-byte cap by using short prefixes:
#   mp:<provider>:<page>   -> open provider's page (page is 0-indexed)
#   sm:<full_model_id>     -> set the model active
# Provider names are at most ~20 chars; OpenRouter model ids are
# typically <50 chars, so `sm:<id>` is well under the limit. Any
# pathologically long id (>62 bytes after prefix) is filtered out.
_MODELS_PER_PAGE = 8
_OTHERS_PER_PAGE = 8
_CB_MAX_BYTES = 60  # leave 4 bytes of headroom under Telegram's 64-byte cap

# Telegram caps each ``sendMessage`` body at 4096 characters; longer
# payloads come back as ``TelegramBadRequest: message is too long``.
# We sit a touch below that (4000) so we never have to worry about
# multi-byte UTF-8 inflating the wire size past the limit.
_TELEGRAM_MAX_MSG_CHARS = 4000


def _split_for_telegram(text: str, limit: int) -> list[str]:
    """Split *text* into chunks no longer than *limit* characters.

    Tries paragraph boundaries (``\\n\\n``) first, then line breaks,
    then word boundaries, then a hard cut. The goal is to never cut
    mid-word and to keep semantic units (code blocks, paragraphs)
    intact when there's room. Empty inputs return a single empty
    string so the calling code still emits one message.
    """
    if not text:
        return [""]
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[:limit]
        # Prefer paragraph break, then newline, then space.
        for sep in ("\n\n", "\n", " "):
            cut = window.rfind(sep)
            if cut > limit // 2:
                # Drop the separator from the head of the next chunk
                # to avoid leading whitespace lines.
                chunks.append(remaining[:cut])
                remaining = remaining[cut + len(sep):]
                break
        else:
            # No good break point in the window — hard cut.
            chunks.append(window)
            remaining = remaining[limit:]
    if remaining:
        chunks.append(remaining)
    return chunks

# P3-4: prominent providers get their own top-level button on the
# provider list. Everything else funnels into "🌐 Others" — a paginated
# secondary screen. The user explicitly asked for this curation
# ("only 5 company providers: OpenAI, Anthropic, DeepSeek, Google, xAI;
# put others in others tab"). Identifiers below match OpenRouter's
# slugs — note ``x-ai`` (with hyphen) for xAI, not ``xai``.
_PROMINENT_PROVIDERS: tuple[str, ...] = (
    "openai",
    "anthropic",
    "google",
    "x-ai",
    "deepseek",
)

# Per-provider icon for the picker buttons. Keys are OpenRouter slugs.
# Anything missing falls back to the generic 🤖 emoji prefix.
_PROVIDER_DISPLAY_OVERRIDE: dict[str, str] = {
    "openai": "🟢 OpenAI",
    "anthropic": "🟣 Anthropic",
    "google": "🔵 Google",
    "x-ai": "⚫ xAI",
    "deepseek": "🐋 DeepSeek",
}


def _provider_display(provider: str) -> str:
    """Pretty-print a provider id: 'meta-llama' -> 'Meta-Llama'."""
    if provider in _PROVIDER_DISPLAY_OVERRIDE:
        return _PROVIDER_DISPLAY_OVERRIDE[provider]
    return "-".join(part.capitalize() for part in provider.split("-")) if provider else provider


def _eligible_model(model: CatalogModel) -> bool:
    """Filter out models whose callback data wouldn't fit in 64 bytes."""
    return len(f"sm:{model.id}".encode("utf-8")) <= _CB_MAX_BYTES


def _is_free_model(model: CatalogModel) -> bool:
    """True if this model is OpenRouter's free tier.

    OpenRouter marks free variants with a ``:free`` suffix on the slug.
    We also accept any model whose published price is exactly $0/M on
    both sides as a defensive belt-and-braces check (it's how the
    catalog computes pricing today, but the suffix is the canonical
    signal).
    """
    if model.id.endswith(":free"):
        return True
    return (
        model.price.input_per_1m_usd == 0.0
        and model.price.output_per_1m_usd == 0.0
    )


def _strip_provider_prefix(name: str, provider: str) -> str:
    """Trim a redundant ``"<Provider>: "`` head from a model display name.

    OpenRouter names follow the pattern ``"OpenAI: GPT-4.1 Mini"``, but
    once the user has tapped 🟢 OpenAI we don't need to repeat it on
    every row — it's noise. We strip the prefix case-insensitively and
    also try the human-readable display label (so ``"x-ai"`` matches
    ``"xAI: ..."``). If no known prefix matches, we return the name
    unchanged so unfamiliar providers still render their full name.
    """
    candidates = {provider.lower()}
    pretty = _PROVIDER_DISPLAY_OVERRIDE.get(provider, "")
    if pretty:
        # Drop the leading emoji + space if present, keep the brand name.
        parts = pretty.split(" ", 1)
        if len(parts) == 2:
            candidates.add(parts[1].lower())
    candidates.add(provider.replace("-", "").lower())
    candidates.add(provider.replace("-", " ").lower())
    for prefix in candidates:
        head = f"{prefix}: "
        if name.lower().startswith(head):
            return name[len(head):]
    return name


async def _send_provider_list(
    message_or_callback,
    *,
    edit: bool,
    lang: str,
    active_model: str,
):
    """Render the provider-list screen, either as a fresh message or by
    editing the calling callback's message."""
    catalog = await get_catalog()
    if not catalog.by_provider:
        text = t(lang, "models_picker_empty")
        builder = InlineKeyboardBuilder()
        _back_to_menu_button(builder, lang)
        if edit:
            await message_or_callback.message.edit_text(text, reply_markup=builder.as_markup())
        else:
            await message_or_callback.answer(text, reply_markup=builder.as_markup())
        return

    # P3-4: render only the prominent providers as top-level buttons.
    # Everything else funnels through "🌐 Others" → a paginated screen
    # of less-popular providers. We still pre-compute eligibility so a
    # prominent provider with zero eligible chat models (post-modality
    # filter) doesn't render an empty bucket.
    eligible_count_by_provider: dict[str, int] = {}
    for provider, ms in catalog.by_provider.items():
        eligible = sum(1 for m in ms if _eligible_model(m))
        if eligible:
            eligible_count_by_provider[provider] = eligible

    builder = InlineKeyboardBuilder()
    for provider in _PROMINENT_PROVIDERS:
        if provider in eligible_count_by_provider:
            builder.button(
                text=_provider_display(provider),
                callback_data=f"mp:{provider}:0",
            )
    # P3-6: surface a top-level "🆓 Free models" entry that aggregates
    # every free-tier model across providers. Free models still appear
    # under their provider category (so OpenAI's free tier remains
    # under 🟢 OpenAI), but the dedicated entry makes them easy to find
    # — the user reported "free models are hard to find right now".
    has_free = any(
        _is_free_model(m) and _eligible_model(m)
        for ms in catalog.by_provider.values()
        for m in ms
    )
    if has_free:
        builder.button(
            text=t(lang, "btn_models_free"), callback_data="fm:0"
        )

    # Anything that isn't a prominent provider goes under the Others
    # bucket. We only render the entry button if there's at least one
    # eligible non-prominent provider — otherwise it's a dead-end click.
    others_count = sum(
        1 for p in eligible_count_by_provider if p not in _PROMINENT_PROVIDERS
    )
    if others_count:
        builder.button(
            text=t(lang, "btn_models_others"), callback_data="op:0"
        )
    _back_to_menu_button(builder, lang)
    builder.adjust(2)  # 2-wide grid for providers, footer auto-wraps

    title = t(lang, "models_picker_title", active_model=active_model)
    if catalog.is_fallback:
        title = title + "\n\n" + t(lang, "models_offline_warning")

    if edit:
        await message_or_callback.message.edit_text(
            title, reply_markup=builder.as_markup(), parse_mode="Markdown"
        )
    else:
        await message_or_callback.answer(
            title, reply_markup=builder.as_markup(), parse_mode="Markdown"
        )


@router.callback_query(F.data.startswith("mp:"))
async def show_provider_models(callback: CallbackQuery):
    """Show the paginated model list for the chosen provider."""
    try:
        _, provider, page_str = callback.data.split(":", 2)
        page = int(page_str)
    except (ValueError, IndexError):
        await callback.answer("Bad data", show_alert=True)
        return

    lang = await _get_user_language(callback.from_user.id)
    user = await db.get_user(callback.from_user.id)
    active_model = user["active_model"] if user else "—"

    catalog = await get_catalog()
    models = [m for m in catalog.by_provider.get(provider, ()) if _eligible_model(m)]
    if not models:
        await callback.answer(t(lang, "models_picker_empty"), show_alert=True)
        return

    total_pages = max(1, (len(models) + _MODELS_PER_PAGE - 1) // _MODELS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    page_slice = models[page * _MODELS_PER_PAGE : (page + 1) * _MODELS_PER_PAGE]

    builder = InlineKeyboardBuilder()
    for model in page_slice:
        marker = "✅ " if model.id == active_model else ""
        # Render the price the user will ACTUALLY be charged (raw
        # OpenRouter price * COST_MARKUP), not the upstream sticker
        # price. Otherwise the picker quotes one number and the wallet
        # deducts a different (larger) one — which the user caught.
        # Free-tier models ($0/M) collapse to $0 through the multiply
        # regardless of markup so the ":free" suffix semantics are
        # preserved.
        display_price = apply_markup_to_price(model.price)
        price_label = t(
            lang,
            "models_price_format",
            input=display_price.input_per_1m_usd,
            output=display_price.output_per_1m_usd,
        )
        # The user has already tapped this provider, so dropping the
        # "OpenAI: " / "Google: " prefix from each row removes
        # redundant noise (P3-6 feedback: "user knows he is using
        # google, dont need to say google: gemini").
        display_name = _strip_provider_prefix(model.name, provider)
        label = f"{marker}{display_name} • {price_label}"
        if len(label) > 60:
            label = label[:57] + "…"
        builder.button(text=label, callback_data=f"sm:{model.id}")
    # One model per row keeps the long labels readable.
    builder.adjust(*([1] * len(page_slice)))

    # Pagination row.
    nav = InlineKeyboardBuilder()
    if page > 0:
        nav.button(
            text=t(lang, "btn_models_prev_page"),
            callback_data=f"mp:{provider}:{page - 1}",
        )
    if page < total_pages - 1:
        nav.button(
            text=t(lang, "btn_models_next_page"),
            callback_data=f"mp:{provider}:{page + 1}",
        )
    nav.button(text=t(lang, "btn_back"), callback_data="mp_back")
    nav.button(text=t(lang, "btn_back_to_menu"), callback_data="back_to_hub")
    nav.adjust(2, 2)
    # Append the nav rows after the model rows.
    for row in nav.export():
        builder.row(*row)

    title = t(
        lang,
        "models_provider_title",
        provider=_provider_display(provider),
        active_model=active_model,
        page=page + 1,
        total_pages=total_pages,
    )
    await callback.message.edit_text(
        title, reply_markup=builder.as_markup(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "mp_back")
async def models_back_to_providers(callback: CallbackQuery):
    """Go back from the per-provider page to the provider list."""
    lang = await _get_user_language(callback.from_user.id)
    user = await db.get_user(callback.from_user.id)
    active_model = user["active_model"] if user else "—"
    await _send_provider_list(callback, edit=True, lang=lang, active_model=active_model)
    await callback.answer()


@router.callback_query(F.data.startswith("op:"))
async def show_others_providers(callback: CallbackQuery):
    """Paginated list of "non-prominent" providers under the Others bucket.

    P3-4: the main provider list shows only the 5 curated companies
    (OpenAI, Anthropic, Google, xAI, DeepSeek). Everything else lives
    here. Each row is one provider; tapping it drops into the existing
    per-provider model list (``mp:<provider>:0``).
    """
    try:
        page = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Bad data", show_alert=True)
        return

    lang = await _get_user_language(callback.from_user.id)
    catalog = await get_catalog()

    # Same eligibility criterion as _send_provider_list — keep behaviour
    # consistent so a provider showing here actually has tappable
    # models when the user clicks it.
    others: list[tuple[str, int]] = []
    for provider, ms in catalog.by_provider.items():
        if provider in _PROMINENT_PROVIDERS:
            continue
        eligible = sum(1 for m in ms if _eligible_model(m))
        if eligible:
            others.append((provider, eligible))
    others.sort(key=lambda pc: (-pc[1], pc[0]))

    # Defensive: if the catalog refreshed between the user seeing the
    # 🌐 Others entry and tapping it, every non-prominent provider may
    # now be empty (or filtered out). Without this guard we'd render
    # an Others screen with a title and Back button but zero rows —
    # confusing dead-end. Surface as an alert and bail out instead.
    # (Devin Review caught this on PR #28.)
    if not others:
        await callback.answer(t(lang, "models_picker_empty"), show_alert=True)
        return

    total_pages = max(1, (len(others) + _OTHERS_PER_PAGE - 1) // _OTHERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * _OTHERS_PER_PAGE
    page_slice = others[start : start + _OTHERS_PER_PAGE]

    builder = InlineKeyboardBuilder()
    for provider, count in page_slice:
        builder.button(
            text=f"{_provider_display(provider)} ({count})",
            callback_data=f"mp:{provider}:0",
        )
    builder.adjust(1)  # one provider per row in the Others list

    nav = InlineKeyboardBuilder()
    if page > 0:
        nav.button(
            text=t(lang, "btn_models_prev_page"),
            callback_data=f"op:{page - 1}",
        )
    if page < total_pages - 1:
        nav.button(
            text=t(lang, "btn_models_next_page"),
            callback_data=f"op:{page + 1}",
        )
    nav.button(text=t(lang, "btn_back"), callback_data="mp_back")
    nav.button(text=t(lang, "btn_back_to_menu"), callback_data="back_to_hub")
    nav.adjust(2, 2)
    for row in nav.export():
        builder.row(*row)

    title = t(
        lang,
        "models_others_title",
        page=page + 1,
        total_pages=total_pages,
    )
    await callback.message.edit_text(
        title, reply_markup=builder.as_markup(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("fm:"))
async def show_free_models(callback: CallbackQuery):
    """Paginated list of all free-tier models across providers.

    P3-6: the user reported that free models were hard to find under
    the per-provider buckets. This screen aggregates every model
    flagged free by ``_is_free_model`` into a single flat list,
    sorted by provider for grouping then by name. Free models still
    appear under their provider category — this is an additional
    entry point, not a relocation.
    """
    try:
        page = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Bad data", show_alert=True)
        return

    lang = await _get_user_language(callback.from_user.id)
    user = await db.get_user(callback.from_user.id)
    active_model = user["active_model"] if user else "—"

    catalog = await get_catalog()
    free_models: list[CatalogModel] = []
    for provider in sorted(catalog.by_provider):
        for m in catalog.by_provider[provider]:
            if _is_free_model(m) and _eligible_model(m):
                free_models.append(m)
    # Stable sort so pagination is deterministic across catalog refreshes.
    free_models.sort(key=lambda m: (m.provider, m.name.lower(), m.id))

    if not free_models:
        await callback.answer(t(lang, "models_picker_empty"), show_alert=True)
        return

    total_pages = max(1, (len(free_models) + _MODELS_PER_PAGE - 1) // _MODELS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    page_slice = free_models[page * _MODELS_PER_PAGE : (page + 1) * _MODELS_PER_PAGE]

    builder = InlineKeyboardBuilder()
    for model in page_slice:
        marker = "✅ " if model.id == active_model else ""
        # Inside the Free list we need the provider hint back on the
        # row (the user is browsing across providers here). For
        # prominent providers with an emoji override
        # ("🟢 OpenAI") we strip the leading emoji + space so the
        # row reads "OpenAI • <model>" without doubling the emoji.
        # For non-prominent providers (e.g. "Meta-Llama"), there's
        # nothing to strip — use the full pretty-printed display
        # string instead of falling back to the raw slug.
        full_display = _provider_display(model.provider)
        parts = full_display.split(" ", 1)
        provider_label = parts[1] if len(parts) == 2 else full_display
        clean_name = _strip_provider_prefix(model.name, model.provider)
        label = f"{marker}{provider_label} • {clean_name}"
        if len(label) > 60:
            label = label[:57] + "…"
        builder.button(text=label, callback_data=f"sm:{model.id}")
    builder.adjust(*([1] * len(page_slice)))

    nav = InlineKeyboardBuilder()
    if page > 0:
        nav.button(
            text=t(lang, "btn_models_prev_page"),
            callback_data=f"fm:{page - 1}",
        )
    if page < total_pages - 1:
        nav.button(
            text=t(lang, "btn_models_next_page"),
            callback_data=f"fm:{page + 1}",
        )
    nav.button(text=t(lang, "btn_back"), callback_data="mp_back")
    nav.button(text=t(lang, "btn_back_to_menu"), callback_data="back_to_hub")
    nav.adjust(2, 2)
    for row in nav.export():
        builder.row(*row)

    title = t(
        lang,
        "models_free_title",
        active_model=active_model,
        page=page + 1,
        total_pages=total_pages,
    )
    await callback.message.edit_text(
        title, reply_markup=builder.as_markup(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("sm:"))
async def set_active_model_handler(callback: CallbackQuery):
    """Set the user's active_model to the chosen catalog entry."""
    model_id = callback.data.removeprefix("sm:")
    lang = await _get_user_language(callback.from_user.id)

    catalog = await get_catalog()
    if catalog.get(model_id) is None:
        await callback.answer(t(lang, "models_set_unknown"), show_alert=True)
        return

    updated = await db.set_active_model(callback.from_user.id, model_id)
    if not updated:
        # User row missing — they need to /start. Surface as alert so we
        # don't strip the keyboard out from under them.
        await callback.answer(t(lang, "ai_no_account"), show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    _back_to_menu_button(builder, lang)
    await callback.message.edit_text(
        t(lang, "models_set_success", model_id=model_id),
        parse_mode="Markdown",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


# ==========================================
# Wallet → Redeem gift code (button-driven flow). The slash command
# ``/redeem CODE`` still works; this is the discoverability twin so
# users can find redemption from the wallet menu without typing.
# ==========================================
@router.callback_query(F.data == "hub_redeem_gift")
async def hub_redeem_gift_handler(callback: CallbackQuery, state: FSMContext):
    """Prompt the user for a gift code; arm the ``waiting_gift_code`` state."""
    lang = await _get_user_language(callback.from_user.id)
    await state.set_state(UserStates.waiting_gift_code)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(2)
    await callback.message.edit_text(
        t(lang, "redeem_input_prompt"),
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.message(UserStates.waiting_gift_code)
async def process_gift_code_input(message: Message, state: FSMContext):
    """Read the typed gift code, redeem it, surface the result.

    Same defensive ``from_user is None`` guard as the other
    waiting_* handlers (see PR #51 / cleanup PR comments). On any
    outcome — success or error — we drop the FSM state so the next
    message routes back to the AI chat handler.
    """
    if message.from_user is None:
        return
    user_id = message.from_user.id
    lang = await _get_user_language(user_id)
    code_arg = (message.text or "").strip()
    await state.set_state(None)
    if not code_arg:
        await message.answer(t(lang, "redeem_bad_code"))
        return
    reply = await _redeem_code_for_user(user_id, code_arg, lang)
    await message.answer(reply)


def _build_wallet_keyboard(lang: str) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_add_crypto"), callback_data="add_crypto")
    # HANDOFF Stage-8-Part-3 deferred this button: gift codes shipped
    # only as a slash command (``/redeem CODE``), which most users
    # don't discover. Surface a real wallet-menu button alongside
    # "Add crypto" so the redemption flow is reachable through buttons.
    builder.button(
        text=t(lang, "btn_redeem_gift"), callback_data="hub_redeem_gift"
    )
    # Stage-12-Step-C: a user-facing "Recent top-ups" view of the
    # transactions ledger. The data was previously only reachable
    # via the admin panel; routing it through a dedicated DB method
    # (`Database.list_user_transactions`) hard-codes the
    # ``WHERE telegram_id = …`` filter so a future caller can't drop
    # the user-scope by accident.
    builder.button(
        text=t(lang, "btn_receipts"), callback_data="hub_receipts"
    )
    _back_to_menu_button(builder, lang)
    builder.adjust(1, 1, 1, 1)
    return builder


# ==========================================
# Stage-12-Step-C: wallet → recent top-ups (paginated receipt feed).
# Cursor pagination over `transaction_id` so a fresh top-up landing
# while the user is browsing doesn't shift pages or surface dupes.
# Callback shape:
#   "hub_receipts"            → first page (no cursor)
#   "receipts_more:<before>"  → subsequent pages, ``<before>`` is the
#                                smallest tx-id from the previous page.
# ``Database.list_user_transactions`` hard-codes the
# ``WHERE telegram_id = …`` filter so the caller can't drop the
# user-scope by accident.
# ==========================================


def _build_receipts_keyboard(
    lang: str, *, next_before_id: int | None
) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    if next_before_id is not None:
        builder.button(
            text=t(lang, "btn_receipts_more"),
            callback_data=f"receipts_more:{int(next_before_id)}",
        )
    builder.button(
        text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet"
    )
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    if next_before_id is not None:
        builder.adjust(1, 2)
    else:
        builder.adjust(2)
    return builder


async def _render_receipts_page(
    callback: CallbackQuery, *, before_id: int | None
) -> None:
    """Render one page of receipts as an edit on the current message.

    Shared by ``hub_receipts_handler`` (initial page, ``before_id=None``)
    and ``receipts_more_handler`` (subsequent pages with the cursor
    embedded in the callback payload).
    """
    user_id = callback.from_user.id
    lang = await _get_user_language(user_id)
    page_size = get_receipts_page_size()
    try:
        page = await db.list_user_transactions(
            telegram_id=user_id,
            limit=page_size,
            before_id=before_id,
        )
    except ValueError:
        # Defensive — list_user_transactions raises on a missing /
        # zero / negative ``telegram_id`` to keep a future buggy
        # caller from leaking other users' rows. ``callback.from_user.id``
        # is always set for inline-button callbacks, but if Telegram
        # ever sends one without a from_user we'd rather show the
        # empty state than 500.
        log.exception("list_user_transactions refused for user %r", user_id)
        page = {"rows": [], "has_more": False, "next_before_id": None}

    rows = page["rows"]
    if not rows and before_id is None:
        # First-page empty state: brand-new user, no top-ups ever.
        text = (
            f"{t(lang, 'receipts_title')}\n\n"
            f"{t(lang, 'receipts_empty')}"
        )
    else:
        # Either a populated first page, or a "Show more" tail page.
        # Even a populated tail with zero rows still renders the
        # title (shouldn't happen — has_more=False would have hidden
        # the More button — but the fallback is "show the title and
        # the empty list" rather than crashing).
        body = format_receipts_page(rows, lang)
        text = f"{t(lang, 'receipts_title')}\n\n{body}"

    builder = _build_receipts_keyboard(
        lang, next_before_id=page["next_before_id"]
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=builder.as_markup(),
            parse_mode="Markdown",
        )
    except TelegramBadRequest:
        # Same "message is not modified" race as _edit_to_hub: a
        # double-tap on the same page renders identical text +
        # keyboard, which Telegram refuses to edit.
        log.debug("receipts edit was a no-op", exc_info=True)


@router.callback_query(F.data == "hub_receipts")
async def hub_receipts_handler(callback: CallbackQuery, state: FSMContext):
    """Render the first page of the user's recent top-ups."""
    # Defensive FSM clear — the wallet menu is reachable from inside
    # the charge flows.
    await state.clear()
    await _render_receipts_page(callback, before_id=None)
    await callback.answer()


@router.callback_query(F.data.startswith("receipts_more:"))
async def receipts_more_handler(callback: CallbackQuery, state: FSMContext):
    """Render the next page of receipts.

    The cursor is embedded in the callback payload; we parse it
    defensively (a tampered payload yields a fresh first-page render
    rather than a crash). Telegram callback_data is capped at 64
    bytes — a positive ``transaction_id`` integer always fits.
    """
    raw = (callback.data or "").split(":", 1)[1] if callback.data else ""
    try:
        before_id = int(raw)
        if before_id <= 0:
            before_id = None
    except (TypeError, ValueError):
        before_id = None
    await _render_receipts_page(callback, before_id=before_id)
    await callback.answer()


@router.message(F.text.in_(_WALLET_LABELS))
async def wallet_text_handler(message: Message, state: FSMContext):
    # Legacy reply-keyboard handler — see _route_legacy_text_to_hub.
    await _route_legacy_text_to_hub(message, state)


@router.message(F.text.in_(_MODEL_LABELS))
async def models_text_handler_legacy(message: Message, state: FSMContext):
    # Legacy reply-keyboard handler — see _route_legacy_text_to_hub.
    await _route_legacy_text_to_hub(message, state)


# ==========================================
# Inline navigation callbacks
# ==========================================
@router.callback_query(F.data == "close_menu")
async def close_menu_handler(callback: CallbackQuery, state: FSMContext):
    # P3-3: legacy alias. Pre-hub, this used to *delete* the inline
    # message; with the single-message hub UI we instead edit back to
    # the hub so the user always has the menu in front of them. Any
    # button that still emits ``callback_data="close_menu"`` from
    # older buttons effectively becomes "back to menu".
    #
    # FSM clear: same reason as before — this is reachable from inside
    # waiting_custom_amount / waiting_promo_code, so the user's next
    # free-text message must not be intercepted by those FSM handlers.
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    await _edit_to_hub(callback, lang)
    await callback.answer()


@router.callback_query(F.data == "back_to_wallet")
async def back_to_wallet_handler(callback: CallbackQuery, state: FSMContext):
    # Leaving the charge flow back to the wallet view: drop any
    # in-flight promo / custom_amount so they don't carry over to the
    # next charge. (Re-entering charge starts fresh.)
    await state.clear()
    user_id = callback.from_user.id
    lang = await _get_user_language(user_id)
    user_data = await db.get_user(user_id)
    # Same NaN guard the primary ``hub_wallet_handler`` uses; this
    # exit path lands on the same ``wallet_text`` template.
    raw_balance = float(user_data["balance_usd"]) if user_data else 0.0
    balance = raw_balance if math.isfinite(raw_balance) else 0.0
    builder = _build_wallet_keyboard(lang)
    snap = await get_usd_to_toman_snapshot()
    toman_line = format_toman_annotation(lang, balance, snap)
    text = t(lang, "wallet_text", balance=balance, toman_line=toman_line)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()


# ==========================================
# Charge wallet flow (3 steps)
# ==========================================

def _promo_banner(lang: str, data: dict) -> str | None:
    """Banner text describing the active promo, or None if none is set.

    Read from FSM data set by ``process_promo_input``. The banner is
    appended to the amount-picker title so the user always sees the
    promo they currently have applied.
    """
    code = data.get("promo_code")
    if not code:
        return None
    pct = data.get("promo_discount_percent")
    amt = data.get("promo_discount_amount")
    if pct is not None:
        return t(lang, "promo_active_banner_percent", code=code, percent=pct)
    if amt is not None:
        return t(lang, "promo_active_banner_amount", code=code, amount=float(amt))
    return None


async def _render_charge_pick_amount(message, lang: str, state: FSMContext) -> None:
    """Render (or re-render) the top-up amount picker.

    Keeps any in-flight promo state visible: the banner + a "remove
    promo" button replace the "add promo" button when an active promo
    is in FSM data. We always edit_text the message in place so the
    user sees the promo applied / removed without a fresh chat bubble.
    """
    data = await state.get_data()
    banner = _promo_banner(lang, data)

    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_amt_5"), callback_data="amt_5")
    builder.button(text=t(lang, "btn_amt_10"), callback_data="amt_10")
    builder.button(text=t(lang, "btn_amt_25"), callback_data="amt_25")
    builder.button(text=t(lang, "btn_amt_custom"), callback_data="amt_custom")
    # Stage-11-Step-B: Toman entry sits alongside USD custom. Wallet
    # balance is still in USD — this button just lets Iranian users
    # *type* the amount in TMN. The handler converts to USD on entry
    # using the live fx_rates ticker.
    builder.button(text=t(lang, "btn_amt_toman"), callback_data="amt_toman")
    if banner:
        # Promo applied → offer removal in place of add.
        builder.button(text=t(lang, "btn_promo_remove"), callback_data="remove_promo")
    else:
        builder.button(text=t(lang, "btn_promo_enter"), callback_data="enter_promo")
    builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    # Layout: 3 fixed-USD amounts | custom USD row | custom Toman row
    # | promo row | back+home.
    builder.adjust(3, 1, 1, 1, 2)

    text = t(lang, "charge_pick_amount")
    if banner:
        text = text + "\n\n" + banner

    await message.edit_text(
        text, parse_mode="Markdown", reply_markup=builder.as_markup()
    )


# Step 1: pick an amount
@router.callback_query(F.data == "add_crypto")
async def process_add_crypto_amount(callback: CallbackQuery, state: FSMContext):
    # This callback is also the "cancel" target of the custom-amount screen
    # (which puts the FSM in waiting_custom_amount) and the back path from
    # the promo-input screen (waiting_promo_code). Drop the FSM *state*
    # but keep FSM data so an already-applied promo or a typed-in custom
    # amount survives back-and-forth nav. (state.clear() would wipe both.)
    await state.set_state(None)
    lang = await _get_user_language(callback.from_user.id)
    await _render_charge_pick_amount(callback.message, lang, state)
    await callback.answer()


# ---- Promo code flow (sub-flow inside the charge wallet flow) ----

@router.callback_query(F.data == "enter_promo")
async def enter_promo_handler(callback: CallbackQuery, state: FSMContext):
    """Open the promo-input screen. The next free-text message is
    consumed by process_promo_input below."""
    await state.set_state(UserStates.waiting_promo_code)
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    # Cancel returns to the amount picker (which clears the FSM state
    # without dropping the rest of the charge data).
    builder.button(text=t(lang, "btn_cancel"), callback_data="add_crypto")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(2)
    await callback.message.edit_text(
        t(lang, "promo_prompt"), reply_markup=builder.as_markup()
    )
    await callback.answer()


@router.callback_query(F.data == "remove_promo")
async def remove_promo_handler(callback: CallbackQuery, state: FSMContext):
    """Drop the promo fields from FSM data and re-render the picker.

    Keeps any other in-flight charge data (e.g. ``custom_amount``)
    intact. The user gets a one-shot Telegram alert confirming the
    removal so they don't think the screen redraw is silent.
    """
    data = await state.get_data()
    for key in ("promo_code", "promo_discount_percent", "promo_discount_amount"):
        data.pop(key, None)
    await state.set_data(data)
    lang = await _get_user_language(callback.from_user.id)
    await _render_charge_pick_amount(callback.message, lang, state)
    await callback.answer(t(lang, "promo_removed"), show_alert=False)


@router.message(UserStates.waiting_promo_code)
async def process_promo_input(message: Message, state: FSMContext):
    """Validate the typed promo code; on success stash it in FSM data
    and bounce the user back to the amount picker. Errors keep the
    user in waiting_promo_code so they can retype without restarting.
    """
    # Anonymous-group-admin / channel-bot messages have ``from_user is
    # None``. Touching ``.id`` would crash with AttributeError and
    # bubble up as a poller-level error. Same defensive guard as
    # ``process_chat`` (PR #51): silently no-op so the user can retry
    # from a private chat or after re-anonymising.
    if message.from_user is None:
        return
    lang = await _get_user_language(message.from_user.id)
    code = (message.text or "").strip().upper()
    if not code:
        await message.answer(t(lang, "promo_invalid_unknown"))
        return

    result = await db.validate_promo_code(code, message.from_user.id)
    if isinstance(result, str):
        # Validation error key → render the matching i18n string and
        # leave the user in waiting_promo_code so they can retype.
        await message.answer(t(lang, f"promo_invalid_{result}"))
        return

    # Persist into FSM data alongside any custom_amount the user already
    # entered. Drop the waiting_promo_code state so process_chat can
    # take over for free-text again, but keep data.
    data = await state.get_data()
    data["promo_code"] = result["code"]
    data["promo_discount_percent"] = result["discount_percent"]
    data["promo_discount_amount"] = result["discount_amount"]
    await state.set_data(data)
    await state.set_state(None)

    # Confirmation message (sent fresh — we can't edit_text on a
    # message the user just typed). Then re-render the picker as a new
    # message so the inline keyboard is reachable again.
    if result["discount_percent"] is not None:
        confirm = t(
            lang,
            "promo_applied_percent",
            code=result["code"],
            percent=result["discount_percent"],
        )
    else:
        confirm = t(
            lang,
            "promo_applied_amount",
            code=result["code"],
            amount=float(result["discount_amount"]),
        )
    sent = await message.answer(confirm, parse_mode="Markdown")
    # Render the picker on the just-sent message (so its inline keyboard
    # carries the promo banner + remove button).
    await _render_charge_pick_amount(sent, lang, state)


# Step 2 (custom-amount path): prompt for free-text input
# IMPORTANT: the specific "amt_custom" / "amt_toman" handlers must be
# registered BEFORE the generic "amt_*" prefix handler. aiogram v3
# dispatches the first matching handler, so registering the prefix
# first would swallow "amt_custom" and the custom-amount flow would be
# unreachable.
@router.callback_query(F.data == "amt_custom")
async def process_custom_amount_request(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_custom_amount)
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_cancel"), callback_data="add_crypto")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(2)
    await callback.message.edit_text(
        t(lang, "charge_custom_prompt"), reply_markup=builder.as_markup()
    )
    await callback.answer()


# Stage-11-Step-B: Toman entry (lets an Iranian user type the amount
# in tomans, converts to USD on the fly). Registered BEFORE the
# generic ``amt_*`` handler for the same aiogram-dispatch reason noted
# above.
@router.callback_query(F.data == "amt_toman")
async def process_toman_amount_request(callback: CallbackQuery, state: FSMContext):
    lang = await _get_user_language(callback.from_user.id)
    snap = await get_usd_to_toman_snapshot()
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_cancel"), callback_data="add_crypto")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(2)
    if snap is None:
        # No rate, no meaningful Toman prompt. Tell the user clearly
        # and keep them on the amount picker via the Cancel button.
        # We do NOT transition to waiting_toman_amount because the
        # next message can't be parsed without a rate anyway.
        await callback.message.edit_text(
            t(lang, "charge_toman_no_rate"),
            reply_markup=builder.as_markup(),
        )
        await callback.answer()
        return
    await state.set_state(UserStates.waiting_toman_amount)
    min_toman = GLOBAL_MIN_TOPUP_USD * snap.toman_per_usd
    await callback.message.edit_text(
        t(
            lang, "charge_toman_prompt",
            rate_toman=snap.toman_per_usd,
            min_toman=min_toman,
        ),
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


def _render_min_amount_refusal(
    lang: str,
    *,
    currency: str,
    min_usd: float | None,
    attempted_usd: float,
) -> str:
    """Build the user-facing "your top-up is below the minimum" text.

    Picks the richest string the data supports:

    * ``charge_min_amount_with_min_and_alt`` — we know the minimum for
      the chosen coin AND we have a cheaper coin to suggest.
    * ``charge_min_amount_with_min`` — we know the minimum but no
      alternative in our supported list can absorb ``attempted_usd``
      (typically because ``attempted_usd`` itself is below the
      ``GLOBAL_MIN_TOPUP_USD`` floor, which applies to every coin).
    * ``charge_min_amount_unknown`` — lookup failed; we can only tell
      the user their amount is too small.

    Centralised here so the custom-amount and fixed-amount flows stay
    in lockstep and the MinAmountError fallback renders the same way
    as the proactive pre-flight check.
    """
    if min_usd is None:
        return t(lang, "charge_min_amount_unknown", currency=currency.upper())
    alt = find_cheaper_alternative(
        requested_usd=attempted_usd,
        excluded_currency=currency,
        candidates=list(SUPPORTED_PAY_CURRENCIES),
    )
    if alt is not None:
        alt_label, _alt_ticker = alt
        return t(
            lang, "charge_min_amount_with_min_and_alt",
            currency=currency.upper(),
            min_usd=min_usd,
            amount_usd=attempted_usd,
            alt_currency=alt_label,
        )
    return t(
        lang, "charge_min_amount_with_min",
        currency=currency.upper(), min_usd=min_usd,
    )


async def _preflight_min_amount_check(
    currency: str, attempted_usd: float
) -> tuple[bool, float | None]:
    """Proactively verify ``attempted_usd`` clears the effective floor
    for ``currency`` before we spend an HTTP call creating an invoice
    NowPayments will just reject.

    Returns ``(ok, min_usd)``:

    * ``ok=True``  — the amount is high enough; ``min_usd`` may be
      ``None`` (we couldn't look up a per-currency floor and only the
      $2 global floor applied) or a positive float (looked up cleanly).
    * ``ok=False`` — the amount is below ``max(GLOBAL_MIN_TOPUP_USD,
      per-currency NowPayments min)``. Callers render the refusal via
      :func:`_render_min_amount_refusal` with the returned ``min_usd``.

    A lookup failure with ``attempted_usd < GLOBAL_MIN_TOPUP_USD`` is
    reported as ``(False, None)`` so the UI falls back to the generic
    "unknown min" message — which is the honest answer when we can't
    name the coin-specific threshold.
    """
    # Ensure the cache has a reasonably fresh number for this coin.
    # ``get_min_amount_usd`` is a no-op HTTP-wise on a cache hit.
    per_currency_min = await get_min_amount_usd(currency)
    effective = effective_min_usd(currency)
    if attempted_usd + 1e-9 >= effective:
        return True, per_currency_min
    # Prefer the per-currency number in the refusal if we got one,
    # otherwise fall back to the global floor so the user still sees
    # a concrete minimum.
    reported_min = per_currency_min if per_currency_min is not None else (
        effective if effective > 0 else None
    )
    return False, reported_min


# Step 2 (fixed-amount path): pick a currency
@router.callback_query(F.data.startswith("amt_"))
async def process_add_crypto_currency(callback: CallbackQuery):
    amount = callback.data.split("_")[1]
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    for label, ticker in SUPPORTED_PAY_CURRENCIES:
        builder.button(text=label, callback_data=f"pay_{ticker}_{amount}")
    builder.button(text=t(lang, "btn_back"), callback_data="add_crypto")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    # Currency grid first, then the back / home footer on its own row.
    builder.adjust(*_CURRENCY_ROWS_LAYOUT, 2)
    await callback.message.edit_text(
        t(lang, "charge_pick_currency", amount=amount),
        parse_mode="Markdown",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.message(UserStates.waiting_custom_amount)
async def process_custom_amount_input(message: Message, state: FSMContext):
    # Same defensive guard as ``process_promo_input`` /
    # ``process_chat``: anonymous-group-admin messages set
    # ``from_user`` to None, and ``.id`` access would AttributeError.
    if message.from_user is None:
        return
    lang = await _get_user_language(message.from_user.id)
    # ``message.text`` is None for stickers / photos / voice / video
    # notes / etc. The previous ``message.text.strip()`` raised
    # ``AttributeError`` and bubbled up as a 500-style "Run polling"
    # crash for that user with no actionable message back. Coerce to
    # empty string so the float parse below fails the same way an
    # alphabetic message would and we route through ``charge_custom_invalid``.
    raw_text = (message.text or "").strip()
    # Stage-11-Step-B: route through ``amount_input.normalize_amount``
    # so fa-digits and thousand-separators work in the USD path too.
    # ``normalize_amount`` already rejects NaN / Inf / ≤0 / empty, so
    # a ``None`` return is unambiguously "invalid input".
    amount = normalize_amount(raw_text)
    if amount is None:
        await message.answer(t(lang, "charge_custom_invalid"))
        return

    if amount < GLOBAL_MIN_TOPUP_USD:
        await message.answer(t(lang, "charge_custom_min_error"))
        return

    # Hard upper bound — we don't want a fat-fingered $9999999999 to
    # create a NowPayments invoice we'd never close out. $10k is well
    # above any real top-up; flag and reject.
    if amount > 10_000:
        await message.answer(t(lang, "charge_custom_invalid"))
        return

    # Drop the waiting_custom_amount state so the user can chat freely
    # while the currency picker is on screen, but keep FSM data so:
    #   * the custom_amount we stash here survives until the user taps
    #     a currency (process_custom_currency_selection reads it back),
    #   * any active promo also survives (it's separate FSM data set by
    #     process_promo_input).
    # state.clear() would wipe both name and data — set_state(None)
    # only drops the name.
    await state.set_state(None)
    await state.update_data(custom_amount=amount)

    builder = InlineKeyboardBuilder()
    for label, ticker in SUPPORTED_PAY_CURRENCIES:
        builder.button(text=label, callback_data=f"cur_{ticker}")
    # Footer: back to amount entry + home. Going back to amt_custom
    # re-prompts for the amount (without dropping promo state) so the
    # user can pick a different value without restarting from the wallet.
    builder.button(text=t(lang, "btn_back"), callback_data="amt_custom")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(*_CURRENCY_ROWS_LAYOUT, 2)

    await message.answer(
        t(lang, "charge_custom_amount_saved", amount=amount),
        reply_markup=builder.as_markup(),
    )


@router.message(UserStates.waiting_toman_amount)
async def process_toman_amount_input(message: Message, state: FSMContext):
    """Stage-11-Step-B: accept a free-text Toman amount, convert to
    USD via the live fx_rates ticker, and hand off to the same
    currency picker the USD path uses.

    The wallet is always denominated in USD — conversion happens
    *here* at entry time. The USD figure we stash in FSM data
    (``custom_amount``) flows through the existing NowPayments
    invoice path unchanged; Toman is input-layer only.

    If the rate ticker has no rate at all (cold boot, prolonged
    source outage), we refuse rather than guess — the prompt
    already told the user the rate was unavailable.
    """
    if message.from_user is None:
        return
    lang = await _get_user_language(message.from_user.id)
    raw_text = (message.text or "").strip()
    entered_toman = normalize_amount(raw_text)
    if entered_toman is None:
        await message.answer(t(lang, "charge_toman_invalid"))
        return

    # Stage-11-Step-D bundled bug fix: read the FX snapshot ONCE and
    # compute the USD figure from that single snapshot, instead of
    # the previous double-read (``convert_toman_to_usd`` then a
    # separate ``get_usd_to_toman_snapshot``). The pre-fix pair could
    # observe two different cache values if the background refresher
    # rotated the snapshot between the two awaits, leaving the
    # (entered_toman, usd_amount, toman_rate_at_entry) triple
    # internally inconsistent — the user saw "X TMN ≈ Y USD at Z TMN/USD"
    # where Y/Z != entered_toman by a few percent. Single read makes
    # the triple a closed identity again.
    snap = await get_usd_to_toman_snapshot()
    if snap is None or snap.toman_per_usd <= 0:
        await message.answer(t(lang, "charge_toman_no_rate"))
        return
    usd_amount = entered_toman / snap.toman_per_usd

    # Reject fat-fingered entries below $2 equivalent using the same
    # threshold the USD path uses, but render the error in Toman so
    # the user sees what they actually typed.
    if usd_amount < GLOBAL_MIN_TOPUP_USD:
        min_toman = GLOBAL_MIN_TOPUP_USD * snap.toman_per_usd
        await message.answer(
            t(
                lang, "charge_toman_min_error",
                min_toman=min_toman,
                entered_toman=entered_toman,
            )
        )
        return

    # Upper bound on the converted USD figure — same $10k rail the
    # USD path uses. A 1 000 000 000 TMN entry should fail cleanly
    # rather than create a $10k+ invoice.
    if usd_amount > 10_000:
        await message.answer(t(lang, "charge_toman_invalid"))
        return

    # Round the USD figure to cents so downstream DECIMAL columns and
    # NowPayments invoice totals render as ``$12.34`` rather than
    # ``$12.345678``. We keep the raw Toman entry in FSM data for
    # the confirmation string only.
    usd_amount_rounded = round(usd_amount, 2)

    await state.set_state(None)
    await state.update_data(
        custom_amount=usd_amount_rounded,
        toman_entry=entered_toman,
        toman_rate_at_entry=snap.toman_per_usd,
    )

    builder = InlineKeyboardBuilder()
    # Stage-11-Step-C: surface the TetraPay (Rial card) option on its
    # own row at the top of the keyboard. Only on the Toman entry path
    # — TetraPay quotes IRR and we have the locked rate in FSM data,
    # so we can compute the exact rial figure. The USD entry path
    # (``process_custom_amount_input``) deliberately does NOT show
    # this button: paying $5 of arbitrary rial without an entry-side
    # rate lock would expose users to settlement-time rate moves.
    builder.button(text=t(lang, "tetrapay_button"), callback_data="cur_tetrapay")
    for label, ticker in SUPPORTED_PAY_CURRENCIES:
        builder.button(text=label, callback_data=f"cur_{ticker}")
    # Back returns to the Toman prompt, not the USD one.
    builder.button(text=t(lang, "btn_back"), callback_data="amt_toman")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    # Layout: TetraPay button alone on top, then the existing 3-wide
    # crypto grid, then the back/home footer.
    builder.adjust(1, *_CURRENCY_ROWS_LAYOUT, 2)

    await message.answer(
        t(
            lang, "charge_toman_amount_saved",
            entered_toman=entered_toman,
            amount=usd_amount_rounded,
            rate_toman=snap.toman_per_usd,
        ),
        reply_markup=builder.as_markup(),
    )


async def _start_tetrapay_invoice(
    callback: CallbackQuery,
    state: FSMContext,
    *,
    lang: str,
    amount_usd: float,
    toman_rate_at_entry: float | None,
    promo_code: str | None,
    promo_bonus_usd: float,
) -> None:
    """Stage-11-Step-C: spin up a TetraPay (Rial card) order for the user.

    Reads the locked rate the user agreed to in
    ``process_toman_amount_input`` (stashed in FSM as
    ``toman_rate_at_entry``) and uses it as the order's per-invoice
    rate lock. The same rate is recorded on the PENDING ``transactions``
    row's ``gateway_locked_rate_toman_per_usd`` for audit.

    Failure modes (all of which are user-facing):

    * The locked rate is missing or non-finite — the FSM data was
      corrupted between Toman entry and currency picking. Render the
      same "no rate available" message the Toman-entry path uses.
    * :func:`tetrapay.create_order` raises (transport, API key
      missing, gateway returned non-100). Render
      ``tetrapay_unreachable``.
    * :meth:`Database.create_pending_transaction` returns ``False``
      (defensive guards or gateway_invoice_id collision). Render
      ``charge_invoice_error`` and log loudly — collision on
      cryptographically random Authority is statistically impossible
      so we'd want to know.
    """
    await state.clear()
    await callback.message.edit_text(t(lang, "tetrapay_creating_order"))

    if (
        toman_rate_at_entry is None
        or not isinstance(toman_rate_at_entry, (int, float))
    ):
        log.error(
            "TetraPay order start: missing toman_rate_at_entry in FSM "
            "for user=%s; refusing", callback.from_user.id,
        )
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="amt_toman")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_toman_no_rate"), reply_markup=builder.as_markup()
        )
        await callback.answer()
        return

    try:
        order = await tetrapay_create_order(
            amount_usd=amount_usd,
            rate_toman_per_usd=float(toman_rate_at_entry),
            description="Meowassist wallet top-up",
            user_id=callback.from_user.id,
        )
    except Exception as exc:
        # Catches both ``TetraPayError`` (gateway returned non-100,
        # missing API key, missing fields in response) AND transport
        # errors (aiohttp.ClientError, asyncio.TimeoutError). Same
        # fail-shape as the NowPayments path: log + user-facing
        # "gateway is down" message + retry button.
        log.exception(
            "TetraPay create_order failed for user=%s amount_usd=%.2f: %s",
            callback.from_user.id, amount_usd, exc,
        )
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="amt_toman")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "tetrapay_unreachable"), reply_markup=builder.as_markup()
        )
        await callback.answer()
        return

    # Persist the PENDING row BEFORE handing the user the redirect URL.
    # If we can't persist, the webhook won't be able to credit on
    # settlement, so we MUST refuse the invoice here (mirrors the
    # NowPayments path's ``if create_pending_transaction failed →
    # return None`` pattern).
    inserted = await db.create_pending_transaction(
        telegram_id=callback.from_user.id,
        gateway="tetrapay",
        currency_used="IRR",
        amount_crypto=float(order.amount_irr),
        amount_usd=order.amount_usd,
        gateway_invoice_id=order.authority,
        promo_code=promo_code,
        promo_bonus_usd=promo_bonus_usd,
        gateway_locked_rate_toman_per_usd=order.locked_rate_toman_per_usd,
    )
    if not inserted:
        log.error(
            "TetraPay create_pending_transaction refused for "
            "authority=%s user=%s — defensive guard fired or "
            "duplicate authority (statistically impossible)",
            order.authority, callback.from_user.id,
        )
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="amt_toman")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_invoice_error"), reply_markup=builder.as_markup()
        )
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "tetrapay_pay_button"), url=order.payment_url)
    builder.button(text=t(lang, "btn_back"), callback_data="amt_toman")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(1, 2)
    await callback.message.edit_text(
        t(
            lang, "tetrapay_order_text",
            amount_irr=order.amount_irr,
            amount_usd=order.amount_usd,
            rate_toman=order.locked_rate_toman_per_usd,
        ),
        parse_mode="Markdown",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cur_"))
async def process_custom_currency_selection(callback: CallbackQuery, state: FSMContext):
    currency = callback.data.split("_", 1)[1]
    lang = await _get_user_language(callback.from_user.id)

    data = await state.get_data()
    amount = data.get("custom_amount")
    if not amount:
        await callback.answer(t(lang, "charge_amount_lost"), show_alert=True)
        return

    # Pull any active promo from FSM data and compute the bonus
    # *before* we wipe state. The bonus rides along on the PENDING
    # transaction row and is only credited on SUCCESS (see
    # database.finalize_payment).
    promo_code = data.get("promo_code")
    promo_bonus_usd = db.compute_promo_bonus(
        float(amount),
        discount_percent=data.get("promo_discount_percent"),
        discount_amount=data.get("promo_discount_amount"),
    ) if promo_code else 0.0

    # Stage-11-Step-C: TetraPay (Rial card / Shaparak) branch. Branches
    # off here BEFORE the NowPayments-specific pre-flight min-amount
    # check (which only applies to crypto floors). The TetraPay path
    # has no per-currency NowPayments minimum because it's not a
    # crypto invoice — the only floor is ``GLOBAL_MIN_TOPUP_USD``,
    # already enforced upstream in ``process_toman_amount_input``.
    if currency == "tetrapay":
        await _start_tetrapay_invoice(
            callback,
            state,
            lang=lang,
            amount_usd=float(amount),
            toman_rate_at_entry=data.get("toman_rate_at_entry"),
            promo_code=promo_code,
            promo_bonus_usd=promo_bonus_usd,
        )
        return

    await state.clear()
    await callback.message.edit_text(t(lang, "charge_creating_invoice"))

    # Pre-flight: ask NowPayments if ``amount`` clears the effective
    # floor for ``currency`` before we spend a POST /v1/payment on it.
    # This saves a round-trip for the common "user tries $2 in BTC,
    # min is $10" case and also unlocks the alternative-coin suggestion
    # without having to wait for the gateway-side rejection.
    ok, pre_min_usd = await _preflight_min_amount_check(
        currency, float(amount)
    )
    if not ok:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = _render_min_amount_refusal(
            lang,
            currency=currency,
            min_usd=pre_min_usd,
            attempted_usd=float(amount),
        )
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        return

    try:
        invoice = await create_crypto_invoice(
            callback.from_user.id,
            amount_usd=float(amount),
            currency=currency,
            promo_code=promo_code,
            promo_bonus_usd=promo_bonus_usd,
        )
    except MinAmountError as e:
        # Pre-flight gave us a clean answer but the gateway still
        # rejected — floor must have shifted between our cache and
        # the POST. Render the same refusal shape as the pre-flight
        # path for UX consistency.
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = _render_min_amount_refusal(
            lang,
            currency=e.currency,
            min_usd=e.min_usd,
            attempted_usd=float(amount),
        )
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        return
    except Exception:
        log.exception(
            "Failed to create custom-amount invoice for user %d", callback.from_user.id
        )
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_invoice_error"), reply_markup=builder.as_markup()
        )
        await callback.answer()
        return

    if invoice:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = t(
            lang,
            "charge_invoice_text",
            amount=amount,
            currency=currency.upper(),
            pay_amount=invoice.get("pay_amount"),
            pay_address=invoice.get("pay_address"),
        )
        await callback.message.edit_text(
            text, parse_mode="Markdown", reply_markup=builder.as_markup()
        )
    else:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_gateway_unreachable"), reply_markup=builder.as_markup()
        )

    await callback.answer()


# Step 3: emit the final invoice (fixed-amount path)
@router.callback_query(F.data.startswith("pay_"))
async def process_final_invoice(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    lang = await _get_user_language(callback.from_user.id)
    if len(parts) != 3:
        await callback.answer("Invalid data", show_alert=True)
        return

    currency = parts[1]
    amount = parts[2]

    # Pull any active promo from FSM data and compute the bonus before
    # invoice creation (see process_custom_currency_selection for the
    # parallel path).
    data = await state.get_data()
    promo_code = data.get("promo_code")
    promo_bonus_usd = db.compute_promo_bonus(
        float(amount),
        discount_percent=data.get("promo_discount_percent"),
        discount_amount=data.get("promo_discount_amount"),
    ) if promo_code else 0.0

    await state.clear()
    await callback.message.edit_text(t(lang, "charge_creating_invoice"))

    # Pre-flight: identical reasoning to the custom-amount path above.
    # A $5-preset in BTC can still trip the per-currency min when
    # network fees spike, so we never skip the check.
    ok, pre_min_usd = await _preflight_min_amount_check(
        currency, float(amount)
    )
    if not ok:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = _render_min_amount_refusal(
            lang,
            currency=currency,
            min_usd=pre_min_usd,
            attempted_usd=float(amount),
        )
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        return

    try:
        invoice = await create_crypto_invoice(
            callback.from_user.id,
            amount_usd=float(amount),
            currency=currency,
            promo_code=promo_code,
            promo_bonus_usd=promo_bonus_usd,
        )
    except MinAmountError as e:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = _render_min_amount_refusal(
            lang,
            currency=e.currency,
            min_usd=e.min_usd,
            attempted_usd=float(amount),
        )
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        return
    except Exception:
        log.exception("Failed to create invoice for user %d", callback.from_user.id)
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_invoice_error"), reply_markup=builder.as_markup()
        )
        await callback.answer()
        return

    if invoice:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        text = t(
            lang,
            "charge_invoice_text",
            amount=amount,
            currency=currency.upper(),
            pay_amount=invoice.get("pay_amount"),
            pay_address=invoice.get("pay_address"),
        )
        await callback.message.edit_text(
            text, parse_mode="Markdown", reply_markup=builder.as_markup()
        )
    else:
        builder = InlineKeyboardBuilder()
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            t(lang, "charge_gateway_unreachable_long"),
            reply_markup=builder.as_markup(),
        )

    await callback.answer()


# ==========================================
# AI chat (free-text outside any reserved button or FSM state)
# ==========================================
@router.message(F.text & ~F.text.startswith("/"))
async def process_chat(message: Message):
    # Reserved-buttons guard: the bottom reply-keyboard buttons reach this
    # handler too because they're plain text. Drop them — there's a
    # dedicated handler for each, registered above.
    if message.text in _ALL_KBD_LABELS:
        return

    # Anonymous group admins, sender_chat-only forwards and certain
    # channel-attribution edge cases land here with ``from_user is
    # None``. We can't bill or rate-limit those — drop silently
    # instead of crashing on ``message.from_user.id``.
    if message.from_user is None:
        log.info(
            "process_chat: dropping message with no from_user "
            "(chat_id=%s, text=%r)",
            message.chat.id, (message.text or "")[:40],
        )
        return
    user_id = message.from_user.id

    # Per-user chat rate limit. Scoped to *this* handler (not a
    # dispatcher-wide middleware) so commands, FSM-state inputs
    # (waiting_custom_amount / waiting_promo_code), and reply-keyboard
    # buttons aren't throttled — they don't cost OpenRouter money.
    if not await consume_chat_token(user_id):
        lang = await _get_user_language(user_id)
        log.info(
            "chat rate-limited telegram_id=%s text=%r",
            user_id,
            (message.text or "")[:40],
        )
        await message.answer(t(lang, "ai_local_rate_limited"))
        return

    # Stage-13-Step-B: at most ONE in-flight OpenRouter request per
    # user. The token bucket above gates throughput; this slot gates
    # *concurrency*. A user firing 5 prompts back-to-back without
    # this slot would otherwise drain $5+ from their wallet in under
    # a second before the bucket reacts. Reject the second prompt
    # with the ``ai_chat_busy`` flash so the user gets clear feedback
    # instead of silent loss + a delayed cost they can't predict.
    if not await try_claim_chat_slot(user_id):
        lang = await _get_user_language(user_id)
        log.info(
            "chat in-flight rejected telegram_id=%s text=%r",
            user_id,
            (message.text or "")[:40],
        )
        await message.answer(t(lang, "ai_chat_busy"))
        return

    try:
        await message.bot.send_chat_action(
            chat_id=message.chat.id, action="typing"
        )
        reply = await chat_with_model(user_id, message.text)
    finally:
        # Idempotent — release in a finally so an OpenRouter
        # exception, a TelegramAPIError on send_chat_action, or any
        # other unexpected raise can't permanently lock the user
        # out of further chats.
        await release_chat_slot(user_id)

    # Stage-13-Step-B bundled bug fix: defend against
    # ``chat_with_model`` returning ``None`` or an empty string.
    # OpenRouter's chat-completion shape carries ``content`` as
    # required, but the spec also lets it be ``null`` for tool-call
    # responses or upstream-policy refusals; the existing 200-with-
    # error guard at ``ai_engine.py`` catches the explicit
    # ``{"error": ...}`` shape but not the ``content: null`` case.
    # Pre-fix, an upstream refusal would surface a literal ``None``
    # → ``_split_for_telegram(None, ...)`` → ``[""]`` → Telegram
    # rejects the send with ``Bad Request: message text is empty``,
    # bubbling up as a poller-level crash for that user with no
    # actionable message back. Treat empty / falsy as the same
    # ``ai_provider_unavailable`` we already render for the explicit
    # error-body shape.
    if not reply:
        lang = await _get_user_language(user_id)
        log.warning(
            "chat_with_model returned empty/None reply for user_id=%s; "
            "falling back to provider-unavailable text",
            user_id,
        )
        await message.answer(t(lang, "ai_provider_unavailable"))
        return

    # Telegram caps a single message at 4096 characters. Long-form
    # AI replies (essay-style answers, code blocks, etc.) routinely
    # exceed that and were crashing the send with
    # ``TelegramBadRequest: message is too long``. Chunk on a
    # paragraph / line / hard boundary, in that order, so the split
    # falls on a natural break when possible.
    for chunk in _split_for_telegram(reply, _TELEGRAM_MAX_MSG_CHARS):
        await message.answer(chunk)
