import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ai_engine import chat_with_model
from database import db
from payments import create_crypto_invoice
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, all_button_labels, t

log = logging.getLogger("bot.handlers")

router = Router()


class UserStates(StatesGroup):
    waiting_custom_amount = State()


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
# Bottom reply keyboard (always visible at the top level)
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
# COMMAND: /start
# ==========================================
@router.message(Command("start"))
async def cmd_start(message: Message):
    await db.create_user(message.from_user.id, message.from_user.username or "Unknown")
    lang = await _get_user_language(message.from_user.id)
    text = t(lang, "start_greeting", first_name=message.from_user.first_name or "")
    await message.answer(text, reply_markup=get_main_keyboard(lang))


# ==========================================
# Top-level reply-keyboard handlers (matched across all languages)
# ==========================================
# Top-level reply-keyboard handlers all defensively clear the FSM. The
# user can reach these by tapping the bottom keyboard from inside any
# screen, including FSM-active flows like the custom-amount entry. If we
# left the FSM set, the user's next free-text message would be
# intercepted by `process_custom_amount_input` instead of `process_chat`.
# `state.clear()` on a session with no state is a no-op so this is safe
# to apply unconditionally. Same class of trap as #15 fixed for the home
# button; this generalizes the fix.
@router.message(F.text.in_(_SUPPORT_LABELS))
async def support_text_handler(message: Message, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(message.from_user.id)
    await message.answer(t(lang, "support_text"), parse_mode="Markdown")


@router.message(F.text.in_(_LANGUAGE_LABELS))
async def language_text_handler(message: Message, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(message.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_lang_fa"), callback_data="set_lang_fa")
    builder.button(text=t(lang, "btn_lang_en"), callback_data="set_lang_en")
    builder.button(text=t(lang, "btn_close_menu"), callback_data="close_menu")
    builder.adjust(2, 1)
    await message.answer(t(lang, "language_picker_title"), reply_markup=builder.as_markup())


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
    # Show the confirmation in the *new* language so the user immediately
    # sees the switch took effect.
    await callback.message.edit_text(t(new_lang, "language_changed"))
    # The bottom reply keyboard was rendered before the user changed
    # language; re-render it so the four top-level buttons are localized.
    # Send as a fresh message because edit_text can't change reply_markup
    # to a ReplyKeyboardMarkup (only inline markups are editable).
    await callback.message.answer(
        t(new_lang, "start_greeting", first_name=callback.from_user.first_name or ""),
        reply_markup=get_main_keyboard(new_lang),
    )
    await callback.answer()


@router.message(F.text.in_(_MODEL_LABELS))
async def models_text_handler(message: Message, state: FSMContext):
    await state.clear()
    lang = await _get_user_language(message.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_close_menu"), callback_data="close_menu")
    await message.answer(
        t(lang, "models_text"), reply_markup=builder.as_markup(), parse_mode="Markdown"
    )


def _build_wallet_keyboard(lang: str) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_add_crypto"), callback_data="add_crypto")
    builder.button(text=t(lang, "btn_close_menu"), callback_data="close_menu")
    builder.adjust(1, 1)
    return builder


@router.message(F.text.in_(_WALLET_LABELS))
async def wallet_text_handler(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    lang = await _get_user_language(user_id)
    user_data = await db.get_user(user_id)
    balance = float(user_data["balance_usd"]) if user_data else 0.0
    text = t(lang, "wallet_text", balance=balance)
    builder = _build_wallet_keyboard(lang)
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


# ==========================================
# Inline navigation callbacks
# ==========================================
@router.callback_query(F.data == "close_menu")
async def close_menu_handler(callback: CallbackQuery, state: FSMContext):
    # The 🏠 home button is reachable from the custom-amount entry screen,
    # which sets the FSM to UserStates.waiting_custom_amount. Without
    # clearing here, the user's next free-text message is intercepted by
    # process_custom_amount_input (it expects an amount) instead of
    # process_chat — so AI chat would silently break until the user
    # restarts the charge flow.
    await state.clear()
    await callback.message.delete()
    await callback.answer()


@router.callback_query(F.data == "back_to_wallet")
async def back_to_wallet_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await _get_user_language(user_id)
    user_data = await db.get_user(user_id)
    balance = float(user_data["balance_usd"]) if user_data else 0.0
    builder = _build_wallet_keyboard(lang)
    text = t(lang, "wallet_text", balance=balance)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()


# ==========================================
# Charge wallet flow (3 steps)
# ==========================================

# Step 1: pick an amount
@router.callback_query(F.data == "add_crypto")
async def process_add_crypto_amount(callback: CallbackQuery, state: FSMContext):
    # This callback is also the "cancel" target of the custom-amount screen
    # (which puts the FSM in waiting_custom_amount). Clear any lingering state
    # so the user isn't stuck — otherwise their next free-text message would
    # be intercepted by process_custom_amount_input instead of process_chat.
    await state.clear()
    lang = await _get_user_language(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_amt_5"), callback_data="amt_5")
    builder.button(text=t(lang, "btn_amt_10"), callback_data="amt_10")
    builder.button(text=t(lang, "btn_amt_20"), callback_data="amt_20")
    builder.button(text=t(lang, "btn_amt_custom"), callback_data="amt_custom")
    builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(3, 1, 2)
    await callback.message.edit_text(
        t(lang, "charge_pick_amount"), reply_markup=builder.as_markup()
    )
    await callback.answer()


# Step 2 (custom-amount path): prompt for free-text input
# IMPORTANT: the specific "amt_custom" handler must be registered BEFORE the
# generic "amt_*" prefix handler. aiogram v3 dispatches the first matching
# handler, so registering the prefix first would swallow "amt_custom" and
# the custom-amount flow would be unreachable.
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
    lang = await _get_user_language(message.from_user.id)
    try:
        amount = float(message.text.strip().replace("$", ""))
    except ValueError:
        await message.answer(t(lang, "charge_custom_invalid"))
        return

    if amount < 5:
        await message.answer(t(lang, "charge_custom_min_error"))
        return

    # Drop the waiting_custom_amount state so the user can chat freely
    # while the currency picker is on screen, but stash the amount in
    # FSM data so process_custom_currency_selection can read it back
    # when they tap a currency. (state.clear() wipes both the state
    # name and the data, so update_data() must come after.)
    await state.clear()

    builder = InlineKeyboardBuilder()
    for label, ticker in SUPPORTED_PAY_CURRENCIES:
        builder.button(text=label, callback_data=f"cur_{ticker}")
    # Footer: back to amount entry + home. Going back to amt_custom
    # re-prompts for the amount (clearing state) so the user can pick a
    # different value without restarting from the wallet.
    builder.button(text=t(lang, "btn_back"), callback_data="amt_custom")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    builder.adjust(*_CURRENCY_ROWS_LAYOUT, 2)

    await state.update_data(custom_amount=amount)

    await message.answer(
        t(lang, "charge_custom_amount_saved", amount=amount),
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("cur_"))
async def process_custom_currency_selection(callback: CallbackQuery, state: FSMContext):
    currency = callback.data.split("_")[1]
    lang = await _get_user_language(callback.from_user.id)

    data = await state.get_data()
    amount = data.get("custom_amount")
    if not amount:
        await callback.answer(t(lang, "charge_amount_lost"), show_alert=True)
        return

    await state.clear()
    await callback.message.edit_text(t(lang, "charge_creating_invoice"))

    try:
        invoice = await create_crypto_invoice(
            callback.from_user.id, amount_usd=float(amount), currency=currency
        )
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
async def process_final_invoice(callback: CallbackQuery):
    parts = callback.data.split("_")
    lang = await _get_user_language(callback.from_user.id)
    if len(parts) != 3:
        await callback.answer("Invalid data", show_alert=True)
        return

    currency = parts[1]
    amount = parts[2]

    await callback.message.edit_text(t(lang, "charge_creating_invoice"))

    try:
        invoice = await create_crypto_invoice(
            callback.from_user.id, amount_usd=float(amount), currency=currency
        )
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

    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    reply = await chat_with_model(message.from_user.id, message.text)
    await message.answer(reply)
