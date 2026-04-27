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
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ai_engine import chat_with_model
from database import db
from models_catalog import CatalogModel, get_catalog
from payments import MinAmountError, create_crypto_invoice
from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, all_button_labels, t

log = logging.getLogger("bot.handlers")

router = Router()


class UserStates(StatesGroup):
    waiting_custom_amount = State()
    waiting_promo_code = State()


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
    balance = float(user["balance_usd"]) if user else 0.0
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
    kb.button(text=t(lang, "hub_btn_new_chat"), callback_data="hub_newchat")
    kb.button(text=t(lang, "hub_btn_support"), callback_data="hub_support")
    kb.button(text=t(lang, "hub_btn_language"), callback_data="hub_language")
    kb.adjust(2, 2, 1)
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
    ``edit_text`` raises a "message is not modified" error which we
    swallow.
    """
    text, kb = await _hub_text_and_kb(callback.from_user.id, lang)
    try:
        await callback.message.edit_text(
            text, reply_markup=kb.as_markup(), parse_mode="Markdown"
        )
    except Exception:
        # Telegram may raise TelegramBadRequest when the message is
        # already exactly the hub. Not user-facing.
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
    await db.create_user(message.from_user.id, message.from_user.username or "Unknown")
    lang = await _get_user_language(message.from_user.id)
    # Quick greeting first (one-shot bubble), then the hub. Greeting is
    # short and addressable so users feel acknowledged before the menu
    # appears. Also strips the legacy bottom keyboard from old clients.
    greeting = t(lang, "start_greeting", first_name=message.from_user.first_name or "")
    await message.answer(greeting, reply_markup=ReplyKeyboardRemove())
    await _send_hub(message, lang, remove_kb=False)


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


@router.callback_query(F.data == "hub_wallet")
async def hub_wallet_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    lang = await _get_user_language(user_id)
    user_data = await db.get_user(user_id)
    balance = float(user_data["balance_usd"]) if user_data else 0.0
    builder = _build_wallet_keyboard(lang)
    await callback.message.edit_text(
        t(lang, "wallet_text", balance=balance),
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
    try:
        await callback.message.edit_text(
            text, parse_mode="Markdown", reply_markup=builder.as_markup()
        )
    except Exception:
        log.debug("memory screen edit_text was a no-op", exc_info=True)


@router.callback_query(F.data == "hub_newchat")
async def hub_newchat_handler(callback: CallbackQuery, state: FSMContext):
    """Open the conversation-memory settings screen.

    The hub button is labelled "🆕 New Chat" because that's the most
    common operation users want here (start a fresh conversation),
    but the screen also exposes the memory toggle since the two
    concepts are tied — "new chat" only matters when memory is on.
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
        price_label = t(
            lang,
            "models_price_format",
            input=model.price.input_per_1m_usd,
            output=model.price.output_per_1m_usd,
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


def _build_wallet_keyboard(lang: str) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text=t(lang, "btn_add_crypto"), callback_data="add_crypto")
    _back_to_menu_button(builder, lang)
    builder.adjust(1, 1)
    return builder


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
    balance = float(user_data["balance_usd"]) if user_data else 0.0
    builder = _build_wallet_keyboard(lang)
    text = t(lang, "wallet_text", balance=balance)
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
    builder.button(text=t(lang, "btn_amt_10"), callback_data="amt_10")
    builder.button(text=t(lang, "btn_amt_25"), callback_data="amt_25")
    builder.button(text=t(lang, "btn_amt_50"), callback_data="amt_50")
    builder.button(text=t(lang, "btn_amt_custom"), callback_data="amt_custom")
    if banner:
        # Promo applied → offer removal in place of add.
        builder.button(text=t(lang, "btn_promo_remove"), callback_data="remove_promo")
    else:
        builder.button(text=t(lang, "btn_promo_enter"), callback_data="enter_promo")
    builder.button(text=t(lang, "btn_back_to_wallet"), callback_data="back_to_wallet")
    builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
    # Layout: 3 amount buttons | custom row | promo row | back+home.
    builder.adjust(3, 1, 1, 2)

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

    if amount < 10:
        await message.answer(t(lang, "charge_custom_min_error"))
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


@router.callback_query(F.data.startswith("cur_"))
async def process_custom_currency_selection(callback: CallbackQuery, state: FSMContext):
    currency = callback.data.split("_")[1]
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

    await state.clear()
    await callback.message.edit_text(t(lang, "charge_creating_invoice"))

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
        builder.button(text=t(lang, "btn_retry"), callback_data="add_crypto")
        builder.button(text=t(lang, "btn_home"), callback_data="close_menu")
        builder.adjust(2)
        if e.min_usd is not None:
            text = t(
                lang, "charge_min_amount_with_min",
                currency=e.currency.upper(), min_usd=e.min_usd,
            )
        else:
            text = t(
                lang, "charge_min_amount_unknown",
                currency=e.currency.upper(),
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
        if e.min_usd is not None:
            text = t(
                lang, "charge_min_amount_with_min",
                currency=e.currency.upper(), min_usd=e.min_usd,
            )
        else:
            text = t(
                lang, "charge_min_amount_unknown",
                currency=e.currency.upper(),
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

    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    reply = await chat_with_model(message.from_user.id, message.text)
    # Telegram caps a single message at 4096 characters. Long-form
    # AI replies (essay-style answers, code blocks, etc.) routinely
    # exceed that and were crashing the send with
    # ``TelegramBadRequest: message is too long``. Chunk on a
    # paragraph / line / hard boundary, in that order, so the split
    # falls on a natural break when possible.
    for chunk in _split_for_telegram(reply, _TELEGRAM_MAX_MSG_CHARS):
        await message.answer(chunk)
