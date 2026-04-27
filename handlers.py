import logging

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import db
from payments import create_crypto_invoice
from ai_engine import chat_with_model

log = logging.getLogger("bot.handlers")

router = Router()

class UserStates(StatesGroup):
    waiting_custom_amount = State()


# Footer-row label constants for nested inline menus. Keeping these in one
# place lets us swap the wording or i18n-ify them later (P2-2) without
# touching every screen.
BTN_BACK = "🔙 بازگشت"
BTN_HOME = "🏠 منوی اصلی"
BTN_BACK_TO_WALLET = "🔙 کیف پول"
BTN_CANCEL = "❌ انصراف"


# ==========================================
# کیبورد اصلی (همیشه پایین صفحه)
# ==========================================
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 مدل‌های هوش مصنوعی"), KeyboardButton(text="💰 کیف پول")],
            [KeyboardButton(text="🎧 پشتیبانی"), KeyboardButton(text="🌍 تغییر زبان")]
        ],
        resize_keyboard=True,
        is_persistent=True
    )

# ==========================================
# COMMAND: /start
# ==========================================
@router.message(Command("start"))
async def cmd_start(message: Message):
    # ثبت کاربر در دیتابیس
    await db.create_user(message.from_user.id, message.from_user.username or "Unknown")
    
    text = (
        f"سلام {message.from_user.first_name}!\n\n"
        "به دروازه هوش مصنوعی ما خوش آمدید. برای شروع از منوی زیر استفاده کنید:"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

# ==========================================
# هندلرهای دکمه‌های اصلی (متنی زیر صفحه)
# ==========================================
@router.message(F.text == "🎧 پشتیبانی")
async def support_text_handler(message: Message):
    await message.answer(
        "🎧 **پشتیبانی فنی**\n\n"
        "برای ارتباط با ادمین به آیدی @Mahan\\_Admin پیام دهید.",
        parse_mode="Markdown",
    )

@router.message(F.text == "🌍 تغییر زبان")
async def language_text_handler(message: Message):
    await message.answer("🌍 این بخش به زودی فعال می‌شود. در حال حاضر فقط زبان فارسی پشتیبانی می‌شود.")

@router.message(F.text == "🤖 مدل‌های هوش مصنوعی")
async def models_text_handler(message: Message):
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ بستن منو", callback_data="close_menu")
    
    text = (
        "🤖 **مدل‌های هوش مصنوعی**\n\n"
        "✅ GPT-4o (فعال)\n"
        "⏳ Claude 3.5 (بزودی...)\n"
        "⏳ Gemini Pro (بزودی...)"
    )
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@router.message(F.text == "💰 کیف پول")
async def wallet_text_handler(message: Message):
    user_id = message.from_user.id
    user_data = await db.get_user(user_id)
    # جلوگیری از ارور در صورت ثبت نشدن یوزر
    balance = user_data['balance_usd'] if user_data else 0.0
    
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ شارژ حساب (Crypto)", callback_data="add_crypto")
    builder.button(text="❌ بستن منو", callback_data="close_menu")
    builder.adjust(1, 1)
    
    text = f"👛 **کیف پول شما**\n\nموجودی فعلی: ${balance:.2f}\n\nبرای شارژ حساب کلیک کنید:"
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# ==========================================
# هندلرهای دکمه‌های شیشه‌ای (ناوبری و بازگشت)
# ==========================================
@router.callback_query(F.data == "close_menu")
async def close_menu_handler(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@router.callback_query(F.data == "back_to_wallet")
async def back_to_wallet_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_data = await db.get_user(user_id)
    balance = user_data['balance_usd'] if user_data else 0.0
    
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ شارژ حساب (Crypto)", callback_data="add_crypto")
    builder.button(text="❌ بستن منو", callback_data="close_menu")
    builder.adjust(1, 1)
    
    text = f"👛 **کیف پول شما**\n\nموجودی فعلی: ${balance:.2f}\n\nبرای شارژ حساب کلیک کنید:"
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()

# ==========================================
# سیستم پرداخت حرفه ای (قیف ۳ مرحله ای)
# ==========================================

# مرحله ۱: انتخاب مبلغ
@router.callback_query(F.data == "add_crypto")
async def process_add_crypto_amount(callback: CallbackQuery, state: FSMContext):
    # This callback is also the "cancel" target of the custom-amount screen
    # (which puts the FSM in waiting_custom_amount). Clear any lingering state
    # so the user isn't stuck — otherwise their next free-text message would
    # be intercepted by process_custom_amount_input instead of process_chat.
    await state.clear()
    builder = InlineKeyboardBuilder()
    builder.button(text="💵 $5", callback_data="amt_5")
    builder.button(text="💵 $10", callback_data="amt_10")
    builder.button(text="💵 $20", callback_data="amt_20")
    builder.button(text="✏️ مبلغ دلخواه", callback_data="amt_custom")
    # Footer: back to wallet + home (close inline menu, the bottom reply
    # keyboard remains visible so the user is back at the top level).
    builder.button(text=BTN_BACK_TO_WALLET, callback_data="back_to_wallet")
    builder.button(text=BTN_HOME, callback_data="close_menu")
    builder.adjust(3, 1, 2)
    
    await callback.message.edit_text(
        "💰 مبلغ شارژ را انتخاب کنید:\n\n"
        "💡 حداقل مبلغ: $5",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# مرحله ۲: انتخاب مبلغ
# IMPORTANT: the specific "amt_custom" handler must be registered BEFORE the
# generic "amt_*" prefix handler. aiogram v3 dispatches the first matching
# handler, so registering the prefix first would swallow "amt_custom" and
# the custom-amount flow would be unreachable.
@router.callback_query(F.data == "amt_custom")
async def process_custom_amount_request(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.waiting_custom_amount)
    # بقیه کد...
    
    builder = InlineKeyboardBuilder()
    builder.button(text=BTN_CANCEL, callback_data="add_crypto")
    builder.button(text=BTN_HOME, callback_data="close_menu")
    builder.adjust(2)

    await callback.message.edit_text(
        "✏️ مبلغ دلخواه خود را وارد کنید:\n\n"
        "💡 حداقل: $5\n"
        "💡 مثال: 15 یا 25.5",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


# مرحله ۲: انتخاب ارز برای مبالغ ثابت ($5 / $10 / $20)
@router.callback_query(F.data.startswith("amt_"))
async def process_add_crypto_currency(callback: CallbackQuery):
    amount = callback.data.split("_")[1]

    builder = InlineKeyboardBuilder()
    builder.button(text="₿ Bitcoin", callback_data=f"pay_btc_{amount}")
    builder.button(text="Ξ Ethereum", callback_data=f"pay_eth_{amount}")
    builder.button(text="💎 TON", callback_data=f"pay_ton_{amount}")
    builder.button(text="💵 USDT (TRC20)", callback_data=f"pay_usdttrc20_{amount}")
    builder.button(text="💵 USDT (ERC20)", callback_data=f"pay_usdterc20_{amount}")
    builder.button(text="🔷 Litecoin", callback_data=f"pay_ltc_{amount}")
    builder.button(text=BTN_BACK, callback_data="add_crypto")
    builder.button(text=BTN_HOME, callback_data="close_menu")
    builder.adjust(2, 2, 2, 2)

    await callback.message.edit_text(
        f"💰 مبلغ: **${amount}**\n\n🪙 ارز خود را انتخاب کنید:",
        parse_mode="Markdown",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@router.message(UserStates.waiting_custom_amount)
async def process_custom_amount_input(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip().replace('$', ''))
        
        if amount < 5:
            await message.answer("❌ حداقل مبلغ $5 است.")
            return
        
        # پاک کردن state
        await state.clear()
        
        # نمایش منوی انتخاب ارز
        builder = InlineKeyboardBuilder()
        currencies = [
            ("Bitcoin (BTC)", "cur_btc"),
            ("Ethereum (ETH)", "cur_eth"),
            ("TON", "cur_ton"),
            ("USDT-TRC20", "cur_usdttrc20"),
            ("USDT-ERC20", "cur_usdterc20"),
            ("Litecoin (LTC)", "cur_ltc"),
        ]
        for name, callback_data in currencies:
            builder.button(text=name, callback_data=callback_data)
        # Footer: back to amount entry + home. Going back to amt_custom
        # re-prompts for the amount (clearing state) so the user can pick a
        # different value without restarting from the wallet.
        builder.button(text=BTN_BACK, callback_data="amt_custom")
        builder.button(text=BTN_HOME, callback_data="close_menu")
        builder.adjust(2, 2, 2, 2)

        # ذخیره مبلغ در state برای استفاده بعدی
        await state.update_data(custom_amount=amount)

        await message.answer(
            f"💵 مبلغ ${amount:.2f} ثبت شد.\n\n🪙 ارز مورد نظر را انتخاب کنید:",
            reply_markup=builder.as_markup()
        )
        
    except ValueError:
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید (مثال: 15 یا 20.5)")

@router.callback_query(F.data.startswith("cur_"))
async def process_custom_currency_selection(callback: CallbackQuery, state: FSMContext):
    currency = callback.data.split("_")[1]  # مثلاً btc
    
    # بازیابی مبلغ از state
    data = await state.get_data()
    amount = data.get('custom_amount')
    
    if not amount:
        await callback.answer("❌ مبلغ یافت نشد. دوباره تلاش کنید.", show_alert=True)
        return
    
    await state.clear()  # پاک کردن state
    
    # ارسال به handler فاکتور
    await callback.message.edit_text("⏳ در حال ارتباط با درگاه ناوپیمنتس...")
    
    try:
        invoice = await create_crypto_invoice(callback.from_user.id, amount_usd=float(amount), currency=currency)
        if invoice:
            pay_address = invoice.get('pay_address')
            pay_amount = invoice.get('pay_amount')
            
            builder = InlineKeyboardBuilder()
            builder.button(text=BTN_BACK_TO_WALLET, callback_data="back_to_wallet")
            builder.button(text=BTN_HOME, callback_data="close_menu")
            builder.adjust(2)

            text = (
                f"🧾 **فاکتور شارژ حساب**\n\n"
                f"مبلغ شارژ خالص: `${amount}`\n"
                f"ارز انتخابی: `{currency.upper()}`\n\n"
                f"مبلغ قابل پرداخت (با احتساب کارمزد):\n"
                f"**`{pay_amount}`**\n\n"
                f"آدرس واریز:\n`{pay_address}`\n\n"
                "⚠️ دقیقاً همین مبلغ را واریز کنید."
            )

            await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=builder.as_markup())
        else:
            builder = InlineKeyboardBuilder()
            builder.button(text="🔄 تلاش مجدد", callback_data="add_crypto")
            builder.button(text=BTN_HOME, callback_data="close_menu")
            builder.adjust(2)
            await callback.message.edit_text("❌ درگاه پاسخگو نیست.", reply_markup=builder.as_markup())
    except Exception:
        log.exception(
            "Failed to create custom-amount invoice for user %d",
            callback.from_user.id,
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 تلاش مجدد", callback_data="add_crypto")
        builder.button(text=BTN_HOME, callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            "❌ ایجاد فاکتور با خطا مواجه شد. لطفاً دوباره تلاش کنید.",
            reply_markup=builder.as_markup(),
        )
        
    await callback.answer()

# مرحله ۳: صدور فاکتور نهایی
@router.callback_query(F.data.startswith("pay_"))
async def process_final_invoice(callback: CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ دیتای نامعتبر!", show_alert=True)
        return
        
    currency = parts[1]
    amount = parts[2]
    
    await callback.message.edit_text("⏳ در حال ارتباط با درگاه ناوپیمنتس. لطفا صبر کنید...")
    
    try:
        invoice = await create_crypto_invoice(callback.from_user.id, amount_usd=float(amount), currency=currency)
        if invoice:
            pay_address = invoice.get('pay_address')
            pay_amount = invoice.get('pay_amount')
            
            builder = InlineKeyboardBuilder()
            builder.button(text=BTN_BACK_TO_WALLET, callback_data="back_to_wallet")
            builder.button(text=BTN_HOME, callback_data="close_menu")
            builder.adjust(2)

            text = (
                f"🧾 **فاکتور شارژ حساب**\n\n"
                f"مبلغ شارژ خالص: `${amount}`\n"
                f"ارز انتخابی: `{currency.upper()}`\n\n"
                f"مبلغ قابل پرداخت (با احتساب کارمزد شبکه و درگاه):\n"
                f"**`{pay_amount}`**\n\n"
                f"آدرس واریز:\n`{pay_address}`\n\n"
                "⚠️ لطفاً دقیقاً همین مبلغ را واریز کنید. ربات منتظر تایید شبکه است."
            )

            await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=builder.as_markup())
        else:
            builder = InlineKeyboardBuilder()
            builder.button(text="🔄 تلاش مجدد", callback_data="add_crypto")
            builder.button(text=BTN_HOME, callback_data="close_menu")
            builder.adjust(2)
            await callback.message.edit_text("❌ درگاه در حال حاضر پاسخگو نیست. تنظیمات پنل ناوپیمنتس را چک کنید.", reply_markup=builder.as_markup())
    except Exception:
        log.exception(
            "Failed to create invoice for user %d",
            callback.from_user.id,
        )
        builder = InlineKeyboardBuilder()
        builder.button(text=BTN_BACK_TO_WALLET, callback_data="back_to_wallet")
        builder.button(text=BTN_HOME, callback_data="close_menu")
        builder.adjust(2)
        await callback.message.edit_text(
            "❌ ایجاد فاکتور با خطا مواجه شد. لطفاً دوباره تلاش کنید.",
            reply_markup=builder.as_markup(),
        )
        
    await callback.answer()

# ==========================================
# چت با هوش مصنوعی (ارسال درخواست‌ها به موتور)
# ==========================================
@router.message(F.text & ~F.text.startswith('/'))
async def process_chat(message: Message):
    reserved_buttons = ["🤖 مدل‌های هوش مصنوعی", "💰 کیف پول", "🎧 پشتیبانی", "🌍 تغییر زبان"]
    if message.text in reserved_buttons:
        return
        
    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    reply = await chat_with_model(message.from_user.id, message.text)
    await message.answer(reply)
