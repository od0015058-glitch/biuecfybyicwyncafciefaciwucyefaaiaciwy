"""Bilingual UI strings for the bot.

Every user-facing piece of text lives here, keyed by an ASCII slug. Look
up with :func:`t`. Two locales are supported today:

* ``fa`` — Persian (default)
* ``en`` — English

Adding a third locale: extend :data:`_STRINGS` with the new code, fill in
every key, and add it to :data:`SUPPORTED_LANGUAGES`. Missing keys will
fall back to ``fa`` rather than raising, so a partially-translated locale
is safe to ship for testing.

Format strings use Python ``str.format`` placeholders. Any caller passing
``**kwargs`` to :func:`t` will substitute them in.
"""

from __future__ import annotations

DEFAULT_LANGUAGE = "fa"
SUPPORTED_LANGUAGES = ("fa", "en")

# Mapping: lang -> {key -> string}.
_STRINGS: dict[str, dict[str, str]] = {
    "fa": {
        # ---- Top-level reply keyboard ----
        "kbd_models": "🤖 مدل‌های هوش مصنوعی",
        "kbd_wallet": "💰 کیف پول",
        "kbd_support": "🎧 پشتیبانی",
        "kbd_language": "🌍 تغییر زبان",
        # ---- Generic nav buttons ----
        "btn_back": "🔙 بازگشت",
        "btn_home": "🏠 منوی اصلی",
        "btn_back_to_wallet": "🔙 کیف پول",
        "btn_cancel": "❌ انصراف",
        "btn_close_menu": "❌ بستن منو",
        "btn_retry": "🔄 تلاش مجدد",
        # ---- /start ----
        "start_greeting": (
            "سلام {first_name}!\n\n"
            "به دروازه هوش مصنوعی ما خوش آمدید. "
            "برای شروع از منوی زیر استفاده کنید:"
        ),
        # ---- Support ----
        "support_text": (
            "🎧 **پشتیبانی فنی**\n\n"
            "برای ارتباط با ادمین به آیدی @Mahan\\_Admin پیام دهید."
        ),
        # ---- Language picker ----
        "language_picker_title": "🌍 زبان مورد نظر خود را انتخاب کنید:",
        "language_changed": "✅ زبان به فارسی تغییر یافت.",
        "btn_lang_fa": "🇮🇷 فارسی",
        "btn_lang_en": "🇬🇧 English",
        # ---- Models menu (placeholder until P2-4) ----
        "models_text": (
            "🤖 **مدل‌های هوش مصنوعی**\n\n"
            "✅ GPT-4o (فعال)\n"
            "⏳ Claude 3.5 (بزودی...)\n"
            "⏳ Gemini Pro (بزودی...)"
        ),
        # ---- Wallet ----
        "wallet_text": (
            "👛 **کیف پول شما**\n\n"
            "موجودی فعلی: ${balance:.2f}\n\n"
            "برای شارژ حساب کلیک کنید:"
        ),
        "btn_add_crypto": "➕ شارژ حساب (Crypto)",
        # ---- Charge wallet flow ----
        "charge_pick_amount": (
            "💰 مبلغ شارژ را انتخاب کنید:\n\n"
            "💡 حداقل مبلغ: $5"
        ),
        "btn_amt_5": "💵 $5",
        "btn_amt_10": "💵 $10",
        "btn_amt_20": "💵 $20",
        "btn_amt_custom": "✏️ مبلغ دلخواه",
        "charge_custom_prompt": (
            "✏️ مبلغ دلخواه خود را وارد کنید:\n\n"
            "💡 حداقل: $5\n"
            "💡 مثال: 15 یا 25.5"
        ),
        "charge_custom_min_error": "❌ حداقل مبلغ $5 است.",
        "charge_custom_invalid": "❌ لطفاً یک عدد معتبر وارد کنید (مثال: 15 یا 20.5)",
        "charge_custom_amount_saved": (
            "💵 مبلغ ${amount:.2f} ثبت شد.\n\n🪙 ارز مورد نظر را انتخاب کنید:"
        ),
        "charge_pick_currency": "💰 مبلغ: **${amount}**\n\n🪙 ارز خود را انتخاب کنید:",
        "charge_amount_lost": "❌ مبلغ یافت نشد. دوباره تلاش کنید.",
        "charge_creating_invoice": "⏳ در حال ارتباط با درگاه ناوپیمنتس. لطفا صبر کنید...",
        "charge_invoice_text": (
            "🧾 **فاکتور شارژ حساب**\n\n"
            "مبلغ شارژ خالص: `${amount}`\n"
            "ارز انتخابی: `{currency}`\n\n"
            "مبلغ قابل پرداخت (با احتساب کارمزد شبکه و درگاه):\n"
            "**`{pay_amount}`**\n\n"
            "آدرس واریز:\n`{pay_address}`\n\n"
            "⚠️ لطفاً دقیقاً همین مبلغ را واریز کنید. ربات منتظر تایید شبکه است."
        ),
        "charge_gateway_unreachable": "❌ درگاه پاسخگو نیست.",
        "charge_gateway_unreachable_long": (
            "❌ درگاه در حال حاضر پاسخگو نیست. "
            "تنظیمات پنل ناوپیمنتس را چک کنید."
        ),
        "charge_invoice_error": "❌ ایجاد فاکتور با خطا مواجه شد. لطفاً دوباره تلاش کنید.",
        # ---- AI engine error replies ----
        "ai_no_account": "❌ حساب کاربری شما یافت نشد. لطفا ابتدا ربات را /start کنید.",
        "ai_insufficient_balance": "⚠️ اعتبار شما کافی نیست. لطفا از منوی کیف پول، حساب خود را شارژ کنید.",
        "ai_provider_unavailable": "❌ سرور هوش مصنوعی موقتاً در دسترس نیست. لطفاً دوباره تلاش کنید.",
        "ai_transient_error": "❌ خطای ارتباطی موقت رخ داد. لطفاً چند لحظه دیگر دوباره تلاش کنید.",
        # ---- Payment notifications ----
        "pay_credited_full": "✅ پرداخت تایید شد! مبلغ ${delta:.4f} به حساب شما اضافه شد.",
        "pay_credited_total_only": (
            "✅ پرداخت شما تکمیل شد. "
            "در مجموع مبلغ ${total:.4f} به حساب شما اضافه شده است."
        ),
        "pay_partial": (
            "⚠️ پرداخت شما کمتر از مبلغ فاکتور بود. "
            "مبلغ ${delta:.4f} به حساب شما اضافه شد "
            "(مجموع شارژ این فاکتور: ${total:.4f}). "
            "اگر می‌خواهید مابقی را پرداخت کنید، می‌توانید همچنان به همان آدرس واریز کنید."
        ),
        "pay_expired_pending": (
            "⏰ مهلت پرداخت فاکتور شما به پایان رسید و وجهی دریافت نشد. "
            "اگر می‌خواهید شارژ کنید، لطفاً یک فاکتور جدید ایجاد کنید."
        ),
        "pay_expired_partial": (
            "⏰ مهلت پرداخت فاکتور شما به پایان رسید. "
            "مبلغ ${credited:.4f} که قبلاً پرداخت کرده بودید "
            "به حساب شما اضافه شده است. برای پرداخت مابقی، "
            "لطفاً یک فاکتور جدید ایجاد کنید."
        ),
        "pay_failed_pending": (
            "❌ پرداخت شما ناموفق بود. "
            "اگر مبلغی از حساب شما کسر شده است، با پشتیبانی تماس بگیرید."
        ),
        "pay_failed_partial": (
            "❌ پرداخت شما ناموفق اعلام شد. "
            "مبلغ ${credited:.4f} که قبلاً موفق پرداخت شده بود "
            "همچنان در حساب شما باقی می‌ماند. "
            "اگر مبلغ بیشتری از حساب شما کسر شده، با پشتیبانی تماس بگیرید."
        ),
        "pay_refunded_pending": "↩️ پرداخت شما بازگشت داده شد و به حساب شما اضافه نشد.",
        "pay_refunded_partial": (
            "↩️ پرداخت شما بازگشت داده شد. "
            "مبلغ ${credited:.4f} که قبلاً به حساب شما اضافه شده بود "
            "همچنان قابل استفاده است. اگر این طور نیست با پشتیبانی تماس بگیرید."
        ),
    },
    "en": {
        # ---- Top-level reply keyboard ----
        "kbd_models": "🤖 AI Models",
        "kbd_wallet": "💰 Wallet",
        "kbd_support": "🎧 Support",
        "kbd_language": "🌍 Change language",
        # ---- Generic nav buttons ----
        "btn_back": "🔙 Back",
        "btn_home": "🏠 Main menu",
        "btn_back_to_wallet": "🔙 Wallet",
        "btn_cancel": "❌ Cancel",
        "btn_close_menu": "❌ Close",
        "btn_retry": "🔄 Try again",
        # ---- /start ----
        "start_greeting": (
            "Hi {first_name}!\n\n"
            "Welcome to our AI gateway. "
            "Use the menu below to get started:"
        ),
        # ---- Support ----
        "support_text": (
            "🎧 **Technical support**\n\n"
            "Message @Mahan\\_Admin on Telegram to get in touch."
        ),
        # ---- Language picker ----
        "language_picker_title": "🌍 Choose your preferred language:",
        "language_changed": "✅ Language switched to English.",
        "btn_lang_fa": "🇮🇷 فارسی",
        "btn_lang_en": "🇬🇧 English",
        # ---- Models menu (placeholder until P2-4) ----
        "models_text": (
            "🤖 **AI models**\n\n"
            "✅ GPT-4o (active)\n"
            "⏳ Claude 3.5 (coming soon...)\n"
            "⏳ Gemini Pro (coming soon...)"
        ),
        # ---- Wallet ----
        "wallet_text": (
            "👛 **Your wallet**\n\n"
            "Current balance: ${balance:.2f}\n\n"
            "Tap below to top up:"
        ),
        "btn_add_crypto": "➕ Top up (Crypto)",
        # ---- Charge wallet flow ----
        "charge_pick_amount": (
            "💰 Choose a top-up amount:\n\n"
            "💡 Minimum: $5"
        ),
        "btn_amt_5": "💵 $5",
        "btn_amt_10": "💵 $10",
        "btn_amt_20": "💵 $20",
        "btn_amt_custom": "✏️ Custom amount",
        "charge_custom_prompt": (
            "✏️ Enter your custom amount:\n\n"
            "💡 Minimum: $5\n"
            "💡 Examples: 15 or 25.5"
        ),
        "charge_custom_min_error": "❌ Minimum amount is $5.",
        "charge_custom_invalid": "❌ Please enter a valid number (example: 15 or 20.5).",
        "charge_custom_amount_saved": (
            "💵 ${amount:.2f} saved.\n\n🪙 Pick your currency:"
        ),
        "charge_pick_currency": "💰 Amount: **${amount}**\n\n🪙 Pick your currency:",
        "charge_amount_lost": "❌ Amount not found. Please try again.",
        "charge_creating_invoice": "⏳ Contacting NowPayments. Please wait...",
        "charge_invoice_text": (
            "🧾 **Top-up invoice**\n\n"
            "Net amount: `${amount}`\n"
            "Currency: `{currency}`\n\n"
            "Total payable (network + gateway fees included):\n"
            "**`{pay_amount}`**\n\n"
            "Send to:\n`{pay_address}`\n\n"
            "⚠️ Send EXACTLY this amount. The bot is waiting for network confirmation."
        ),
        "charge_gateway_unreachable": "❌ Gateway is not responding.",
        "charge_gateway_unreachable_long": (
            "❌ Gateway is not responding right now. "
            "Check your NowPayments dashboard configuration."
        ),
        "charge_invoice_error": "❌ Invoice creation failed. Please try again.",
        # ---- AI engine error replies ----
        "ai_no_account": "❌ Your account was not found. Please /start the bot first.",
        "ai_insufficient_balance": "⚠️ Insufficient balance. Please top up from the wallet menu.",
        "ai_provider_unavailable": "❌ AI provider is temporarily unavailable. Please try again.",
        "ai_transient_error": "❌ A temporary connectivity error occurred. Please try again in a moment.",
        # ---- Payment notifications ----
        "pay_credited_full": "✅ Payment confirmed! ${delta:.4f} has been added to your wallet.",
        "pay_credited_total_only": (
            "✅ Your payment is complete. "
            "${total:.4f} has been added to your wallet from this invoice."
        ),
        "pay_partial": (
            "⚠️ You paid less than the invoice amount. "
            "${delta:.4f} has been credited to your wallet "
            "(total credited from this invoice: ${total:.4f}). "
            "You can still send the remainder to the same address if you wish."
        ),
        "pay_expired_pending": (
            "⏰ Your invoice has expired and no funds were received. "
            "Please create a new invoice if you'd like to top up."
        ),
        "pay_expired_partial": (
            "⏰ Your invoice has expired. "
            "The ${credited:.4f} you already paid has been credited to your wallet. "
            "Please create a new invoice if you'd like to pay the remainder."
        ),
        "pay_failed_pending": (
            "❌ Your payment failed. "
            "If money was deducted from your account, please contact support."
        ),
        "pay_failed_partial": (
            "❌ Your payment is reported as failed. "
            "The ${credited:.4f} that was already credited to your wallet stays. "
            "If more was deducted from your account, please contact support."
        ),
        "pay_refunded_pending": "↩️ Your payment was refunded; nothing was added to your wallet.",
        "pay_refunded_partial": (
            "↩️ Your payment was refunded. "
            "The ${credited:.4f} previously credited to your wallet is still usable. "
            "If that's not the case, please contact support."
        ),
    },
}


def t(lang: str | None, key: str, **kwargs: object) -> str:
    """Look up *key* in *lang* and ``str.format(**kwargs)`` it.

    Falls back to :data:`DEFAULT_LANGUAGE` if *lang* is unknown or the
    requested locale doesn't have the key. If the key is missing in both,
    returns the key itself (so a typo surfaces visibly during development
    rather than as a confusing ``KeyError``).
    """
    if lang not in _STRINGS:
        lang = DEFAULT_LANGUAGE
    template = _STRINGS[lang].get(key)
    if template is None and lang != DEFAULT_LANGUAGE:
        template = _STRINGS[DEFAULT_LANGUAGE].get(key)
    if template is None:
        return key
    if kwargs:
        return template.format(**kwargs)
    return template


def all_button_labels(key: str) -> tuple[str, ...]:
    """Return *key*'s value across every supported language.

    Used for ``aiogram`` ``F.text.in_(...)`` filters that need to match a
    main-keyboard button regardless of the user's current language. (We
    can't filter on the user's locale at dispatch time because the
    handler hasn't run yet.)
    """
    return tuple(_STRINGS[lang].get(key, "") for lang in SUPPORTED_LANGUAGES)


__all__ = [
    "DEFAULT_LANGUAGE",
    "SUPPORTED_LANGUAGES",
    "all_button_labels",
    "t",
]
