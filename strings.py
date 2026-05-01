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

**Runtime overrides (Stage-9-Step-1.6).** :func:`t` first consults an
in-memory ``_OVERRIDES`` cache keyed by ``(lang, key)``. Admins edit
this cache via the web admin's ``/admin/strings`` page; the
``database.bot_strings`` table is the persistent source. The cache is
seeded at boot from :func:`database.Database.load_all_string_overrides`
and refreshed after every successful admin write. Compiled defaults
in :data:`_STRINGS` are the fallback — overrides never *delete* a slug,
they just shadow it, so reverting an override resurrects the default.
"""

from __future__ import annotations

import logging
import string as _string_module

log = logging.getLogger("bot.strings")

DEFAULT_LANGUAGE = "fa"
SUPPORTED_LANGUAGES = ("fa", "en")

# Mapping: lang -> {key -> string}.
_STRINGS: dict[str, dict[str, str]] = {
    "fa": {
        # ---- Legacy reply-keyboard labels (kept so old clients with a
        # cached ReplyKeyboardMarkup at the bottom can still reach the
        # bot — those handlers now just re-render the inline hub and
        # send ReplyKeyboardRemove). New users never see these.
        "kbd_models": "🤖 مدل‌های هوش مصنوعی",
        "kbd_wallet": "💰 کیف پول",
        "kbd_support": "🎧 پشتیبانی",
        "kbd_language": "🌍 تغییر زبان",
        # ---- Generic nav buttons ----
        "btn_back": "🔙 بازگشت",
        "btn_home": "🏠 منوی اصلی",
        "btn_back_to_menu": "🏠 بازگشت به منو",
        "btn_back_to_wallet": "🔙 کیف پول",
        "btn_cancel": "❌ انصراف",
        "btn_close_menu": "❌ بستن منو",
        "btn_retry": "🔄 تلاش مجدد",
        # ---- Inline hub (single-message UI, /start renders this) ----
        "hub_title": (
            "🐱 **Meowassist**\n\n"
            "🤖 مدل فعال: `{active_model}`\n"
            "💰 موجودی: `${balance:.2f}`\n"
            "🌍 زبان: `{lang_label}`\n"
            "🧠 حافظه: {memory_label}\n\n"
            "از دکمه‌های زیر برای ادامه استفاده کنید:"
        ),
        "hub_btn_wallet": "💰 کیف پول",
        "hub_btn_models": "🤖 مدل‌های هوش مصنوعی",
        "hub_btn_new_chat": "🆕 گفتگوی جدید",
        "hub_btn_memory": "🧠 حافظه: {state}",
        "hub_btn_support": "💬 پشتیبانی",
        "hub_btn_language": "🌍 تغییر زبان",
        "hub_no_active_model": "—",
        "newchat_no_memory_hint": (
            "💡 حافظه خاموش است؛ دکمه «حافظه» در منوی اصلی را باز کنید تا فعال بشود."
        ),
        "hub_lang_label_fa": "🇮🇷 فارسی",
        "hub_lang_label_en": "🇬🇧 English",
        # ---- Memory toggle / new-chat screen (P3-5) ----
        "memory_state_on": "🟢 روشن",
        "memory_state_off": "⚪ خاموش",
        "memory_screen": (
            "🧠 **حافظه گفتگو**\n\n"
            "وضعیت فعلی: {state}\n\n"
            "وقتی حافظه روشن باشد، ربات پیام‌های قبلی شما را به‌خاطر می‌سپارد "
            "و می‌توانید گفتگوی چندمرحله‌ای داشته باشید. هزینه با طول گفتگو افزایش می‌یابد:\n"
            "• ۱۰ پیام ≈ ۵ برابر یک پیام مستقل\n"
            "• ۳۰ پیام ≈ ۱۵ برابر یک پیام مستقل\n\n"
            "هر زمان دکمه «گفتگوی جدید» را بزنید، حافظه پاک می‌شود (رایگان)."
        ),
        "btn_memory_enable": "🟢 فعال کردن حافظه",
        "btn_memory_disable": "⚪ خاموش کردن حافظه",
        "btn_memory_reset": "🆕 شروع گفتگوی جدید",
        "memory_toggled_on": "✅ حافظه فعال شد",
        "memory_toggled_off": "⚪ حافظه خاموش شد",
        "memory_reset_done": "🆕 گفتگو پاک شد ({count} پیام)",
        "memory_reset_empty": "💭 گفتگویی برای پاک کردن وجود ندارد.",
        # Stage-15-Step-E #1 (conversation history export — first slice).
        "btn_memory_export": "📥 دریافت تاریخچه گفتگو",
        "memory_export_empty": "📭 تاریخچه‌ای برای دریافت وجود ندارد.",
        "memory_export_caption": "📥 تاریخچه گفتگو ({count} پیام)",
        "memory_export_done": "✅ {count} پیام به صورت فایل ارسال شد",
        # ---- User spending analytics (Stage-15-Step-E #2 — first slice) ----
        "btn_my_stats": "📊 آمار مصرف من",
        "stats_title": "📊 **آمار مصرف شما**",
        "stats_balance_line": "💰 موجودی فعلی: *${balance:.2f}*",
        "stats_empty": (
            "_هنوز هیچ مصرفی ثبت نشده است._\n"
            "بعد از اولین گفتگو با مدل، آمار اینجا نمایش داده می‌شود."
        ),
        "stats_lifetime_header": "📈 *مجموع از ابتدا*",
        "stats_lifetime_line": (
            "  • تعداد درخواست: *{calls:,}*\n"
            "  • مجموع توکن: *{tokens:,}*\n"
            "  • هزینهٔ کل: *${cost:.4f}*"
        ),
        "stats_window_header": "🕒 *{days} روز اخیر*",
        "stats_window_line": (
            "  • تعداد درخواست: *{calls:,}*\n"
            "  • مجموع توکن: *{tokens:,}*\n"
            "  • هزینه: *${cost:.4f}*"
        ),
        "stats_top_models_header": "🔝 *مدل‌های پرکاربرد ({days} روز اخیر)*",
        "stats_top_models_line": (
            "  {rank}. `{model}` — {calls:,} درخواست، ${cost:.4f}"
        ),
        # Stage-15-Step-E #2 follow-up #3: per-day spending breakdown.
        # Rendered as ASCII bars in a fenced code block; the days
        # placeholder is the rolling window the bars cover.
        "stats_daily_header": "📅 *روند روزانه ({days} روز اخیر)*",
        # Stage-15-Step-E #2 follow-up: window selector buttons.
        # ``{days}روزه`` = "<days>-day" — short enough to keep the
        # four buttons on one row in a Telegram message.
        "stats_window_btn": "{days} روزه",
        # ---- Wallet redeem button (Stage-8-Part-3.5) ----
        "btn_redeem_gift": "🎁 استفاده از کد هدیه",
        "redeem_input_prompt": (
            "📥 لطفاً کد هدیه خود را ارسال کنید.\n"
            "برای انصراف دکمهٔ بازگشت را بزنید."
        ),
        # ---- Wallet receipts (Stage-12-Step-C) ----
        "btn_receipts": "🧾 رسیدهای اخیر",
        "receipts_title": "🧾 **رسیدهای اخیر**",
        "receipts_empty": (
            "📭 هنوز رسیدی ندارید.\n\n"
            "وقتی شارژ موفق ثبت شود اینجا نمایش داده می‌شود."
        ),
        "receipts_status_success": "✅",
        "receipts_status_partial": "⚠️",
        "receipts_status_refunded": "🔄",
        "btn_receipts_more": "⏬ موارد بیشتر",
        # ---- /start ----
        "start_greeting": (
            "سلام {first_name}!\n\n"
            "به دروازه هوش مصنوعی ما خوش آمدید."
        ),
        # ---- Required-channel gate (Stage-13-Step-A) ----
        "force_join_text": (
            "📢 **برای استفاده از ربات ابتدا باید عضو کانال ما شوید.**\n\n"
            "لطفاً کانال {channel} را عضو شوید و سپس روی دکمهٔ \"عضو شدم\" بزنید."
        ),
        "force_join_not_yet": (
            "❌ هنوز عضو کانال نشده‌اید.\n\n"
            "لطفاً ابتدا کانال {channel} را عضو شوید و سپس دوباره دکمهٔ \"عضو شدم\" را بزنید."
        ),
        "btn_force_join_join": "📢 عضویت در کانال",
        "btn_force_join_check": "✅ عضو شدم",
        # ---- Referral codes (Stage-13-Step-C) ----
        "btn_invite_friend": "🎁 دعوت از دوستان",
        "invite_text": (
            "🎁 **دعوت از دوستان**\n\n"
            "هر کسی که با کد دعوت شما ربات را شروع کند و اولین شارژ موفق را انجام دهد،\n"
            "هم شما و هم خودش هرکدام **{bonus_percent}٪ از مبلغ شارژ (تا سقف ${bonus_max:.2f})** هدیه می‌گیرید!\n\n"
            "🔗 لینک اختصاصی شما:\n"
            "`{share_url}`\n\n"
            "🆔 یا کد دعوت شما:\n"
            "`{code}`\n\n"
            "📊 آمار شما:\n"
            "• در انتظار شارژ: {pending}\n"
            "• تکمیل شده: {paid}\n"
            "• پاداش دریافتی: ${total_bonus:.2f}"
        ),
        "invite_text_no_link": (
            "🎁 **دعوت از دوستان**\n\n"
            "هر کسی که با کد دعوت شما ربات را شروع کند و اولین شارژ موفق را انجام دهد،\n"
            "هم شما و هم خودش هرکدام **{bonus_percent}٪ از مبلغ شارژ (تا سقف ${bonus_max:.2f})** هدیه می‌گیرید!\n\n"
            "🆔 کد دعوت شما:\n"
            "`{code}`\n\n"
            "دوست شما باید این پیام را به ربات بفرستد:\n"
            "`/start ref_{code}`\n\n"
            "📊 آمار شما:\n"
            "• در انتظار شارژ: {pending}\n"
            "• تکمیل شده: {paid}\n"
            "• پاداش دریافتی: ${total_bonus:.2f}"
        ),
        "referral_claim_ok": (
            "🎉 از طریق دعوت یک دوست به ربات وارد شدید!\n"
            "بعد از اولین شارژ موفق، هر دو نفر پاداش می‌گیرید."
        ),
        "referral_claim_self": (
            "ℹ️ نمی‌توانید از کد دعوت خودتان استفاده کنید."
        ),
        "referral_claim_already": (
            "ℹ️ قبلاً با یک کد دعوت ثبت‌نام کرده‌اید."
        ),
        "referral_claim_unknown": (
            "ℹ️ کد دعوت معتبر نیست. به ربات خوش آمدید!"
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
        # ---- Model picker ----
        "models_picker_title": (
            "🤖 **انتخاب مدل هوش مصنوعی**\n\n"
            "مدل فعلی: `{active_model}`\n\n"
            "ابتدا یک ارائه‌دهنده را انتخاب کنید:"
        ),
        "models_picker_empty": "❌ لیست مدل‌ها در دسترس نیست. لطفاً بعداً تلاش کنید.",
        "models_provider_title": (
            "🤖 ارائه‌دهنده: **{provider}**\n\n"
            "مدل فعلی: `{active_model}`\n\n"
            "💡 قیمت‌ها شامل کارمزد سرویس هستند و دقیقاً همان مبلغی است که از کیف پول شما کسر می‌شود.\n\n"
            "صفحه {page} از {total_pages}"
        ),
        "models_price_format": "${input:.2f} / ${output:.2f} per 1M",
        "models_set_success": (
            "✅ مدل فعال شما به `{model_id}` تغییر یافت."
        ),
        "models_set_unknown": "❌ این مدل در فهرست یافت نشد.",
        "models_set_disabled": "❌ این مدل توسط ادمین غیرفعال شده است. لطفاً مدل دیگری انتخاب کنید.",
        "models_offline_warning": (
            "⚠️ ارتباط با OpenRouter برقرار نشد؛ از فهرست محلی استفاده می‌شود."
        ),
        "btn_models_prev_page": "◀️ قبلی",
        "btn_models_next_page": "بعدی ▶️",
        "btn_models_others": "🌐 سایر شرکت‌ها",
        "btn_models_free": "🆓 مدل‌های رایگان",
        "models_others_title": (
            "🌐 **سایر شرکت‌ها**\n\n"
            "ارائه‌دهندگان دیگر مدل‌های متنی را انتخاب کنید.\n"
            "صفحه {page} از {total_pages}"
        ),
        "models_free_title": (
            "🆓 **مدل‌های رایگان**\n\n"
            "این مدل‌ها بدون هزینه پاسخ می‌دهند ولی ارائه‌دهنده بالادستی ممکن است درخواست‌ها را محدود کند.\n"
            "مدل فعلی: `{active_model}`\n"
            "صفحه {page} از {total_pages}"
        ),
        # ---- Wallet ----
        # Stage-11-Step-D: ``{toman_line}`` is an optional pre-newlined
        # annotation supplied by ``wallet_display.format_toman_annotation``.
        # Empty when no FX rate is cached, ``\n≈ N تومان`` when it is, or
        # ``\n≈ N تومان (نرخ تقریبی)`` when the cached rate is stale.
        # The wallet is still denominated in USD — Toman is display-only.
        "wallet_text": (
            "👛 **کیف پول شما**\n\n"
            "موجودی فعلی: ${balance:.2f}{toman_line}\n\n"
            "برای شارژ حساب کلیک کنید:"
        ),
        "wallet_toman_line": "≈ {toman:,.0f} تومان",
        "wallet_toman_line_stale": "≈ {toman:,.0f} تومان (نرخ تقریبی)",
        "btn_add_crypto": "➕ شارژ حساب (Crypto)",
        # ---- Charge wallet flow ----
        "charge_pick_amount": (
            "💰 مبلغ شارژ را انتخاب کنید:\n\n"
            "💡 حداقل مبلغ: $2\n"
            "⚠️ برخی ارزها به دلیل کارمزد شبکه حداقل بالاتری دارند."
        ),
        "btn_amt_5": "💵 $5",
        "btn_amt_10": "💵 $10",
        "btn_amt_25": "💵 $25",
        "btn_amt_custom": "✏️ مبلغ دلخواه (دلار)",
        "btn_amt_toman": "✏️ مبلغ دلخواه (تومان)",
        "charge_custom_prompt": (
            "✏️ مبلغ دلخواه خود را به دلار وارد کنید:\n\n"
            "💡 حداقل: $2\n"
            "💡 مثال: 15 یا 25.5"
        ),
        "charge_toman_prompt": (
            "✏️ مبلغ مورد نظر خود را به تومان وارد کنید:\n\n"
            "💡 نرخ فعلی: هر دلار ≈ {rate_toman:,.0f} تومان\n"
            "💡 حداقل معادل: $2 (≈ {min_toman:,.0f} تومان)\n"
            "💡 مثال: 400000 یا ۴۰۰٬۰۰۰"
        ),
        "charge_toman_no_rate": (
            "⚠️ در حال حاضر نرخ زنده دلار به تومان در دسترس نیست.\n"
            "لطفاً چند دقیقه بعد دوباره تلاش کنید یا مبلغ را به دلار وارد کنید."
        ),
        "charge_custom_min_error": "❌ حداقل مبلغ $2 است.",
        "charge_toman_min_error": (
            "❌ حداقل معادل $2 است (≈ {min_toman:,.0f} تومان). "
            "شما وارد کردید: {entered_toman:,.0f} تومان."
        ),
        "charge_custom_invalid": "❌ لطفاً یک عدد معتبر وارد کنید (مثال: 15 یا 20.5)",
        "charge_toman_invalid": (
            "❌ لطفاً یک عدد معتبر وارد کنید (مثال: 400000 یا ۴۰۰٬۰۰۰)."
        ),
        "charge_custom_amount_saved": (
            "💵 مبلغ ${amount:.2f} ثبت شد.\n\n🪙 ارز مورد نظر را انتخاب کنید:"
        ),
        "charge_toman_amount_saved": (
            "💵 مبلغ {entered_toman:,.0f} تومان ≈ ${amount:.2f} ثبت شد.\n"
            "(نرخ تبدیل: هر دلار ≈ {rate_toman:,.0f} تومان)\n\n"
            "🪙 روش پرداخت را انتخاب کنید:"
        ),
        "charge_pick_currency": "💰 مبلغ: **${amount}**\n\n🪙 ارز خود را انتخاب کنید:",
        "charge_amount_lost": "❌ مبلغ یافت نشد. دوباره تلاش کنید.",
        "charge_creating_invoice": "⏳ در حال ارتباط با درگاه ناوپیمنتس. لطفا صبر کنید...",
        # Stage-11-Step-C: TetraPay (Rial card / Shaparak) gateway.
        "tetrapay_button": "💳 پرداخت با کارت ایرانی",
        "tetrapay_creating_order": "⏳ در حال ارتباط با درگاه پرداخت ایرانی. لطفا صبر کنید...",
        "tetrapay_order_text": (
            "🧾 **پرداخت با کارت ایرانی**\n\n"
            "مبلغ: `{amount_irr:,} ریال` (≈ ${amount_usd:.2f})\n"
            "نرخ تبدیل قفل‌شده: هر دلار ≈ {rate_toman:,.0f} تومان\n\n"
            "روی دکمه زیر بزنید تا به درگاه پرداخت هدایت شوید.\n"
            "پس از پرداخت، حساب شما به طور خودکار شارژ می‌شود.\n\n"
            "⚠️ این لینک حدود ۲۰ دقیقه اعتبار دارد."
        ),
        "tetrapay_pay_button": "💳 رفتن به درگاه پرداخت",
        "tetrapay_unreachable": (
            "❌ درگاه پرداخت ایرانی پاسخگو نیست. "
            "لطفاً چند دقیقه بعد دوباره تلاش کنید یا از ارز دیجیتال استفاده کنید."
        ),
        "tetrapay_credit_notification": (
            "✅ پرداخت شما با موفقیت تأیید شد.\n"
            "**${amount:.2f}** به کیف پول شما اضافه گردید."
        ),
        # Stage-15-Step-E #8 follow-up #1: Zarinpal Telegram FSM
        # mirrors the TetraPay strings 1:1 so the two card-gateway
        # surfaces have identical UX vocabulary; only the gateway
        # name in the user-visible copy changes.
        "zarinpal_button": "💳 پرداخت با زرین‌پال",
        "zarinpal_creating_order": "⏳ در حال ارتباط با درگاه زرین‌پال. لطفا صبر کنید...",
        "zarinpal_order_text": (
            "🧾 **پرداخت با زرین‌پال**\n\n"
            "مبلغ: `{amount_irr:,} ریال` (≈ ${amount_usd:.2f})\n"
            "نرخ تبدیل قفل‌شده: هر دلار ≈ {rate_toman:,.0f} تومان\n\n"
            "روی دکمه زیر بزنید تا به درگاه زرین‌پال هدایت شوید.\n"
            "پس از پرداخت، حساب شما به طور خودکار شارژ می‌شود.\n\n"
            "⚠️ این لینک حدود ۲۰ دقیقه اعتبار دارد."
        ),
        "zarinpal_pay_button": "💳 رفتن به درگاه زرین‌پال",
        "zarinpal_unreachable": (
            "❌ درگاه زرین‌پال پاسخگو نیست. "
            "لطفاً چند دقیقه بعد دوباره تلاش کنید یا از روش پرداخت دیگری استفاده کنید."
        ),
        "zarinpal_credit_notification": (
            "✅ پرداخت زرین‌پال شما با موفقیت تأیید شد.\n"
            "**${amount:.2f}** به کیف پول شما اضافه گردید."
        ),
        "charge_invoice_text": (
            "🧾 **فاکتور شارژ حساب**\n\n"
            "مبلغ شارژ خالص: `${amount}`\n"
            "ارز انتخابی: `{currency}`\n\n"
            "مبلغ قابل پرداخت (با احتساب کارمزد شبکه و درگاه):\n"
            "**`{pay_amount}`**\n\n"
            "آدرس واریز:\n`{pay_address}`\n\n"
            "⚠️ لطفاً دقیقاً همین مبلغ را واریز کنید.\n"
            "🕒 برای دریافت با نرخ بالا تا ۶۰ دقیقه فرصت دارید. پس از آن پرداخت تا ۷ روز با نرخ لحظه‌ای دنبال می‌شود. بعد از ۷ روز باید فاکتور جدید بسازید."
        ),
        "charge_gateway_unreachable": "❌ درگاه پاسخگو نیست.",
        "charge_gateway_unreachable_long": (
            "❌ درگاه در حال حاضر پاسخگو نیست. "
            "تنظیمات پنل ناوپیمنتس را چک کنید."
        ),
        "charge_invoice_error": "❌ ایجاد فاکتور با خطا مواجه شد. لطفاً دوباره تلاش کنید.",
        "charge_min_amount_with_min": (
            "❌ حداقل مبلغ قابل پرداخت برای {currency} برابر است با ${min_usd:.2f}.\n"
            "لطفاً مبلغ بیشتری وارد کنید یا ارز دیگری انتخاب کنید."
        ),
        "charge_min_amount_with_min_and_alt": (
            "❌ حداقل مبلغ قابل پرداخت برای {currency} برابر است با ${min_usd:.2f}.\n"
            "💡 برای پرداخت ${amount_usd:.2f} می‌توانید از {alt_currency} استفاده کنید."
        ),
        "charge_min_amount_unknown": (
            "❌ مبلغ شما برای {currency} کمتر از حداقل قابل پرداخت است.\n"
            "لطفاً مبلغ بیشتری وارد کنید یا ارز دیگری (مثلاً USDT-TRC20) انتخاب کنید."
        ),
        # ---- AI engine error replies ----
        "ai_no_account": "❌ حساب کاربری شما یافت نشد. لطفا ابتدا ربات را /start کنید.",
        "ai_insufficient_balance": "⚠️ اعتبار شما کافی نیست. لطفا از منوی کیف پول، حساب خود را شارژ کنید.",
        "ai_model_disabled": "⚠️ مدل فعلی شما توسط ادمین غیرفعال شده است. لطفاً از منوی مدل‌ها یک مدل دیگر انتخاب کنید.",
        "gateway_disabled": "⚠️ این روش پرداخت در حال حاضر غیرفعال است.",
        "ai_provider_unavailable": "❌ سرور هوش مصنوعی موقتاً در دسترس نیست. لطفاً دوباره تلاش کنید.",
        "ai_rate_limited": (
            "⏳ این مدل در حال حاضر در سمت سرور با محدودیت نرخ مواجه شده است.\n"
            "لطفاً چند ثانیه صبر کنید یا مدل دیگری انتخاب کنید."
        ),
        "ai_rate_limited_free": (
            "⏳ این مدل *رایگان* الآن بیش از حد در حال استفاده است و توسط ارائه‌دهنده بالادستی محدود شده است.\n"
            "برای داشتن پاسخ بدون انتظار، یک مدل پولی را انتخاب کنید یا یک-دو دقیقه دیگر صبر کنید."
        ),
        "ai_transient_error": "❌ خطای ارتباطی موقت رخ داد. لطفاً چند لحظه دیگر دوباره تلاش کنید.",
        "ai_local_rate_limited": (
            "⏳ سرعت ارسال پیام‌های شما زیاد است. لطفاً یک لحظه صبر کنید."
        ),
        "ai_chat_busy": (
            "⏳ پاسخ پیام قبلی شما هنوز در حال پردازش است.\n"
            "لطفاً تا تکمیل آن صبر کنید و سپس پیام بعدی را بفرستید."
        ),
        # Stage-15-Step-E #10: vision (image) chat error replies.
        "ai_model_no_vision": (
            "🖼️ مدل فعلی شما از ارسال تصویر پشتیبانی نمی‌کند. "
            "برای پرسش درباره‌ی عکس، یک مدل بینایی (Vision) "
            "مثل GPT-4o، Claude 3 یا Gemini 1.5 از منوی مدل‌ها انتخاب کنید."
        ),
        "ai_image_oversize": (
            "🖼️ این تصویر برای پردازش بسیار بزرگ است. "
            "لطفاً تصویر کوچک‌تری بفرستید (حداکثر چند مگابایت)."
        ),
        "ai_image_unsupported_format": (
            "🖼️ فرمت این تصویر پشتیبانی نمی‌شود. "
            "لطفاً تصویر را با فرمت JPEG، PNG، GIF یا WEBP بفرستید."
        ),
        "ai_image_too_many": (
            "🖼️ تعداد تصاویر هر پیام بیشتر از حد مجاز است "
            "(حداکثر {max_images} عکس در هر پیام)."
        ),
        "ai_image_download_failed": (
            "🖼️ دریافت تصویر از تلگرام ممکن نشد. "
            "لطفاً دوباره تلاش کنید."
        ),
        "ai_image_document_instruction": (
            "🖼️ این تصویر به‌صورت «فایل» ارسال شده است و من نمی‌توانم آن را تحلیل کنم. "
            "لطفاً همان تصویر را به‌صورت «عکس» (در تلگرام هنگام ارسال، گزینهٔ «Photo» نه «File») "
            "دوباره بفرستید تا یک مدل بینایی بتواند آن را ببیند. "
            "فرمت‌های HEIC / HEIF (پیش‌فرض آیفون) پشتیبانی نمی‌شوند؛ "
            "تلگرام هنگام ارسال به‌صورت «Photo» تصویر را به JPEG تبدیل می‌کند."
        ),
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
        "pay_promo_bonus": (
            "🎁 کد تخفیف اعمال شد و مبلغ ${bonus:.4f} به‌عنوان هدیه به حساب شما اضافه شد."
        ),
        # ---- Promo codes ----
        "btn_promo_enter": "🎁 کد تخفیف",
        "btn_promo_remove": "❌ حذف کد تخفیف",
        "promo_prompt": (
            "🎁 کد تخفیف خود را وارد کنید:\n\n"
            "💡 کد به حروف کوچک/بزرگ حساس نیست."
        ),
        "promo_applied_percent": (
            "✅ کد تخفیف `{code}` ثبت شد ({percent}٪ هدیه روی شارژ).\n\n"
            "اکنون مبلغ شارژ را انتخاب کنید — مبلغ هدیه روی هر شارژ موفق محاسبه و به حساب شما اضافه می‌شود."
        ),
        "promo_applied_amount": (
            "✅ کد تخفیف `{code}` ثبت شد (${amount:.2f} هدیه روی شارژ).\n\n"
            "اکنون مبلغ شارژ را انتخاب کنید — مبلغ هدیه پس از تایید پرداخت به حساب شما اضافه می‌شود."
        ),
        "promo_active_banner_percent": (
            "🎁 کد فعلی: `{code}` ({percent}٪ هدیه)"
        ),
        "promo_active_banner_amount": (
            "🎁 کد فعلی: `{code}` (${amount:.2f} هدیه)"
        ),
        "promo_removed": "❌ کد تخفیف از این پرداخت برداشته شد.",
        "promo_invalid_unknown": "❌ این کد تخفیف پیدا نشد.",
        "promo_invalid_inactive": "❌ این کد تخفیف غیرفعال است.",
        "promo_invalid_expired": "❌ مهلت استفاده از این کد تخفیف به پایان رسیده.",
        "promo_invalid_exhausted": "❌ ظرفیت استفاده از این کد تخفیف تکمیل شده.",
        "promo_invalid_already_used": "❌ شما قبلاً از این کد تخفیف استفاده کرده‌اید.",

        # /redeem — gift codes (Stage-8-Part-3). Distinct from promo
        # codes: the "amount" is added directly to the wallet, no
        # purchase required.
        "redeem_usage": (
            "📥 برای استفاده از کد هدیه:\n`/redeem CODE`\n"
            "مثال: `/redeem WELCOME5`"
        ),
        "redeem_bad_code": "❌ فرمت کد نامعتبر است.",
        "redeem_ok": (
            "🎉 کد هدیه فعال شد! ${amount:.2f} به موجودی شما اضافه شد.\n"
            "موجودی فعلی: ${balance:.2f}"
        ),
        "redeem_not_found": "❌ این کد هدیه پیدا نشد.",
        "redeem_inactive": "❌ این کد هدیه غیرفعال است.",
        "redeem_expired": "❌ مهلت این کد هدیه به پایان رسیده.",
        "redeem_exhausted": "❌ ظرفیت استفاده از این کد هدیه تکمیل شده.",
        "redeem_already_redeemed": "❌ شما قبلاً از این کد هدیه استفاده کرده‌اید.",
        "redeem_user_unknown": (
            "❌ ابتدا /start را ارسال کنید و سپس دوباره تلاش کنید."
        ),
        "redeem_error": "❌ خطایی رخ داد. لطفاً بعداً تلاش کنید.",
    },
    "en": {
        # ---- Legacy reply-keyboard labels (kept so old clients with a
        # cached ReplyKeyboardMarkup at the bottom can still reach the
        # bot — those handlers now just re-render the inline hub and
        # send ReplyKeyboardRemove). New users never see these.
        "kbd_models": "🤖 AI Models",
        "kbd_wallet": "💰 Wallet",
        "kbd_support": "🎧 Support",
        "kbd_language": "🌍 Change language",
        # ---- Generic nav buttons ----
        "btn_back": "🔙 Back",
        "btn_home": "🏠 Main menu",
        "btn_back_to_menu": "🏠 Back to menu",
        "btn_back_to_wallet": "🔙 Wallet",
        "btn_cancel": "❌ Cancel",
        "btn_close_menu": "❌ Close",
        "btn_retry": "🔄 Try again",
        # ---- Inline hub (single-message UI, /start renders this) ----
        "hub_title": (
            "🐱 **Meowassist**\n\n"
            "🤖 Active model: `{active_model}`\n"
            "💰 Balance: `${balance:.2f}`\n"
            "🌍 Language: `{lang_label}`\n"
            "🧠 Memory: {memory_label}\n\n"
            "Tap a button to continue:"
        ),
        "hub_btn_wallet": "💰 Wallet",
        "hub_btn_models": "🤖 AI Models",
        "hub_btn_new_chat": "🆕 New Chat",
        "hub_btn_memory": "🧠 Memory: {state}",
        "hub_btn_support": "💬 Support",
        "hub_btn_language": "🌍 Change language",
        "hub_no_active_model": "—",
        "newchat_no_memory_hint": (
            "💡 Memory is OFF — turn it on from the Memory button on the main menu first."
        ),
        "hub_lang_label_fa": "🇮🇷 فارسی",
        "hub_lang_label_en": "🇬🇧 English",
        # ---- Memory toggle / new-chat screen (P3-5) ----
        "memory_state_on": "🟢 ON",
        "memory_state_off": "⚪ OFF",
        "memory_screen": (
            "🧠 **Conversation memory**\n\n"
            "Current state: {state}\n\n"
            "When memory is ON the bot remembers your previous messages so "
            "you can have a real multi-turn conversation. Cost grows with "
            "conversation length:\n"
            "• 10 turns ≈ 5× one independent message\n"
            "• 30 turns ≈ 15× one independent message\n\n"
            "Tap '🆕 New chat' any time to wipe memory and start fresh (free)."
        ),
        "btn_memory_enable": "🟢 Enable memory",
        "btn_memory_disable": "⚪ Disable memory",
        "btn_memory_reset": "🆕 Start new chat",
        "memory_toggled_on": "✅ Memory enabled",
        "memory_toggled_off": "⚪ Memory disabled",
        "memory_reset_done": "🆕 Conversation cleared ({count} messages)",
        "memory_reset_empty": "💭 No conversation to clear.",
        # Stage-15-Step-E #1 (conversation history export — first slice).
        "btn_memory_export": "📥 Export conversation",
        "memory_export_empty": "📭 No conversation history to export yet.",
        "memory_export_caption": "📥 Conversation history ({count} messages)",
        "memory_export_done": "✅ Sent {count} messages as a file",
        # ---- User spending analytics (Stage-15-Step-E #2 — first slice) ----
        "btn_my_stats": "📊 My usage stats",
        "stats_title": "📊 **Your usage stats**",
        "stats_balance_line": "💰 Current balance: *${balance:.2f}*",
        "stats_empty": (
            "_No usage logged yet._\n"
            "Stats will show up here once you've chatted with a model."
        ),
        "stats_lifetime_header": "📈 *Lifetime totals*",
        "stats_lifetime_line": (
            "  • Calls: *{calls:,}*\n"
            "  • Tokens: *{tokens:,}*\n"
            "  • Spent: *${cost:.4f}*"
        ),
        "stats_window_header": "🕒 *Last {days} days*",
        "stats_window_line": (
            "  • Calls: *{calls:,}*\n"
            "  • Tokens: *{tokens:,}*\n"
            "  • Spent: *${cost:.4f}*"
        ),
        "stats_top_models_header": "🔝 *Top models (last {days} days)*",
        "stats_top_models_line": (
            "  {rank}. `{model}` — {calls:,} calls, ${cost:.4f}"
        ),
        # Stage-15-Step-E #2 follow-up #3: per-day spending breakdown.
        "stats_daily_header": "📅 *Daily breakdown (last {days} days)*",
        # Stage-15-Step-E #2 follow-up: window selector buttons.
        "stats_window_btn": "{days}d",
        # ---- Wallet redeem button (Stage-8-Part-3.5) ----
        "btn_redeem_gift": "🎁 Redeem gift code",
        "redeem_input_prompt": (
            "📥 Send your gift code now.\n"
            "Tap Back to cancel."
        ),
        # ---- Wallet receipts (Stage-12-Step-C) ----
        "btn_receipts": "🧾 Recent top-ups",
        "receipts_title": "🧾 **Recent top-ups**",
        "receipts_empty": (
            "📭 No top-ups yet.\n\n"
            "Successful charges will show up here."
        ),
        "receipts_status_success": "✅",
        "receipts_status_partial": "⚠️",
        "receipts_status_refunded": "🔄",
        "btn_receipts_more": "⏬ Show more",
        # ---- /start ----
        "start_greeting": (
            "Hi {first_name}!\n\n"
            "Welcome to our AI gateway."
        ),
        # ---- Required-channel gate (Stage-13-Step-A) ----
        "force_join_text": (
            "📢 **Please join our channel to use the bot.**\n\n"
            "Join {channel} and then tap \"I've joined\" below."
        ),
        "force_join_not_yet": (
            "❌ You're not a member of the channel yet.\n\n"
            "Please join {channel} first, then tap \"I've joined\" again."
        ),
        "btn_force_join_join": "📢 Join channel",
        "btn_force_join_check": "✅ I've joined",
        # ---- Referral codes (Stage-13-Step-C) ----
        "btn_invite_friend": "🎁 Invite a friend",
        "invite_text": (
            "🎁 **Invite a friend**\n\n"
            "When someone starts the bot with your invite code and completes their first paid top-up, "
            "**both of you get {bonus_percent}% of the top-up amount (up to ${bonus_max:.2f}) as a bonus!**\n\n"
            "🔗 Your share link:\n"
            "`{share_url}`\n\n"
            "🆔 Or your invite code:\n"
            "`{code}`\n\n"
            "📊 Your stats:\n"
            "• Awaiting top-up: {pending}\n"
            "• Completed: {paid}\n"
            "• Bonus earned: ${total_bonus:.2f}"
        ),
        "invite_text_no_link": (
            "🎁 **Invite a friend**\n\n"
            "When someone starts the bot with your invite code and completes their first paid top-up, "
            "**both of you get {bonus_percent}% of the top-up amount (up to ${bonus_max:.2f}) as a bonus!**\n\n"
            "🆔 Your invite code:\n"
            "`{code}`\n\n"
            "Your friend should send this to the bot:\n"
            "`/start ref_{code}`\n\n"
            "📊 Your stats:\n"
            "• Awaiting top-up: {pending}\n"
            "• Completed: {paid}\n"
            "• Bonus earned: ${total_bonus:.2f}"
        ),
        "referral_claim_ok": (
            "🎉 You arrived via a friend's invite!\n"
            "After your first paid top-up, both of you get a bonus."
        ),
        "referral_claim_self": (
            "ℹ️ You can't use your own invite code."
        ),
        "referral_claim_already": (
            "ℹ️ You've already signed up via an invite code."
        ),
        "referral_claim_unknown": (
            "ℹ️ That invite code isn't valid. Welcome to the bot!"
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
        # ---- Model picker ----
        "models_picker_title": (
            "🤖 **Pick an AI model**\n\n"
            "Currently active: `{active_model}`\n\n"
            "First, choose a provider:"
        ),
        "models_picker_empty": "❌ Model list unavailable. Please try again later.",
        "models_provider_title": (
            "🤖 Provider: **{provider}**\n\n"
            "Currently active: `{active_model}`\n\n"
            "💡 Prices shown include our service fee — this is exactly what gets deducted from your wallet.\n\n"
            "Page {page} of {total_pages}"
        ),
        "models_price_format": "${input:.2f} / ${output:.2f} per 1M",
        "models_set_success": (
            "✅ Active model switched to `{model_id}`."
        ),
        "models_set_unknown": "❌ This model is not in the catalog.",
        "models_set_disabled": "❌ This model has been disabled by the admin. Please choose another model.",
        "models_offline_warning": (
            "⚠️ Couldn't reach OpenRouter; showing the cached fallback list."
        ),
        "btn_models_prev_page": "◀️ Prev",
        "btn_models_next_page": "Next ▶️",
        "btn_models_others": "🌐 Others",
        "btn_models_free": "🆓 Free models",
        "models_free_title": (
            "🆓 **Free models**\n\n"
            "These models reply at no cost, but the upstream provider may rate-limit requests.\n"
            "Active model: `{active_model}`\n"
            "Page {page} of {total_pages}"
        ),
        "models_others_title": (
            "🌐 **Other providers**\n\n"
            "Pick a provider to see its text models.\n"
            "Page {page} of {total_pages}"
        ),
        # ---- Wallet ----
        # Stage-11-Step-D: see fa-locale comment above.
        "wallet_text": (
            "👛 **Your wallet**\n\n"
            "Current balance: ${balance:.2f}{toman_line}\n\n"
            "Tap below to top up:"
        ),
        "wallet_toman_line": "≈ {toman:,.0f} TMN",
        "wallet_toman_line_stale": "≈ {toman:,.0f} TMN (approx)",
        "btn_add_crypto": "➕ Top up (Crypto)",
        # ---- Charge wallet flow ----
        "charge_pick_amount": (
            "💰 Choose a top-up amount:\n\n"
            "💡 Minimum: $2\n"
            "⚠️ Some currencies have a higher minimum due to network fees."
        ),
        "btn_amt_5": "💵 $5",
        "btn_amt_10": "💵 $10",
        "btn_amt_25": "💵 $25",
        "btn_amt_custom": "✏️ Custom (USD)",
        "btn_amt_toman": "✏️ Custom (Toman)",
        "charge_custom_prompt": (
            "✏️ Enter your custom amount in USD:\n\n"
            "💡 Minimum: $2\n"
            "💡 Examples: 15 or 25.5"
        ),
        "charge_toman_prompt": (
            "✏️ Enter the amount you want to top up, in Toman:\n\n"
            "💡 Current rate: 1 USD ≈ {rate_toman:,.0f} TMN\n"
            "💡 Minimum: $2 (≈ {min_toman:,.0f} TMN)\n"
            "💡 Examples: 400000 or ۴۰۰٬۰۰۰"
        ),
        "charge_toman_no_rate": (
            "⚠️ The live USD→Toman rate is not available right now.\n"
            "Please try again in a few minutes, or enter the amount in USD instead."
        ),
        "charge_custom_min_error": "❌ Minimum amount is $2.",
        "charge_toman_min_error": (
            "❌ Minimum is the equivalent of $2 (≈ {min_toman:,.0f} TMN). "
            "You entered: {entered_toman:,.0f} TMN."
        ),
        "charge_custom_invalid": "❌ Please enter a valid number (example: 15 or 20.5).",
        "charge_toman_invalid": (
            "❌ Please enter a valid number (example: 400000 or ۴۰۰٬۰۰۰)."
        ),
        "charge_custom_amount_saved": (
            "💵 ${amount:.2f} saved.\n\n🪙 Pick your currency:"
        ),
        "charge_toman_amount_saved": (
            "💵 {entered_toman:,.0f} TMN ≈ ${amount:.2f} saved.\n"
            "(Conversion rate: 1 USD ≈ {rate_toman:,.0f} TMN)\n\n"
            "🪙 Pick a payment method:"
        ),
        "charge_pick_currency": "💰 Amount: **${amount}**\n\n🪙 Pick your currency:",
        "charge_amount_lost": "❌ Amount not found. Please try again.",
        "charge_creating_invoice": "⏳ Contacting NowPayments. Please wait...",
        # Stage-11-Step-C: TetraPay (Rial card / Shaparak) gateway.
        "tetrapay_button": "💳 Pay with Iranian card",
        "tetrapay_creating_order": "⏳ Contacting the Iranian card gateway. Please wait...",
        "tetrapay_order_text": (
            "🧾 **Pay with Iranian card**\n\n"
            "Amount: `{amount_irr:,} IRR` (≈ ${amount_usd:.2f})\n"
            "Locked rate: 1 USD ≈ {rate_toman:,.0f} TMN\n\n"
            "Tap the button below to be redirected to the payment gateway.\n"
            "Your wallet will be credited automatically after payment.\n\n"
            "⚠️ This link is valid for about 20 minutes."
        ),
        "tetrapay_pay_button": "💳 Go to payment gateway",
        "tetrapay_unreachable": (
            "❌ The Iranian card gateway is not responding. "
            "Please try again in a few minutes, or pay with crypto."
        ),
        "tetrapay_credit_notification": (
            "✅ Your payment has been confirmed.\n"
            "**${amount:.2f}** has been added to your wallet."
        ),
        # Stage-15-Step-E #8 follow-up #1: Zarinpal Telegram FSM.
        "zarinpal_button": "💳 Pay with Zarinpal",
        "zarinpal_creating_order": "⏳ Contacting the Zarinpal gateway. Please wait...",
        "zarinpal_order_text": (
            "🧾 **Pay with Zarinpal**\n\n"
            "Amount: `{amount_irr:,} IRR` (≈ ${amount_usd:.2f})\n"
            "Locked rate: 1 USD ≈ {rate_toman:,.0f} TMN\n\n"
            "Tap the button below to be redirected to Zarinpal.\n"
            "Your wallet will be credited automatically after payment.\n\n"
            "⚠️ This link is valid for about 20 minutes."
        ),
        "zarinpal_pay_button": "💳 Go to Zarinpal",
        "zarinpal_unreachable": (
            "❌ The Zarinpal gateway is not responding. "
            "Please try again in a few minutes, or use another payment method."
        ),
        "zarinpal_credit_notification": (
            "✅ Your Zarinpal payment has been confirmed.\n"
            "**${amount:.2f}** has been added to your wallet."
        ),
        "charge_invoice_text": (
            "🧾 **Top-up invoice**\n\n"
            "Net amount: `${amount}`\n"
            "Currency: `{currency}`\n\n"
            "Total payable (network + gateway fees included):\n"
            "**`{pay_amount}`**\n\n"
            "Send to:\n`{pay_address}`\n\n"
            "⚠️ Send EXACTLY this amount.\n"
            "🕒 The quoted rate is locked for ~60 minutes. After that the payment is still tracked at the live rate for up to 7 days. If you haven't paid by then, just create a new invoice."
        ),
        "charge_gateway_unreachable": "❌ Gateway is not responding.",
        "charge_gateway_unreachable_long": (
            "❌ Gateway is not responding right now. "
            "Check your NowPayments dashboard configuration."
        ),
        "charge_invoice_error": "❌ Invoice creation failed. Please try again.",
        "charge_min_amount_with_min": (
            "❌ The minimum payable amount for {currency} is ${min_usd:.2f}.\n"
            "Please pick a higher amount or a different currency."
        ),
        "charge_min_amount_with_min_and_alt": (
            "❌ The minimum payable amount for {currency} is ${min_usd:.2f}.\n"
            "💡 You can pay ${amount_usd:.2f} with {alt_currency} instead."
        ),
        "charge_min_amount_unknown": (
            "❌ Your amount is below the minimum payable for {currency}.\n"
            "Please try a higher amount, or pick a cheaper currency (e.g. USDT-TRC20)."
        ),
        # ---- AI engine error replies ----
        "ai_no_account": "❌ Your account was not found. Please /start the bot first.",
        "ai_insufficient_balance": "⚠️ Insufficient balance. Please top up from the wallet menu.",
        "ai_model_disabled": "⚠️ Your active model has been disabled by the admin. Please choose another model from the Models menu.",
        "gateway_disabled": "⚠️ This payment method is currently disabled.",
        "ai_provider_unavailable": "❌ AI provider is temporarily unavailable. Please try again.",
        "ai_rate_limited": (
            "⏳ This model is currently rate-limited upstream.\n"
            "Please wait a few seconds or pick a different model."
        ),
        "ai_rate_limited_free": (
            "⏳ This *free* model is being heavily used and the upstream provider is rate-limiting it.\n"
            "For an immediate reply, pick a paid model — or try again in a minute."
        ),
        "ai_transient_error": "❌ A temporary connectivity error occurred. Please try again in a moment.",
        "ai_local_rate_limited": (
            "⏳ You're sending messages too quickly. Please wait a moment."
        ),
        "ai_chat_busy": (
            "⏳ Your previous message is still being processed.\n"
            "Please wait for it to finish before sending another one."
        ),
        # Stage-15-Step-E #10: vision (image) chat error replies.
        "ai_model_no_vision": (
            "🖼️ Your active model doesn't support images. "
            "Pick a vision-capable model (e.g. GPT-4o, Claude 3, "
            "Gemini 1.5) from the Models menu and try again."
        ),
        "ai_image_oversize": (
            "🖼️ This image is too large to process. "
            "Please send a smaller image (a few megabytes max)."
        ),
        "ai_image_unsupported_format": (
            "🖼️ This image format isn't supported. "
            "Please send a JPEG, PNG, GIF or WEBP image."
        ),
        "ai_image_too_many": (
            "🖼️ Too many images in one message "
            "(max {max_images} per message)."
        ),
        "ai_image_download_failed": (
            "🖼️ Couldn't fetch your image from Telegram. "
            "Please try again."
        ),
        "ai_image_document_instruction": (
            "🖼️ You sent this image as a file attachment, which I can't analyze. "
            "Please re-send the same image as a photo (in Telegram's attach menu pick "
            "\"Photo\", not \"File\") so a vision-capable model can see it. "
            "HEIC / HEIF (the iPhone default) isn't supported either; sending as a "
            "Photo automatically converts the image to JPEG, which is."
        ),
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
        "pay_promo_bonus": (
            "🎁 Promo applied: ${bonus:.4f} bonus credited to your wallet."
        ),
        # ---- Promo codes ----
        "btn_promo_enter": "🎁 Promo code",
        "btn_promo_remove": "❌ Remove promo",
        "promo_prompt": (
            "🎁 Enter your promo code:\n\n"
            "💡 Codes are not case-sensitive."
        ),
        "promo_applied_percent": (
            "✅ Promo `{code}` applied ({percent}% bonus).\n\n"
            "Now pick the top-up amount — the bonus is credited on top of every successful payment."
        ),
        "promo_applied_amount": (
            "✅ Promo `{code}` applied (${amount:.2f} bonus).\n\n"
            "Now pick the top-up amount — the bonus is credited once payment confirms."
        ),
        "promo_active_banner_percent": (
            "🎁 Promo: `{code}` ({percent}% bonus)"
        ),
        "promo_active_banner_amount": (
            "🎁 Promo: `{code}` (${amount:.2f} bonus)"
        ),
        "promo_removed": "❌ Promo code removed from this top-up.",
        "promo_invalid_unknown": "❌ Unknown promo code.",
        "promo_invalid_inactive": "❌ This promo code is inactive.",
        "promo_invalid_expired": "❌ This promo code has expired.",
        "promo_invalid_exhausted": "❌ This promo code has been fully used up.",
        "promo_invalid_already_used": "❌ You've already used this promo code.",

        # /redeem — gift codes (Stage-8-Part-3).
        "redeem_usage": (
            "📥 To use a gift code:\n`/redeem CODE`\n"
            "Example: `/redeem WELCOME5`"
        ),
        "redeem_bad_code": "❌ Invalid code format.",
        "redeem_ok": (
            "🎉 Gift code redeemed! ${amount:.2f} was added to your wallet.\n"
            "Current balance: ${balance:.2f}"
        ),
        "redeem_not_found": "❌ Gift code not found.",
        "redeem_inactive": "❌ This gift code has been revoked.",
        "redeem_expired": "❌ This gift code has expired.",
        "redeem_exhausted": "❌ This gift code has been fully redeemed.",
        "redeem_already_redeemed": (
            "❌ You've already redeemed this gift code."
        ),
        "redeem_user_unknown": (
            "❌ Please send /start first, then try again."
        ),
        "redeem_error": "❌ Something went wrong. Please try again later.",
    },
}


# Admin-edited per-(lang, key) overrides. Replaced wholesale by
# :func:`set_overrides` at boot and after each successful admin write
# in ``web_admin``. Empty dict = "every slug serves the compiled
# default", which is the behaviour pre-Stage-9-Step-1.6.
_OVERRIDES: dict[tuple[str, str], str] = {}

# Suppression set for the missing-key warning. Without this every
# ``t()`` call for a missing slug would re-emit the warning on every
# turn — instant log spam. We log once per (lang, key) per process and
# then go silent for that slug. Ops only needs to see drift once to
# act on it.
_MISSING_KEY_WARNED: set[tuple[str, str]] = set()


def set_overrides(overrides: dict[tuple[str, str], str]) -> None:
    """Replace the in-memory override cache.

    Called at boot from ``main.py`` once :func:`database.Database.connect`
    has populated the pool, and again after each successful write in the
    ``/admin/strings`` web handlers. Pass an empty dict to revert to the
    compiled defaults (used in tests).
    """
    global _OVERRIDES
    _OVERRIDES = dict(overrides)


def get_override(lang: str, key: str) -> str | None:
    """Return the override for *(lang, key)* or None. Used by the
    admin UI to show "current value vs compiled default" side by side."""
    return _OVERRIDES.get((lang, key))


def get_compiled_default(lang: str, key: str) -> str | None:
    """Return the compiled default text for *(lang, key)* without
    consulting the override cache. ``None`` if the slug doesn't exist
    in the requested locale."""
    if lang not in _STRINGS:
        return None
    return _STRINGS[lang].get(key)


_FORMATTER = _string_module.Formatter()


def extract_format_fields(template: str) -> set[str]:
    """Return the set of top-level ``str.format`` field names referenced by
    *template*.

    For example::

        "Balance: ${balance:.2f}, model={user.name}"  ->  {"balance", "user"}

    Only the **top-level** name is recorded — ``user.name`` and
    ``user[0]`` both contribute ``"user"`` because that's the kwarg the
    caller has to supply. Positional placeholders (``"{0}"``, ``"{}"``)
    are intentionally not modelled: every call site in this codebase
    uses keyword arguments, so a positional placeholder in an admin
    override is by definition broken.

    **Nested placeholders in the format spec are also extracted.**
    ``"{amount:.{precision}f}"`` returns ``{"amount", "precision"}`` —
    the spec itself is a format-string-fragment that ``str.format``
    resolves against ``**kwargs``, so a kwarg referenced *only* in
    the spec is just as required as one in the body. Pre-fix,
    :func:`validate_override` accepted such overrides and the runtime
    ``template.format(**kwargs)`` then raised ``KeyError`` for the
    nested kwarg, falling through to the bare-slug fallback so the
    operator's override silently never rendered.

    Raises :class:`ValueError` if *template* has invalid format
    syntax (unclosed brace, bare ``{``, ``}``, etc.).
    """
    fields: set[str] = set()
    for _literal, field_name, format_spec, _conversion in _FORMATTER.parse(
        template
    ):
        if field_name is None:
            # Pure-literal segment with no placeholder.
            continue
        # Strip attribute / index access — we only need the kwarg name.
        # ``user.name`` -> ``user``;  ``items[0]`` -> ``items``.
        head = field_name.split(".", 1)[0].split("[", 1)[0]
        if head == "" or head.isdigit():
            # Bare ``"{}"`` (auto-numbered) or indexed ``"{0}"`` /
            # ``"{1}"``. ``str.format(**kwargs)`` can't satisfy these
            # — caller passes only kwargs — so treat the template as
            # broken. Surface as ValueError so the validator can
            # produce a clear error message.
            raise ValueError(
                "positional placeholders ({} or {0}) are not allowed in "
                "string overrides; use named placeholders like {balance}."
            )
        fields.add(head)
        # Descend into the format spec — it's another format-string
        # fragment that ``str.format`` resolves against the same
        # kwargs. ``Formatter.parse`` reports the spec as a flat
        # string; recursively extracting from it picks up nested
        # references like ``{amount:.{precision}f}``. Empty / missing
        # specs short-circuit the recursion. Catch ValueError from
        # malformed nested syntax so the outer caller still sees the
        # original *template's* validation error rather than a
        # confusing inner-spec error.
        if format_spec:
            try:
                fields.update(extract_format_fields(format_spec))
            except ValueError:
                # Nested syntax is broken — let the outer template's
                # ``str.format`` raise the clean error at runtime
                # instead of swallowing it here. Returning the
                # already-collected fields keeps the validator's
                # "this slug accepts <X>" hint informative.
                pass
    return fields


def validate_override(
    lang: str, key: str, value: str
) -> str | None:
    """Validate that *value* is safe to use as the ``(lang, key)``
    override.

    Returns ``None`` on success, or a short error message describing
    why the override would break runtime ``t()`` calls. The web admin
    handler uses the message verbatim in a flash banner so admins can
    see what's wrong without trawling logs.

    Two failure modes:

    * **Invalid syntax.** Unbalanced braces, unrecognised conversion,
      etc. — :class:`ValueError` from :class:`string.Formatter.parse`.
      Pre-fix this got saved into ``_OVERRIDES`` and then *every*
      ``t()`` call for the slug raised, crashing the handler that
      tried to render it (e.g. the wallet view).
    * **Unknown placeholder.** ``"{bal}"`` when the compiled default
      uses ``"{balance}"``. ``str.format(balance=…)`` raises
      ``KeyError: 'bal'`` for the override, same crash mode.

    Both checks are run against the compiled default for the slug —
    we trust the compiled default's placeholder set and require the
    override to use a subset. Dropping placeholders entirely is fine
    (an override that ignores ``{balance}`` just renders without it).
    """
    default = get_compiled_default(lang, key)
    if default is None:
        return f"Unknown slug {lang}:{key}."
    try:
        default_fields = extract_format_fields(default)
    except ValueError:
        # The compiled default itself is malformed — that's a code
        # bug, not the admin's. Don't block the override on it; the
        # runtime fallback in :func:`t` will catch any subsequent
        # render failure.
        default_fields = set()
    try:
        override_fields = extract_format_fields(value)
    except ValueError as exc:
        return f"Invalid placeholder syntax: {exc}"
    extra = override_fields - default_fields
    if extra:
        allowed = (
            ", ".join(sorted(f"{{{f}}}" for f in default_fields))
            if default_fields
            else "(none — this slug takes no placeholders)"
        )
        return (
            f"Unknown placeholder(s) {sorted(extra)!r}. "
            f"Allowed for this slug: {allowed}"
        )
    return None


def iter_compiled_strings():
    """Yield ``(lang, key, default_value)`` for every compiled string.

    Used by the admin UI to enumerate every editable slug. The order
    is deterministic — sorted by lang then key — so the admin page is
    reproducible across reloads.
    """
    for lang in SUPPORTED_LANGUAGES:
        for key in sorted(_STRINGS[lang]):
            yield lang, key, _STRINGS[lang][key]


def t(lang: str | None, key: str, **kwargs: object) -> str:
    """Look up *key* in *lang* and ``str.format(**kwargs)`` it.

    Resolution order:

    1. ``_OVERRIDES[(lang, key)]`` — admin-set runtime override.
    2. ``i18n_runtime.gettext_lookup(lang, key)`` — community
       translation loaded from
       ``locale/<lang>/LC_MESSAGES/messages.po``. Returns ``None``
       when the runtime cache hasn't been initialised, the entry
       is missing, or ``msgstr`` is empty (gettext convention for
       "untranslated") so the caller falls through to (3).
       Stage-15-Step-E #7 follow-up #1 added this layer.
    3. ``_STRINGS[lang][key]`` — compiled default for the requested locale.
    4. ``_STRINGS[DEFAULT_LANGUAGE][key]`` — fallback to the default locale
       (with the same admin-override + ``.po`` precedence applied to
       the default locale).
    5. The bare slug itself, with a one-shot WARNING logged so dictionary
       drift surfaces in ops logs instead of silently shipping a slug to
       the user. Pre-Stage-9-Step-1.6 step (5) was a silent return.

    The ``.po`` layer (2) is opt-in: ``i18n_runtime.init_translations``
    must have been called for it to return anything (otherwise
    :func:`i18n_runtime.gettext_lookup` returns ``None`` and we
    fall straight through to ``_STRINGS``). Tests that don't care
    about the ``.po`` layer don't need to do anything special; the
    behaviour is identical to the pre-#7-#1 lookup chain.
    """
    # Late import keeps ``strings`` import-time cheap and avoids a
    # circular dependency: ``i18n_runtime`` imports ``strings``
    # (lazily, also at call time) for ``SUPPORTED_LANGUAGES``.
    import i18n_runtime

    if lang not in _STRINGS:
        lang = DEFAULT_LANGUAGE
    template = _OVERRIDES.get((lang, key))
    if template is None:
        template = i18n_runtime.gettext_lookup(lang, key)
    if template is None:
        template = _STRINGS[lang].get(key)
        if template is None and lang != DEFAULT_LANGUAGE:
            # Try the override cache for the default locale, then
            # the .po layer for the default locale, before falling
            # back to its compiled default — admin overrides should
            # win regardless of which locale we're rendering, and
            # the same applies to community translations.
            template = _OVERRIDES.get((DEFAULT_LANGUAGE, key))
            if template is None:
                template = i18n_runtime.gettext_lookup(
                    DEFAULT_LANGUAGE, key
                )
            if template is None:
                template = _STRINGS[DEFAULT_LANGUAGE].get(key)
    if template is None:
        # Bug fix bundled in this PR: pre-fix this branch silently
        # returned ``key`` so a typo'd slug or a translation gap was
        # invisible until a user copy-pasted the literal slug back.
        # Now we log once per (lang, key) per process so ops sees
        # the drift on the very next deploy.
        if (lang, key) not in _MISSING_KEY_WARNED:
            _MISSING_KEY_WARNED.add((lang, key))
            log.warning(
                "strings.t(): missing key %r in lang %r and default %r — "
                "returning the bare slug. Add the key to strings._STRINGS "
                "or set an override at /admin/strings.",
                key,
                lang,
                DEFAULT_LANGUAGE,
            )
        return key
    if kwargs:
        try:
            return template.format(**kwargs)
        except (KeyError, IndexError, ValueError) as exc:
            # Defensive runtime fallback for a broken admin override.
            # ``string_save_post`` validates new overrides via
            # :func:`validate_override`, but a legacy DB row (saved
            # before that validation existed) can still slip through;
            # without this guard, ``str.format`` would raise here and
            # bubble up as a poller-level crash, taking out the
            # handler that tried to render the slug.
            #
            # Strategy: if we're rendering an override (template !=
            # compiled default), retry with the compiled default,
            # which we trust to have correct placeholders.
            # Otherwise — or if the default also fails — surface the
            # bare slug, same fallback as the missing-key branch.
            default_template = (
                _STRINGS.get(lang, {}).get(key)
                or _STRINGS[DEFAULT_LANGUAGE].get(key)
            )
            if default_template is not None and default_template != template:
                try:
                    return default_template.format(**kwargs)
                except (KeyError, IndexError, ValueError):
                    pass
            log.warning(
                "strings.t(): format failed for key=%r lang=%r (%s); "
                "rendering bare slug. Fix the override at "
                "/admin/strings/%s/%s.",
                key, lang, exc, lang, key,
            )
            return key
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
    "extract_format_fields",
    "get_compiled_default",
    "get_override",
    "iter_compiled_strings",
    "set_overrides",
    "t",
    "validate_override",
]
