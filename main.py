import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
from dotenv import load_dotenv

import strings
from admin import parse_admin_user_ids, router as admin_router
from bot_commands import publish_bot_commands
from database import db
from handlers import router
from middlewares import UserUpsertMiddleware
from payments import payment_webhook
from rate_limit import install_webhook_rate_limit
from web_admin import setup_admin_routes

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bot.main")


async def start_webhook_server(bot: Bot) -> web.AppRunner:
    """Spins up a background web server to listen for payment IPNs."""
    app = web.Application()
    # Per-IP token-bucket middleware first so a flood can't even reach
    # the JSON parsing / signature verification step. NowPayments'
    # legitimate retry rhythm is well under the cap (30 tokens, 5/sec
    # refill); see rate_limit.install_webhook_rate_limit.
    install_webhook_rate_limit(app)
    app["bot"] = bot  # Give the server access to the bot so it can send messages
    app.router.add_post("/nowpayments-webhook", payment_webhook)

    # Mount the web admin panel under /admin/. Same aiohttp app, same
    # process — one less thing to deploy. Auth is HMAC-cookie based,
    # gated by ADMIN_PASSWORD + ADMIN_SESSION_SECRET. If either is
    # unset the admin panel is unreachable (login refuses all attempts
    # and logs a WARNING on boot).
    setup_admin_routes(
        app,
        db=db,
        password=os.getenv("ADMIN_PASSWORD", ""),
        session_secret=os.getenv("ADMIN_SESSION_SECRET", ""),
        ttl_hours=int(os.getenv("ADMIN_SESSION_TTL_HOURS", "24")),
        # Default ON so cookies are HTTPS-only. Set ADMIN_COOKIE_SECURE=0
        # locally if you're running over plain HTTP for development.
        cookie_secure=os.getenv("ADMIN_COOKIE_SECURE", "1") != "0",
        # Stage-8-Part-5: the broadcast page's background worker
        # needs a ``Bot`` to send Telegram messages through. The
        # webhook handler still reads ``app["bot"]`` directly, so we
        # leave that legacy stash in place above — this kwarg just
        # routes the same instance into the admin plumbing via a
        # typed AppKey.
        bot=bot,
        # Stage-9-Step-3: optional TOTP / 2FA. Empty secret keeps the
        # password-only login. ``setup_admin_routes`` validates the
        # base32 string at boot so a typo surfaces immediately
        # instead of failing at first login.
        totp_secret=os.getenv("ADMIN_2FA_SECRET", ""),
        totp_issuer=os.getenv("ADMIN_2FA_ISSUER", "Meowassist Admin"),
    )

    port = int(os.getenv("WEBHOOK_PORT", "8080"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Payment webhook listening on port %d", port)
    return runner


def build_fsm_storage() -> BaseStorage:
    """Pick the FSM backing store based on env.

    If ``REDIS_URL`` is set, store FSM state in Redis so a bot restart
    mid-checkout doesn't trap users in ``waiting_custom_amount`` /
    ``waiting_promo_code``. Otherwise fall back to in-memory storage
    and log a loud warning — fine for local dev, NOT for production.

    Importing ``RedisStorage`` lazily so the bot doesn't hard-depend
    on the ``redis`` package when a deployer chooses memory.
    """
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        log.warning(
            "REDIS_URL is not set — falling back to in-memory FSM storage. "
            "A bot restart will lose mid-checkout state. Set REDIS_URL "
            "(e.g. redis://redis:6379/0) for production."
        )
        return MemoryStorage()
    from aiogram.fsm.storage.redis import RedisStorage  # noqa: PLC0415

    log.info("Using Redis FSM storage at %s", redis_url)
    return RedisStorage.from_url(redis_url)


async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher(storage=build_fsm_storage())

    # Upsert ``users`` row before every handler runs so anything that
    # FK-references ``users.telegram_id`` (e.g. ``transactions``) never
    # hits ``transactions_telegram_id_fkey`` for a Telegram client that
    # tapped a button without re-sending /start.
    upsert = UserUpsertMiddleware()
    dp.message.outer_middleware(upsert)
    dp.callback_query.outer_middleware(upsert)

    # Per-user chat rate limiting is implemented INSIDE the AI catch-all
    # handler via ``rate_limit.consume_chat_token`` — not as a
    # dispatcher-wide middleware, because a middleware on dp.message
    # would also fire for /start, waiting_custom_amount input, promo
    # code input, and reply-keyboard handlers, none of which cost
    # OpenRouter money. See handlers.process_chat.

    # Admin commands first so /admin* never falls through to the public
    # router's catch-all chat handler. Non-admin callers see a silent
    # no-op (see admin.is_admin) and the message keeps propagating.
    dp.include_router(admin_router)
    dp.include_router(router)

    await db.connect()
    log.info("Proxy bot is online.")

    # Seed the in-memory string-override cache from the bot_strings
    # table. Admin edits via /admin/strings refresh the cache after
    # each save; this seed handles the boot case (process restart,
    # rolling deploy, etc.). See strings.set_overrides.
    try:
        overrides = await db.load_all_string_overrides()
        strings.set_overrides(overrides)
        log.info("loaded %d bot_strings overrides from DB", len(overrides))
    except Exception:
        # Don't take the bot down for an override-load failure —
        # the compiled defaults are a safe fallback. Surface loudly
        # so ops investigates (vs. silently shipping stale text).
        log.exception(
            "failed to load bot_strings overrides — using compiled defaults"
        )

    # Overwrite BotFather's cached slash-command list with the
    # canonical one. Without this, Telegram shows whatever was last
    # typed into the BotFather "Edit Commands" panel — including
    # leftover entries the bot has no handlers for. See bot_commands.
    admin_ids = parse_admin_user_ids(os.getenv("ADMIN_USER_IDS"))
    await publish_bot_commands(bot, admin_ids)

    runner = await start_webhook_server(bot)

    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
