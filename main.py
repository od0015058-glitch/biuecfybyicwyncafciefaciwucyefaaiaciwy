import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiohttp import web
from dotenv import load_dotenv

from database import db
from handlers import router
from payments import payment_webhook

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bot.main")


async def start_webhook_server(bot: Bot) -> web.AppRunner:
    """Spins up a background web server to listen for payment IPNs."""
    app = web.Application()
    app["bot"] = bot  # Give the server access to the bot so it can send messages
    app.router.add_post("/nowpayments-webhook", payment_webhook)

    port = int(os.getenv("WEBHOOK_PORT", "8080"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Payment webhook listening on port %d", port)
    return runner


async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher()
    dp.include_router(router)

    await db.connect()
    log.info("Proxy bot is online.")

    runner = await start_webhook_server(bot)

    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
