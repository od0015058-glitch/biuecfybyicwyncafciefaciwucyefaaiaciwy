import asyncio
import os
from aiogram import Bot, Dispatcher
from dotenv import load_dotenv
from aiohttp import web
from database import db
from handlers import router
from payments import payment_webhook

load_dotenv()

async def start_webhook_server(bot: Bot):
    """Spins up a background web server on port 8080 to listen for payments."""
    app = web.Application()
    app['bot'] = bot # Give the server access to the bot so it can send messages
    app.router.add_post('/nowpayments-webhook', payment_webhook)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print("🌐 Payment Webhook listening on port 8080...")

async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher()
    dp.include_router(router)

    await db.connect()
    print("🟢 Proxy Bot is online.")
    
    # Start the webhook server AND start listening to Telegram
    await start_webhook_server(bot)
    
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())