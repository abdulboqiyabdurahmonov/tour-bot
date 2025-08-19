import os
import logging
from fastapi import FastAPI, Request
from aiogram import types
from bot import dp, bot   # <-- твой bot.py с логикой бота

# ================== CONFIG ==================
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "tourbotsecret")
RENDER_URL = os.getenv("RENDER_URL", "https://tour-bot.onrender.com")
WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FASTAPI ==================
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    # Удаляем старый вебхук (если был) и ставим новый
    await bot.delete_webhook()
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"✅ Webhook установлен: {WEBHOOK_URL}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logger.info("❌ Webhook удалён")

@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    """
    Обработка апдейтов от Telegram
    """
    try:
        data = await request.json()
        update = types.Update(**data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Ошибка обработки апдейта: {e}")
    return {"ok": True}

@app.get("/")
async def root():
    """
    Health-check эндпоинт для Render
    """
    return {
        "status": "ok",
        "message": "Tour Bot is alive 🚀",
        "webhook": WEBHOOK_URL
    }
