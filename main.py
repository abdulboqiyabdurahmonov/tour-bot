import os
import logging
from fastapi import FastAPI, Request
from aiogram import types
from bot import dp, bot   # логика бота внутри bot.py

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "tourbotsecret")
WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"
RENDER_URL = os.getenv("RENDER_URL", "https://tour-bot.onrender.com")
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"✅ Webhook set to {WEBHOOK_URL}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logger.info("🛑 Webhook deleted")

@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def root():
    return {"status": "ok", "message": "Tour Bot is alive"}
