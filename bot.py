import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from fastapi import FastAPI
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = "supersecret"  # придумай строку сам
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- handlers ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("Привет! Я тур-бот 🤖\nПиши /tours чтобы увидеть примеры туров.")

@dp.message(Command("tours"))
async def tours_cmd(message: types.Message):
    # пока заглушка: выводим список из 3 туров
    tours = [
        "🇹🇷 Анталия — 500$ за 7 ночей",
        "🇹🇭 Пхукет — 800$ за 10 ночей",
        "🇪🇬 Шарм-эль-Шейх — 450$ за 7 ночей"
    ]
    await message.answer("\n".join(tours))

# --- fastapi for Render ---
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    # выставляем вебхук на Render URL
    render_url = os.getenv("RENDER_EXTERNAL_URL")  # Render сам задаёт
    webhook_url = f"{render_url}{WEBHOOK_PATH}"
    await bot.set_webhook(webhook_url, secret_token=WEBHOOK_SECRET)
    print(f"✅ Webhook set: {webhook_url}")

aio_app = web.Application()
SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(aio_app, path=WEBHOOK_PATH)
app.mount("/", web.AppRunner(aio_app))
