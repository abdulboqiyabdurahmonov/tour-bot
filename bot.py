import os
import asyncio
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# === ENV ===
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")  # можешь переопределить в Render
PORT = int(os.getenv("PORT", 8080))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

# === Aiogram core ===
bot = Bot(token=TOKEN)
dp = Dispatcher()

# === Handlers ===
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("Привет! Я тур-бот 🤖\nПиши /tours чтобы увидеть примеры туров.")

@dp.message(Command("tours"))
async def tours_cmd(message: types.Message):
    # Заглушка — позже подменим на реальные данные из Collector
    tours = [
        "🇹🇷 Анталия — 500$ за 7 ночей",
        "🇹🇭 Пхукет — 800$ за 10 ночей",
        "🇪🇬 Шарм-эль-Шейх — 450$ за 7 ночей"
    ]
    await message.answer("\n".join(tours))

# === FastAPI app ===
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "tour-bot"}

@app.on_event("startup")
async def on_startup():
    # Формируем абсолютный URL для вебхука на Render
    # Render сам пробрасывает RENDER_EXTERNAL_URL
    base = os.getenv("RENDER_EXTERNAL_URL")
    if not base:
        # fallback: локально / на нестандартных платформах
        base = os.getenv("PUBLIC_URL", f"http://0.0.0.0:{PORT}")
    webhook_url = f"{base}{WEBHOOK_PATH}"
    # Переставим вебхук (перезапишет старый, это норм)
    await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
    print(f"✅ Webhook set: {webhook_url}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    # Проверяем секрет X-Telegram-Bot-Api-Secret-Token
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)
    data = await request.json()
    # Валидируем Update и отдаём его диспетчеру
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
