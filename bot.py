import os
import asyncio
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# --- Config ---
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.getenv("PORT", 8080))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

# --- Aiogram ---
bot = Bot(token=TOKEN)
dp = Dispatcher()

# === Примерная база туров (заглушка) ===
TOURS = [
    {"title": "Анталия", "price": "500$", "nights": 7},
    {"title": "Пхукет", "price": "800$", "nights": 10},
    {"title": "Шарм-эль-Шейх", "price": "450$", "nights": 7},
    {"title": "Бодрум", "price": "600$", "nights": 7},
    {"title": "Бали", "price": "1200$", "nights": 12},
]

# --- Handlers ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет! Я тур-бот 🤖\n"
        "Напиши /tours <страна/город>, и я найду подходящие предложения.\n\n"
        "Пример: /tours Турция"
    )

@dp.message(Command("tours"))
async def tours_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await message.answer("Укажи страну или город после команды.\nНапример: `/tours Турция`")
        return

    query = args[1].lower()
    results = [t for t in TOURS if query in t["title"].lower()]

    if not results:
        await message.answer("❌ Ничего не найдено. Попробуй другой запрос.")
        return

    response = "🔎 Нашёл такие туры:\n\n"
    for t in results:
        response += f"🌍 {t['title']} — {t['price']} за {t['nights']} ночей\n"

    await message.answer(response)

# --- FastAPI ---
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "tour-bot"}

@app.on_event("startup")
async def on_startup():
    base = os.getenv("RENDER_EXTERNAL_URL")
    if not base:
        base = os.getenv("PUBLIC_URL", f"http://0.0.0.0:{PORT}")
    webhook_url = f"{base}{WEBHOOK_PATH}"

    await bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET
    )
    print(f"✅ Webhook set: {webhook_url}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)

    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)

    return {"ok": True}
