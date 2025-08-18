import os
import json
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import gspread
from google.oauth2.service_account import Credentials

# --- ENV ---
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.getenv("PORT", 8080))
SHEET_NAME = os.getenv("SHEET_NAME", "Tours")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

# --- Google Sheets auth ---
creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
if not creds_json:
    raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS не задан")

creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)
sheet = gc.open(SHEET_NAME).sheet1  # первая вкладка

# --- Aiogram ---
bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет! Я тур-бот 🤖\n"
        "Напиши /tours <страна/город>, и я найду туры из базы.\n\n"
        "Пример: /tours Турция"
    )

@dp.message(Command("tours"))
async def tours_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await message.answer("Укажи страну или город после команды.\nНапример: `/tours Турция`")
        return

    query = args[1].lower()

    # читаем все строки из таблицы
    rows = sheet.get_all_records()
    results = [row for row in rows if query in row["Текст"].lower()]

    if not results:
        await message.answer("❌ Ничего не найдено.")
        return

    # формируем ответ
    response = "🔎 Нашёл такие туры:\n\n"
    for row in results[:5]:  # максимум 5
        response += f"🌍 {row['Текст']}\n💰 {row.get('Цена', 'не указана')}\n🔗 {row.get('Ссылка','')}\n\n"

    await message.answer(response.strip())

# --- FastAPI ---
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "tour-bot"}

@app.on_event("startup")
async def on_startup():
    base = os.getenv("RENDER_EXTERNAL_URL", f"http://0.0.0.0:{PORT}")
    webhook_url = f"{base}{WEBHOOK_PATH}"
    await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
    print(f"✅ Webhook set: {webhook_url}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)

    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)

    return {"ok": True}
