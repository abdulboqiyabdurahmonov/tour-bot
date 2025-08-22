import os
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO)

# ============ ENV ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")

bot = Bot(token=TELEGRAM_TOKEN,
          default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher()
app = FastAPI()

# ============ БАЗА ДАННЫХ ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    """Создаём таблицу пользователей, если её нет"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
    logging.info("✅ Таблица users готова")

def save_user(user: types.User):
    """Сохраняем пользователя в БД"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                """, (user.id, user.username, user.first_name, user.last_name))
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пользователя: {e}")

# ============ ХЕНДЛЕРЫ ============
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    save_user(message.from_user)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Найти тур", callback_data="find_tour")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")]
    ])

    name = message.from_user.first_name or "друг"
    username = f"(@{message.from_user.username})" if message.from_user.username else ""

    await message.answer(
        f"👋 Привет, *{name}* {username}!\n\n"
        "Я помогу тебе найти лучшие туры ✈️🏝\n\n"
        "Выбери, что хочешь сделать:",
        reply_markup=kb
    )

@dp.callback_query(F.data == "find_tour")
async def find_tour(callback: types.CallbackQuery):
    await callback.message.answer("✈️ Введи страну или город, куда хочешь поехать:")

@dp.callback_query(F.data == "help")
async def help_cmd(callback: types.CallbackQuery):
    await callback.message.answer(
        "ℹ️ Я бот для поиска туров.\n\n"
        "Команды:\n"
        "/start – начать заново\n"
        "🔍 Найти тур – ввести запрос\n"
    )

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    init_db()
    await bot.set_webhook(f"{WEBHOOK_URL}/webhook")
    logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}/webhook")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logging.info("🛑 Webhook удалён, бот выключен")

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        return JSONResponse(content={"ok": True})
    except Exception as e:
        logging.error(f"Ошибка обработки апдейта: {e}")
        return JSONResponse(content={"ok": False})

@app.get("/")
async def root():
    return {"status": "ok", "message": "🤖 Tour Bot работает!"}
