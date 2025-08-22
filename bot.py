import os
import logging
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row

import httpx
from aiogram.utils.markdown import quote_md

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO)

# ============ ENV ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")

if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)
dp = Dispatcher()
app = FastAPI()


# ============ БАЗА ДАННЫХ ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)


def init_db():
    """Создаём таблицы, если их нет"""
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    question TEXT,
                    answer TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
    logging.info("✅ Таблицы users и requests готовы")


def save_user(user: types.User):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
            """, (user.id, user.username, user.first_name, user.last_name))


def save_request(user_id: int, question: str, answer: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO requests (user_id, question, answer)
                VALUES (%s, %s, %s)
            """, (user_id, question, answer))


# ============ GPT ============
async def ask_gpt(question: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "Ты — помощник по туризму. Отвечай кратко, полезно и дружелюбно."},
                        {"role": "user", "content": question},
                    ],
                    "max_tokens": 300,
                }
            )
            data = response.json()
            return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.error(f"Ошибка GPT: {e}")
        return "⚠️ Извини, у меня не получилось получить ответ. Попробуй ещё раз."


# ============ ХЕНДЛЕРЫ ============
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    save_user(message.from_user)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Найти тур", callback_data="find_tour")],
        [InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")]
    ])

    name = message.from_user.first_name or "друг"
    await message.answer(
        f"👋 Привет, *{quote_md(name)}*!\n\n"
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
        "Или просто задай вопрос 😉"
    )


@dp.message()
async def handle_question(message: types.Message):
    q = message.text.strip()
    answer = await ask_gpt(q)
    save_request(message.from_user.id, q, answer)

    await message.answer(quote_md(answer))


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
    return {"status": "ok", "message": "🤖 Tour Bot с GPT работает!"}
