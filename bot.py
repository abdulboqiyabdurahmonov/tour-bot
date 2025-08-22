import os
import logging
import asyncio
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row
import openai

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO)

# ============ ENV ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not TELEGRAM_TOKEN or not DATABASE_URL or not WEBHOOK_URL or not OPENAI_API_KEY:
    raise ValueError("❌ Проверь переменные окружения!")

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)
dp = Dispatcher()
app = FastAPI()
openai.api_key = OPENAI_API_KEY

# ============ БАЗА ДАННЫХ ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                query TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
    logging.info("✅ Таблицы готовы")

def save_user(user: types.User):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user.id, user.username, user.first_name, user.last_name))

def save_request(user_id: int, query: str, response: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO requests (user_id, query, response)
            VALUES (%s, %s, %s);
        """, (user_id, query, response))

def search_tours(query: str):
    """Ищем туры в таблице по ключевым словам"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM tours
            WHERE country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s
            ORDER BY posted_at DESC
            LIMIT 5;
        """, (f"%{query}%", f"%{query}%", f"%{query}%"))
        return cur.fetchall()

# ============ GPT ============
async def ask_gpt(user_text: str, tours=None, premium=False):
    context = "Ты тревел-ассистент. Отвечай кратко и понятно. Если просят тур, используй данные из базы. Не придумывай новые туры."
    if tours:
        tours_text = "\n".join([
            f"🏨 {t['hotel'] or 'Отель не указан'} | {t['city']}, {t['country']}\n"
            f"💵 {t['price']} {t['currency']} | 📅 {t['dates'] or 'даты не указаны'}\n"
            f"{t['description'][:120]}..."
            + (f"\n🔗 https://t.me/{t['source_chat']}/{t['message_id']}" if premium else "")
            for t in tours
        ])
        user_text = f"Пользователь ищет тур: {user_text}\n\nВот найденные варианты:\n{tours_text}"

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": context},
            {"role": "user", "content": user_text}
        ],
        max_tokens=500,
        temperature=0.7
    )
    return response.choices[0].message["content"]

# ============ ХЕНДЛЕРЫ ============
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    save_user(message.from_user)
    name = message.from_user.first_name or "друг"
    await message.answer(
        f"👋 Привет, *{name}*!\n\n"
        "Я твой тревел-ассистент. Пиши, куда хочешь поехать, а я подберу лучшие туры ✈️🌴\n\n"
        "_Пример: 'Хочу тур в Нячанг на октябрь'_"
    )

@dp.message()
async def handle_message(message: types.Message):
    user_text = message.text.strip()
    user_id = message.from_user.id

    # 1. Ищем туры
    tours = search_tours(user_text)
    premium = False  # потом сделаем проверку подписки

    # 2. GPT отвечает
    reply = await ask_gpt(user_text, tours, premium)

    # 3. Сохраняем в БД
    save_request(user_id, user_text, reply)

    # 4. Отправляем пользователю
    await message.answer(reply)

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    init_db()
    await bot.set_webhook(f"{WEBHOOK_URL}/webhook")
    logging.info("✅ Webhook установлен")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logging.info("🛑 Webhook удалён")

@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        return JSONResponse(content={"ok": True})
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        return JSONResponse(content={"ok": False})

@app.get("/")
async def root():
    return {"status": "ok", "message": "🤖 Tour Bot работает!"}
