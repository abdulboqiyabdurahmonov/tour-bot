import os
import logging
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
        # таблица users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                is_premium BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # проверка недостающих колонок
        columns = [
            ("premium_until", "TIMESTAMP"),
            ("searches_today", "INT DEFAULT 0"),
            ("last_search_date", "DATE")
        ]
        for name, col_type in columns:
            cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = '{name}'
                ) THEN
                    ALTER TABLE users ADD COLUMN {name} {col_type};
                END IF;
            END$$;
            """)

        # таблица requests
        cur.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                query TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # таблица tours (на будущее)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tours (
                id SERIAL PRIMARY KEY,
                country TEXT,
                city TEXT,
                hotel TEXT,
                price NUMERIC,
                currency TEXT,
                dates TEXT,
                description TEXT,
                source_chat TEXT,
                message_id BIGINT,
                posted_at TIMESTAMP DEFAULT NOW()
            );
        """)

    logging.info("✅ Таблицы users, requests и tours готовы")

def save_user(user: types.User):
    """Сохраняем пользователя в БД"""
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user.id, user.username, full_name))

def save_request(user_id: int, query: str, response: str):
    """Сохраняем запрос юзера и ответ GPT"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO requests (user_id, query, response)
            VALUES (%s, %s, %s);
        """, (user_id, query, response))

def search_tours(query: str):
    """Поиск туров в таблице tours"""
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
    context = "Ты тревел-ассистент. Отвечай кратко и понятно. Если просят тур, используй только данные из таблицы. Не придумывай новые туры."

    if tours:
        tours_text = "\n\n".join([
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

    tours = search_tours(user_text)
    premium = False  # TODO: проверка подписки

    reply = await ask_gpt(user_text, tours, premium)

    save_request(user_id, user_text, reply)

    await message.answer(reply)

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    init_db()
    await bot.set_webhook(f"{WEBHOOK_URL}/webhook")
    logging.info("✅ Webhook установлен и база инициализирована")

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
        logging.error(f"Ошибка: {e}")
        return JSONResponse(content={"ok": False})

@app.get("/")
async def root():
    return {"status": "ok", "message": "🤖 Tour Bot работает!"}
