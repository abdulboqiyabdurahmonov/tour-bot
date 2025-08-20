import os
import logging
import asyncio
import httpx
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from psycopg import connect
from psycopg.rows import dict_row

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO)

# ============ ENV ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def init_db():
    """Создание таблицы пользователей"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_premium BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

async def is_premium(user_id: int):
    """Проверка подписки"""
    init_db()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT is_premium FROM users WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO users (user_id, is_premium) VALUES (%s, %s)",
                (user_id, False)
            )
            return False
        return row["is_premium"]

async def get_latest_tours(query: str = None, limit: int = 5, days: int = 3):
    """Берём свежие туры за N дней, фильтруем по стране/городу"""
    sql = """
        SELECT country, city, hotel, price, currency, dates, description, source_url, posted_at
        FROM tours
        WHERE posted_at >= NOW() - (%s * INTERVAL '1 day')
    """
    params = [days]

    if query:
        sql += " AND (LOWER(country) LIKE %s OR LOWER(city) LIKE %s)"
        q = f"%{query.lower()}%"
        params.extend([q, q])

    sql += " ORDER BY posted_at DESC LIMIT %s"
    params.append(limit)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        logging.info(f"SQL: {sql} | params={params}")
        cur.execute(sql, params)
        return cur.fetchall()

# ============ МЕНЮ ============
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 Найти тур", callback_data="find_tour")],
        [InlineKeyboardButton(text="🔥 Дешёвые туры", callback_data="cheap_tours")],
        [InlineKeyboardButton(text="ℹ️ О проекте", callback_data="about")],
        [InlineKeyboardButton(text="💰 Прайс подписки", callback_data="price")],
    ])

def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="menu")]
    ])

# ============ OPENAI GPT ============
async def ask_gpt(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Ты туристический ассистент. Отвечай строго по теме путешествий. Не придумывай туров. Держись фактов из базы."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

# ============ ПРОГРЕСС ============
async def show_progress(chat_id: int, bot: Bot):
    steps = [
        "🤔 Думаю...",
        "🔍 Ищу информацию...",
        "📊 Сравниваю варианты...",
        "✅ Почти готово..."
    ]
    msg = await bot.send_message(chat_id, steps[0])  # первое сообщение
    for step in steps[1:]:
        await asyncio.sleep(2)
        try:
            await bot.edit_message_text(step, chat_id, msg.message_id)
        except Exception:
            pass
    return msg

# ============ ОБРАБОТЧИКИ ============
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "👋 Привет! Я умный тур-бот 🤖\n\n"
        "Мы часть **экосистемы TripleA** — проектов для автоматизации, путешествий и новых возможностей 🚀\n\n"
        "Здесь ты найдёшь только свежие туры за последние 24 часа 🏖️\n\n"
        "Выбирай опцию ниже и погнали! 👇",
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )

@dp.message()
async def handle_plain_text(message: types.Message):
    query = message.text.strip()

    # показываем прогресс
    progress_msg = await show_progress(message.chat.id, bot)

    premium = await is_premium(message.from_user.id)
    tours = await get_latest_tours(query=query, limit=5, days=3)

    if not tours:
        reply = await ask_gpt(f"Пользователь ищет тур: {query}. Если в базе нет, дай совет куда лететь в это направление.")
        await bot.edit_message_text(reply, message.chat.id, progress_msg.message_id)
        return

    if premium:
        text = "\n\n".join([
            f"{t['country']} {t['city'] or ''} — {t['price']} {t['currency']}\n🏨 {t['hotel'] or 'Отель не указан'}\n🔗 {t['source_url'] or ''}"
            for t in tours
        ])
    else:
        text = "\n".join([
            f"{t['country']} {t['city'] or ''} — {t['price']} {t['currency']}"
            for t in tours
        ])

    await bot.edit_message_text(
        f"📋 Нашёл такие варианты:\n\n{text}",
        message.chat.id,
        progress_msg.message_id
    )

# ============ CALLBACKS ============
@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

@dp.callback_query(F.data == "about")
async def about(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🌐 Мы — часть экосистемы **TripleA**.\n\n"
        "Наши проекты помогают бизнесу и людям:\n"
        "• Автоматизация процессов 🤖\n"
        "• Путешествия и выгодные туры 🏝️\n"
        "• Новые возможности для роста 🚀",
        parse_mode="Markdown",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "price")
async def price(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 Подписка TripleA Travel:\n\n"
        "• Бесплатно — цены без отелей\n"
        "• Премиум — отели, ссылки и туроператоры\n\n"
        "Подключение премиум подписки скоро будет доступно 🔑",
        reply_markup=back_menu(),
    )

# ============ FASTAPI (WEBHOOK) ============
@app.on_event("startup")
async def on_startup():
    init_db()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info("✅ Webhook установлен")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logging.info("🛑 Webhook удалён")

@app.post("/webhook")
async def webhook_handler(request: Request):
    update = types.Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}
