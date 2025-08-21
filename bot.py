import os
import logging
import asyncio
import httpx
import random
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_premium BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
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
            source_url TEXT,
            posted_at TIMESTAMP DEFAULT NOW()
        );
        """)

async def is_premium(user_id: int):
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

async def get_latest_tours(query: str = None, budget: int = None, limit: int = 5, hours: int = 24):
    sql = """
        SELECT country, city, hotel, price, currency, dates, description, source_url, posted_at
        FROM tours
        WHERE posted_at >= NOW() - (%s || ' hours')::interval
    """
    params = [str(hours)]

    if query:
        sql += " AND (LOWER(country) LIKE %s OR LOWER(city) LIKE %s)"
        q = f"%{query.lower()}%"
        params.extend([q, q])

    if budget:
        sql += " AND price <= %s"
        params.append(budget)

    sql += " ORDER BY posted_at DESC LIMIT %s"
    params.append(limit)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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

# ============ OPENAI ============
async def ask_gpt(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Ты туристический ассистент. Отвечай строго по теме путешествий, подсказывай советы и лайфхаки для туристов."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.4
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

# ============ ПРОГРЕСС ============
async def show_progress(chat_id: int, bot: Bot):
    steps = [
        "🤔 Думаю...",
        "🔍 Ищу туры...",
        "📊 Сравниваю варианты...",
        "✅ Почти готово..."
    ]
    msg = await bot.send_message(chat_id, steps[0])
    for step in steps[1:]:
        await asyncio.sleep(2)
        try:
            await bot.edit_message_text(step, chat_id, msg.message_id)
        except Exception:
            pass
    return msg

# ============ ПРИВЕТСТВИЯ ============
WELCOME_MESSAGES = [
    "👋 Привет, путешественник!\n\nЯ — твой личный *тур-ассистент* 🏝️🤖",
    "✈️ Готов лететь? Я помогу найти свежие туры 🌍",
    "🌞 Хочешь море и солнце прямо сейчас? Давай посмотрим варианты!",
    "🚀 TripleA Travel к твоим услугам — только лучшие туры за 24 часа!",
    "🔥 Ищу горячие туры специально для тебя 🔎"
]

# ============ ОБРАБОТЧИКИ ============
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    greeting = random.choice(WELCOME_MESSAGES)
    await message.answer(
        f"{greeting}\n\n"
        "📌 У меня есть только *свежие туры* за последние 24 часа.\n"
        "💰 По бюджету? Напиши: _Туры до 1000 долларов_\n"
        "🌍 По направлению? Пиши: _Турция_ или _Дубай_\n\n"
        "✨ Бесплатно: страна, отель, цена, даты и описание.\n"
        "🔑 Премиум: ссылки, контакты туроператоров и больше деталей.\n\n"
        "Выбирай и поехали! 🚀",
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )

@dp.message()
async def handle_plain_text(message: types.Message):
    query = message.text.strip()
    budget = None

    # Понимаем бюджет из текста (например: "до 1000 долларов")
    import re
    budget_match = re.search(r"до\s+(\d+)", query.lower())
    if budget_match:
        budget = int(budget_match.group(1))

    progress_msg = await show_progress(message.chat.id, bot)
    premium = await is_premium(message.from_user.id)
    tours = await get_latest_tours(query=query if not budget else None, budget=budget, limit=5, hours=24)

    if not tours:
        reply = f"⚠️ За последние 24 часа туров по запросу '{query}' не найдено.\n\n"
        gpt_suggestion = await ask_gpt(
            f"Пользователь ищет тур: {query}. "
            f"Если в базе нет, предложи альтернативные направления в регионе."
        )
        reply += gpt_suggestion
        await bot.edit_message_text(
            text=reply,
            chat_id=message.chat.id,
            message_id=progress_msg.message_id,
            reply_markup=back_menu()
        )
        return

    # Форматирование
    if premium:
        text = "\n\n".join([
            f"🌍 **{t['country']} {t['city'] or ''}**\n"
            f"🏨 {t['hotel'] or 'Отель не указан'}\n"
            f"💲 {t['price']} {t['currency']}\n"
            f"📅 {t['dates'] or 'Даты не указаны'}\n"
            f"📝 {t['description'] or 'Без описания'}\n"
            f"🔗 {t['source_url'] or ''}"
            for t in tours
        ])
    else:
        text = "\n\n".join([
            f"🌍 **{t['country']} {t['city'] or ''}**\n"
            f"🏨 {t['hotel'] or 'Отель не указан'}\n"
            f"💲 {t['price']} {t['currency']}\n"
            f"📅 {t['dates'] or 'Даты не указаны'}\n"
            f"📝 {t['description'] or 'Без описания'}"
            for t in tours
        ])

    await bot.edit_message_text(
        text=f"📋 Нашёл такие варианты:\n\n{text}",
        chat_id=message.chat.id,
        message_id=progress_msg.message_id,
        reply_markup=back_menu(),
        parse_mode="Markdown"
    )

# ============ CALLBACKS ============
@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

@dp.callback_query(F.data == "about")
async def about(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🌐 Мы — часть экосистемы **TripleA**.\n\n"
        "Автоматизация процессов 🤖\n"
        "Путешествия и выгодные туры 🏝️\n"
        "Новые возможности для роста 🚀",
        parse_mode="Markdown",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "price")
async def price(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 Подписка TripleA Travel:\n\n"
        "• Бесплатно — страна, отель, цена, даты, описание\n"
        "• Премиум — ссылки и контакты туроператоров\n\n"
        "Подключение премиум скоро 🔑",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "find_tour")
async def find_tour(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введи название страны или города:",
        reply_markup=back_menu()
    )

@dp.callback_query(F.data == "cheap_tours")
async def cheap_tours(callback: types.CallbackQuery):
    tours = await get_latest_tours(limit=5, hours=24)
    if not tours:
        await callback.message.edit_text("⚠️ За последние 24 часа дешёвых туров не найдено.", reply_markup=back_menu())
        return

    text = "\n\n".join([
        f"🌍 **{t['country']} {t['city'] or ''}** — 💲 {t['price']} {t['currency']}"
        for t in tours
    ])

    await callback.message.edit_text(
        f"🔥 Свежие дешёвые туры:\n\n{text}",
        reply_markup=back_menu(),
        parse_mode="Markdown"
    )

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    init_db()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}")

@app.on_event("shutdown")
async def on_shutdown():
    logging.info("🛑 Shutdown event — webhook НЕ удаляется")
    await bot.session.close()

@app.post("/webhook")
async def webhook_handler(request: Request):
    update = types.Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

# ====== HEALTH CHECK + ROOT ======
@app.get("/healthz", include_in_schema=False)
@app.head("/healthz", include_in_schema=False)
async def health_check():
    return JSONResponse(content={"status": "ok"})

@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root():
    return JSONResponse(content={"status": "ok"})
