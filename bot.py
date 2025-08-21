import os
import re
import logging
import asyncio
import httpx
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

async def get_latest_tours(query: str = None, limit: int = 5, hours: int = 24):
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

    sql += " ORDER BY posted_at DESC LIMIT %s"
    params.append(limit)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

# ============ ПАРСЕР БЮДЖЕТА ============
def parse_budget(query: str):
    """Ищет бюджет: 'до 1000 долларов' / 'до 5000 сум' / 'до 800 euro'"""
    pattern = r"до\s*(\d+)\s*(\$|usd|доллар|долл|сум|uzs|₽|руб|eur|€)"
    m = re.search(pattern, query.lower())
    if m:
        amount = int(m.group(1))
        currency = m.group(2)
        # нормализация валют
        if currency in ["$", "usd", "доллар", "долл"]:
            currency = "USD"
        elif currency in ["сум", "uzs"]:
            currency = "UZS"
        elif currency in ["₽", "руб"]:
            currency = "RUB"
        elif currency in ["eur", "€"]:
            currency = "EUR"
        return amount, currency
    return None, None

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
            {"role": "system", "content": "Ты туристический ассистент. Отвечай строго по теме путешествий."},
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
        "🔍 Ищу варианты...",
        "📊 Проверяю источники...",
        "✅ Готовлю результаты..."
    ]
    msg = await bot.send_message(chat_id, steps[0])
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
        "👋 Привет! Я — умный тур-бот 🤖\n\n"
        "Здесь только **свежие туры за последние 24 часа** 🏖️\n\n"
        "Выбирай опцию ниже и поехали! 🚀",
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )

@dp.message()
async def handle_plain_text(message: types.Message):
    query = message.text.strip()
    progress_msg = await show_progress(message.chat.id, bot)

    premium = await is_premium(message.from_user.id)

    # --- проверка бюджета ---
    budget, currency = parse_budget(query)
    if budget:
        tours = await get_latest_tours(limit=20, hours=24)
        tours = [t for t in tours if t["currency"] == currency and t["price"] <= budget]
        header = f"📊 За последние 24 часа нашёл туры до {budget} {currency}:"
    else:
        tours = await get_latest_tours(query=query, limit=5, hours=24)
        header = f"📋 Нашёл такие варианты:"

    if not tours:
        reply = f"⚠️ За последние 24 часа туров не найдено по запросу: {query}."
        gpt_suggestion = await ask_gpt(f"Подскажи туристические направления, похожие на: {query}.")
        reply += "\n\n💡 Советы: " + gpt_suggestion
        await bot.edit_message_text(
            text=reply,
            chat_id=message.chat.id,
            message_id=progress_msg.message_id,
            reply_markup=back_menu()
        )
        return

    # --- формирование текста ---
    if premium:
        text = "\n\n".join([
            f"🌍 {t['country']} {t['city'] or ''}\n"
            f"💲 {t['price']} {t['currency']}\n"
            f"🏨 {t['hotel'] or 'Отель не указан'}\n"
            f"📅 {t['dates'] or 'Даты не указаны'}\n"
            f"📝 {t['description'][:120]+'...' if t['description'] else ''}\n"
            f"🔗 {t['source_url'] or ''}"
            for t in tours
        ])
    else:
        text = "\n\n".join([
            f"🌍 {t['country']} {t['city'] or ''}\n"
            f"💲 {t['price']} {t['currency']}\n"
            f"🏨 {t['hotel'] or 'Отель не указан'}\n"
            f"📅 {t['dates'] or 'Даты не указаны'}\n"
            f"📝 {t['description'][:60]+'...' if t['description'] else ''}"
            for t in tours
        ])

    await bot.edit_message_text(
        text=f"{header}\n\n{text}",
        chat_id=message.chat.id,
        message_id=progress_msg.message_id,
        reply_markup=back_menu()
    )

# ============ CALLBACKS ============
@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

@dp.callback_query(F.data == "about")
async def about(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🌐 Мы — часть экосистемы **TripleA**.\n\n"
        "🤖 Автоматизация процессов\n"
        "🏝️ Путешествия и выгодные туры\n"
        "🚀 Новые возможности для роста",
        parse_mode="Markdown",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "price")
async def price(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 Подписка TripleA Travel:\n\n"
        "• Бесплатно — цены + отели + даты\n"
        "• Премиум — полный пакет (отели, описания, ссылки)\n\n"
        "Подключение премиум скоро 🔑",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "find_tour")
async def find_tour(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введи название страны, города или бюджет (например: 'туры до 1000 долларов'):",
        reply_markup=back_menu()
    )

@dp.callback_query(F.data == "cheap_tours")
async def cheap_tours(callback: types.CallbackQuery):
    tours = await get_latest_tours(limit=5, hours=24)
    if not tours:
        await callback.message.edit_text("⚠️ За последние 24 часа дешёвых туров не найдено.", reply_markup=back_menu())
        return

    text = "\n".join([
        f"🌍 {t['country']} {t['city'] or ''} — 💲 {t['price']} {t['currency']}"
        for t in tours
    ])

    await callback.message.edit_text(
        f"🔥 Свежие дешёвые туры:\n\n{text}",
        reply_markup=back_menu()
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
