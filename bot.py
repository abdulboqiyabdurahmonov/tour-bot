import os
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

# 🔎 Поиск туров
def search_tours(query: str):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT *
            FROM tours
            WHERE (
                country ILIKE %(q)s
                OR city ILIKE %(q)s
                OR description ILIKE %(q)s
            )
            ORDER BY posted_at DESC
            LIMIT 10
        """, {"q": f"%{query}%"})
        return cur.fetchall()

# 📝 Форматирование ответа
def format_tour(tour: dict) -> str:
    parts = []
    if tour.get("country") or tour.get("city"):
        parts.append(f"🌍 {tour.get('country','')} {tour.get('city','')}")
    if tour.get("hotel"):
        parts.append(f"🏨 {tour['hotel']}")
    if tour.get("price"):
        parts.append(f"💵 {tour['price']} {tour.get('currency','')}")
    if tour.get("dates"):
        parts.append(f"📅 {tour['dates']}")
    if tour.get("description"):
        desc = tour['description'][:200] + "..." if len(tour['description']) > 200 else tour['description']
        parts.append(f"📝 {desc}")
    if tour.get("source_url"):
        parts.append(f"[Источник]({tour['source_url']})")

    return "\n".join(parts)

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
        "👋 Привет! Я умный тур-бот 🤖\n\n"
        "Мы часть **экосистемы TripleA** 🚀\n\n"
        "Здесь только свежие туры за последние 24 часа 🏖️\n\n"
        "Выбирай опцию ниже и погнали! 👇",
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )

# ✈️ Поиск по команде
@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    query = message.text.replace("/search", "").strip()
    if not query:
        await message.answer("🔍 Введите запрос, например:\n`/search Анталья`\n`/search Дубай`")
        return

    tours = search_tours(query)
    if not tours:
        await message.answer("❌ По вашему запросу ничего не найдено.")
        return

    for t in tours:
        text = format_tour(t)
        kb = None
        if t.get("source_url"):
            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("Открыть пост", url=t["source_url"])
            )
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True, parse_mode="Markdown")

# 💬 Любой текст → поиск
@dp.message()
async def handle_plain_text(message: types.Message):
    query = message.text.strip()

    progress_msg = await show_progress(message.chat.id, bot)

    tours = search_tours(query)

    if not tours:
        reply = await ask_gpt(
            f"Пользователь ищет тур: {query}. "
            f"Если в базе нет, дай совет куда лететь в это направление."
        )
        await bot.edit_message_text(
            text=reply,
            chat_id=message.chat.id,
            message_id=progress_msg.message_id
        )
        return

    for t in tours:
        text = format_tour(t)
        kb = None
        if t.get("source_url"):
            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("Открыть пост", url=t["source_url"])
            )
        await bot.send_message(message.chat.id, text, reply_markup=kb, parse_mode="Markdown", disable_web_page_preview=True)

    try:
        await bot.delete_message(message.chat.id, progress_msg.message_id)
    except Exception:
        pass

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
        "• Бесплатно — цены без отелей\n"
        "• Премиум — отели, ссылки и туроператоры\n\n"
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
    tours = search_tours("")[:5]
    if not tours:
        await callback.message.edit_text("⚠️ Пока нет дешёвых туров.", reply_markup=back_menu())
        return

    text = "\n".join([
        f"{t['country']} {t['city'] or ''} — {t['price']} {t['currency']}"
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

# ====== HEALTH CHECK + ROOT ======
@app.get("/healthz", include_in_schema=False)
@app.head("/healthz", include_in_schema=False)
async def health_check():
    return JSONResponse(content={"status": "ok"})

@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root():
    return JSONResponse(content={"status": "ok"})
