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
            {"role": "system", "content": "Ты туристический ассистент. Отвечай строго по теме путешествий. Не придумывай несуществующих туров. Держись фактов из данных."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

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

    await message.answer(
        "🆘 Как пользоваться ботом:\n\n"
        "• Нажми **🌍 Найти тур**, чтобы искать по стране или городу.\n"
        "   👉 Пример: напиши *Турция* или */tours Дубай*\n\n"
        "• В разделе **🔥 Дешёвые туры** показываем самые выгодные за 3 дня.\n\n"
        "• В меню **ℹ️ О проекте** расскажем подробнее, как работает экосистема TripleA.\n\n"
        "• В **💰 Прайс подписки** смотри тарифы и условия доступа к полным данным.\n\n"
        "📩 Если остались вопросы — пиши прямо сюда, мы всегда на связи!",
        parse_mode="Markdown",
        reply_markup=back_menu(),
    )

@dp.message()
async def handle_plain_text(message: types.Message):
    query = message.text.strip()
    premium = await is_premium(message.from_user.id)

    # эмуляция "поиска туров" (позже сюда подключим парсинг)
    tours = [
        {"country": "Турция", "price": 500, "hotel": "Hilton Antalya"},
        {"country": "ОАЭ", "price": 450, "hotel": "Dubai Marina Hotel"},
        {"country": "Египет", "price": 400, "hotel": "Sharm Beach Resort"},
    ]

    # фильтр по тексту
    results = [t for t in tours if query.lower() in t["country"].lower()]

    if not results:
        reply = await ask_gpt(f"Пользователь ищет тур: {query}. Ответь кратко и строго по теме.")
        await message.answer(reply)
        return

    if premium:
        text = "\n".join([f"{t['country']} — {t['price']}$ ({t['hotel']})" for t in results])
    else:
        text = "\n".join([f"{t['country']} — {t['price']}$" for t in results])

    await message.answer(f"📋 Нашёл такие варианты:\n\n{text}")

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
    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        await bot.set_webhook(webhook_url)
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
