import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from psycopg.rows import dict_row
from db_init import get_conn
from openai import AsyncOpenAI

# ================== LOGS ==================
logging.basicConfig(level=logging.INFO)

# ================== ENV ==================
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.getenv("PORT", 8080))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не задан в переменных окружения")
if not OPENAI_API_KEY:
    raise RuntimeError("❌ OPENAI_API_KEY не задан в переменных окружения")

# ================== Aiogram / GPT ==================
bot = Bot(token=TOKEN)
dp = Dispatcher()
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# -------------------- KEYBOARDS --------------------
def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🌍 Найти тур")],
            [KeyboardButton(text="🔥 Дешёвые туры")],
            [KeyboardButton(text="ℹ️ О проекте"), KeyboardButton(text="💰 Прайс подписки")],
        ],
        resize_keyboard=True,
    )

def back_menu():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True,
    )

# -------------------- DB --------------------
async def search_tours(query: str):
    """Ищем туры за последние 24 часа"""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT country, city, price, currency, description, hotel, source_url, posted_at
            FROM tours
            WHERE posted_at >= NOW() - INTERVAL '24 hours'
              AND (
                   (country IS NOT NULL AND lower(country) LIKE %s)
                OR (city IS NOT NULL AND lower(city) LIKE %s)
              )
            ORDER BY price ASC
            LIMIT 5
            """,
            (f"%{query}%", f"%{query}%"),
        )
        return cur.fetchall()

async def get_cheap_tours(limit=5):
    """Самые дешёвые туры за последние 3 дня"""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT country, city, price, currency, description, hotel, source_url, posted_at
            FROM tours
            WHERE posted_at >= NOW() - INTERVAL '3 days'
            ORDER BY price ASC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()

# -------------------- GPT Format --------------------
async def format_with_gpt(query: str, results: list, premium: bool = False):
    """Форматируем результаты через GPT (free = без отелей и ссылок)"""
    if not results:
        prompt = f"""
        Пользователь ищет туры по запросу "{query}", но в базе пусто.
        Ответь:
        - Не придумывай ничего
        - Скажи, что туров пока нет
        - Пожелай удачи и предложи заглянуть позже
        """
    else:
        if premium:
            visible = [
                {
                    "country": r["country"],
                    "city": r["city"],
                    "price": r["price"],
                    "currency": r.get("currency", ""),
                    "description": r["description"],
                    "hotel": r.get("hotel"),
                    "source_url": r.get("source_url"),
                    "posted_at": str(r["posted_at"]),
                }
                for r in results
            ]
            restriction = "Покажи всю информацию: страну, город, цену, даты, отель, ссылку 🔗."
        else:
            visible = [
                {
                    "country": r["country"],
                    "city": r["city"],
                    "price": r["price"],
                    "currency": r.get("currency", ""),
                    "posted_at": str(r["posted_at"]),
                }
                for r in results
            ]
            restriction = "Покажи только страну, город, цену и дату. Не показывай отели и ссылки."

        prompt = f"""
        Пользователь ищет туры по запросу "{query}".
        Вот данные (строго не придумывай ничего сверх этого):
        {visible}

        Ограничение:
        {restriction}

        Сформулируй краткий (до 700 символов), дружелюбный ответ с эмодзи.
        """

    resp = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Ты помощник турфирмы. Никогда не придумывай несуществующие туры. Всегда отвечай строго по базе. Общайся дружелюбно и позитивно."},
            {"role": "user", "content": prompt},
        ],
    )
    return resp.choices[0].message.content.strip()

# -------------------- HANDLERS --------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет! Я тур-бот 🤖\n"
        "В бесплатной версии я показываю только цену и направление.\n"
        "В подписке открывается полная информация с отелями и ссылками 🔗\n\n"
        "Выберите опцию 👇",
        reply_markup=main_menu(),
    )

@dp.message(F.text == "🌍 Найти тур")
async def menu_tour(message: types.Message):
    await message.answer(
        "Чтобы найти тур, напиши:\n\n`/tours <страна/город>`\n\n"
        "Пример: `/tours Турция` или просто `Турция`",
        parse_mode="Markdown",
        reply_markup=back_menu(),
    )

@dp.message(F.text == "🔥 Дешёвые туры")
async def menu_cheap(message: types.Message):
    tours = await get_cheap_tours(limit=5)
    if not tours:
        await message.answer("😔 За последние 3 дня ничего не нашли.")
        return
    # FREE режим
    text = await format_with_gpt("дешёвые туры", tours, premium=False)
    await message.answer(text, disable_web_page_preview=True, reply_markup=back_menu())

@dp.message(F.text == "ℹ️ О проекте")
async def menu_about(message: types.Message):
    await message.answer(
        "✨ Бот ищет свежие туры из каналов туроператоров.\n"
        "В бесплатной версии показываем цены и направления 🌍\n"
        "В подписке — полный доступ к отелям и ссылкам ✈️",
        reply_markup=back_menu(),
    )

@dp.message(F.text == "💰 Прайс подписки")
async def menu_price(message: types.Message):
    await message.answer(
        "💳 Подписка на туры:\n\n"
        "• 1 месяц — 99 000 UZS\n"
        "• 3 месяца — 249 000 UZS\n"
        "• 6 месяцев — 449 000 UZS\n\n"
        "После подписки открываются отели и ссылки на туроператоров 🔗",
        reply_markup=back_menu(),
    )

@dp.message(F.text == "🔙 Назад")
async def menu_back(message: types.Message):
    await message.answer("Главное меню 👇", reply_markup=main_menu())

@dp.message(Command("tours"))
async def tours_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await message.answer(
            "Укажи страну или город после команды.\nНапример: `/tours Турция`",
            parse_mode="Markdown",
        )
        return
    query = args[1].lower()
    results = await search_tours(query)
    # FREE режим
    text = await format_with_gpt(query, results, premium=False)
    await message.answer(text)

@dp.message(F.text)
async def handle_plain_text(message: types.Message):
    query = message.text.strip().lower()
    if query:
        results = await search_tours(query)
        text = await format_with_gpt(query, results, premium=False)
        await message.answer(text)

@dp.message(Command("debug"))
async def debug_cmd(message: types.Message):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT count(*) AS cnt FROM tours WHERE posted_at >= NOW() - INTERVAL '24 hours'")
        cnt = cur.fetchone()["cnt"]
    await message.answer(f"📊 В базе {cnt} туров за последние 24 часа ✅")

# -------------------- FASTAPI --------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    base = os.getenv("RENDER_EXTERNAL_URL", f"http://0.0.0.0:{PORT}")
    webhook_url = f"{base}{WEBHOOK_PATH}"
    try:
        await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
        logging.info(f"✅ Webhook set: {webhook_url}")
    except Exception as e:
        logging.error(f"❌ Ошибка при установке webhook: {e}")
    yield
    try:
        await bot.delete_webhook()
        logging.info("🛑 Webhook удалён")
    except Exception as e:
        logging.error(f"❌ Ошибка при удалении webhook: {e}")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "ok", "service": "tour-bot"}

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
