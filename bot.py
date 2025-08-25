import os
import logging
import asyncio
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row

# ================= ЛОГИ =================
logging.basicConfig(level=logging.INFO)

# ================= ENV =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")  # теперь вместо SEARCH_API

WEBHOOK_HOST = os.getenv("WEBHOOK_URL", "https://tour-bot-rxi8.onrender.com")  # домен Render
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не найден в переменных окружения!")

# ================= БОТ =================
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)
dp = Dispatcher()
app = FastAPI()

# ================= БАЗА ДАННЫХ =================
async def fetch_tours(query: str):
    """Ищем туры: сначала за последние 24 часа, если пусто — берём последние вообще"""
    try:
        cutoff = datetime.utcnow() - timedelta(hours=24)
        sql_recent = """
            SELECT country, city, hotel, price, currency, dates, source_url, posted_at, created_at
            FROM tours
            WHERE (country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s)
              AND created_at >= %s
            ORDER BY created_at DESC
            LIMIT 10
        """
        params = [f"%{query}%", f"%{query}%", f"%{query}%", cutoff]

        with connect(DATABASE_URL, autocommit=True, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_recent, params)
                rows = cur.fetchall()

                # Если свежих туров нет → берём последние вообще
                if not rows:
                    sql_fallback = """
                        SELECT country, city, hotel, price, currency, dates, source_url, posted_at, created_at
                        FROM tours
                        WHERE (country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s)
                        ORDER BY created_at DESC
                        LIMIT 5
                    """
                    cur.execute(sql_fallback, params[:3])  # cutoff не нужен
                    rows = cur.fetchall()

        return rows
    except Exception as e:
        logging.error(f"Ошибка при fetch_tours: {e}")
        return []

# ================= GPT =================
import httpx

async def ask_gpt(prompt: str, premium: bool = False) -> list[str]:
    """GPT отвечает по теме путешествий"""
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
                        {"role": "system", "content": (
                            "Ты — AI-консультант по путешествиям из экосистемы TripleA. "
                            "Отвечай дружелюбно и информативно. "
                            "Советы, туры, отели, лайфхаки, погода, цены, культура. "
                            "Не уходи от тематики путешествий."
                        )},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.7,
                },
            )

        data = response.json()
        answer = data["choices"][0]["message"]["content"].strip()

        # Доп. логика Free / Premium
        if premium:
            answer += "\n\n🔗 *Источник тура:* [Перейти](https://t.me/triplea_channel)"
        else:
            answer += "\n\n✨ Хочешь видеть прямые ссылки на источники туров? Подключи Premium доступ TripleA."

        # Ограничиваем длину (Telegram лимит ~4096)
        MAX_LEN = 3800
        if len(answer) > MAX_LEN:
            return [answer[i:i+MAX_LEN] for i in range(0, len(answer), MAX_LEN)]
        return [answer]

    except Exception as e:
        logging.error(f"Ошибка GPT: {e}")
        return ["⚠️ Упс! Ошибка при обращении к AI. Попробуй ещё раз."]

# ================= ВСПОМОГАТЕЛЬНОЕ =================
async def show_typing(message: Message, text: str = "🤔 Думаю... Ищу варианты для тебя"):
    """Показываем, что бот думает"""
    try:
        await bot.send_chat_action(message.chat.id, "typing")
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка show_typing: {e}")

# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    intro = (
        "🌍 Привет! Я — *TripleA Travel Bot* ✈️\n\n"
        "Я помогу тебе найти *актуальные туры, советы по странам и лайфхаки путешественников*.\n\n"
        "💡 Просто напиши запрос:\n"
        "• Тур в Турцию в сентябре\n"
        "• Погода в Бали в октябре\n"
        "• Лучшие отели в Дубае\n\n"
        "✨ Доступно: вся информация по турам\n"
        "🔒 Premium: прямая ссылка на источник тура\n\n"
        "Что тебя интересует? 😊"
    )
    await message.answer(intro)

@dp.message(F.text)
async def handle_message(message: Message):
    user_text = message.text.strip()

    # показываем "думаю..."
    await show_typing(message)

    # 1) Пробуем найти свежие туры в базе
    tours = await fetch_tours(user_text)
    if tours:
        reply = "🔥 Нашёл свежие туры за последние 24 часа:\n\n"
        for t in tours:
            reply += (
                f"🌍 {t.get('country') or 'Страна не указана'} — {t.get('city') or 'Город не указан'}\n"
                f"🏨 {t.get('hotel') or 'Отель не указан'}\n"
                f"💵 {t.get('price')} {t.get('currency')}\n"
                f"📅 {t.get('dates') or 'Даты не указаны'}\n"
                f"🔗 [Источник]({t.get('source_url')})\n\n"
            )
        await message.answer(reply)
        return

    # 2) Если нет туров — подключаем GPT
    premium_users = {123456789}
    is_premium = message.from_user.id in premium_users
    replies = await ask_gpt(user_text, premium=is_premium)
    for part in replies:
        await message.answer(part)

# ================= WEBHOOK =================
@app.get("/")
async def root():
    return {"status": "ok", "message": "TripleA Travel Bot is running!"}

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        await asyncio.sleep(0)
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    return JSONResponse({"status": "ok"})

@app.on_event("startup")
async def on_startup():
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}")
    else:
        logging.warning("WEBHOOK_URL не указан — бот не получит апдейты.")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()
