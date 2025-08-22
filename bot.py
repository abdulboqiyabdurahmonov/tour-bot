import os
import logging
import asyncio
import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

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

bot = Bot(token=TELEGRAM_TOKEN, parse_mode="Markdown")
dp = Dispatcher()
app = FastAPI()

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def get_latest_tours(limit=5):
    """Забираем последние туры из таблицы collector"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, price, details, source_url, created_at
                FROM tours
                ORDER BY created_at DESC
                LIMIT %s;
            """, (limit,))
            return cur.fetchall()

# ============ GPT ============
GPT_SYSTEM_PROMPT = """
Ты — дружелюбный и умный ассистент-консультант по путешествиям от TRIPLEA.
Отвечай только в рамках тематики туров, стран, виз, перелётов, отелей, лайфхаков для туристов.
Будь кратким, но полезным. Добавляй эмодзи. 
Презентуй TRIPLEA как экосистему для умных путешествий.
"""

async def ask_gpt(user_message: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": GPT_SYSTEM_PROMPT},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.7,
                },
            )
            data = response.json()
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        logging.error(f"Ошибка GPT: {e}")
        return "😅 Упс, не удалось получить ответ. Попробуйте снова."

# ============ HANDLERS ============
PREMIUM_USERS = {123456789}  # TODO: вставь реальные ID премиум-пользователей

@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "Я — твой *умный гид по путешествиям* от TRIPLEA ✈️🌍\n"
        "Спрашивай о турах, странах, визах, отелях или перелётах — и я помогу.\n\n"
        "🔥 Также могу показать свежие туры прямо сейчас — просто напиши: *туры*"
    )
    await message.answer(text)

@dp.message(F.text.lower() == "туры")
async def show_tours(message: Message):
    tours = get_latest_tours()
    if not tours:
        await message.answer("🙃 Пока нет актуальных туров. Загляни позже.")
        return

    for t in tours:
        base_info = f"🏖 *{t['title']}*\n💵 Цена: {t['price']} USD\n📌 {t['details']}"
        if message.from_user.id in PREMIUM_USERS:
            base_info += f"\n🔗 [Ссылка на источник]({t['source_url']})"
        else:
            base_info += "\n🔒 Ссылка доступна только *премиум* пользователям."

        await message.answer(base_info)

@dp.message()
async def gpt_dialog(message: Message):
    reply = await ask_gpt(message.text)
    await message.answer(reply)

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    logging.info("📦 База данных инициализирована")
    await bot.set_webhook(WEBHOOK_URL)
    logging.info("✅ Webhook установлен")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    logging.info("🛑 Webhook удалён, бот выключен")

@app.post("/webhook")
async def webhook_handler(request: Request):
    update = await request.json()
    await dp.feed_update(bot, update)
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "TRIPLEA Travel Bot is running 🚀"}
