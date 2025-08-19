import os
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command

from psycopg.rows import dict_row
from db_init import get_conn

from openai import AsyncOpenAI

# --- ENV ---
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.getenv("PORT", 8080))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY не задан в переменных окружения")

# --- Aiogram ---
bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- GPT клиент ---
client = AsyncOpenAI(api_key=OPENAI_API_KEY)


# -------------------- HANDLERS --------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет! Я тур-бот 🤖\n"
        "Напиши /tours <страна/город> или просто название города, и я найду свежие туры (за последние 24 часа).\n\n"
        "Пример: /tours Турция или просто Турция"
    )


async def search_tours(query: str):
    """Поиск туров в базе только за последние 24 часа"""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT country, city, price, description, source_url, posted_at
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


async def format_with_gpt(query: str, results: list):
    """Оформляем ответ через GPT"""
    if not results:
        prompt = f"""
        Пользователь ищет туры по запросу "{query}", но в базе за последние 24 часа ничего нет.
        Ответь вежливо, дружелюбно и человечно. 
        Подскажи, что новых туров пока нет, но стоит заглянуть позже.
        """
    else:
        prompt = f"""
        Пользователь ищет туры по запросу "{query}".
        Вот список туров (каждый тур: страна, город, цена, ссылка, описание):
        {results}

        Сформулируй красивый, дружелюбный и понятный ответ для клиента:
        - В начале добавь приветствие с эмодзи
        - Представь туры в виде живого текста (короткие описания, но не сухой список)
        - Если есть ссылки, укажи их с 🔗
        - Не превышай 800 символов
        """

    resp = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Ты помощник-бот турфирмы. Пиши дружелюбно, понятно и продающе."},
            {"role": "user", "content": prompt},
        ],
    )
    return resp.choices[0].message.content.strip()


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
    text = await format_with_gpt(query, results)
    await message.answer(text)


@dp.message(F.text)
async def handle_plain_text(message: types.Message):
    query = message.text.strip().lower()
    if not query:
        return
    results = await search_tours(query)
    text = await format_with_gpt(query, results)
    await message.answer(text)


@dp.message(Command("debug"))
async def debug_cmd(message: types.Message):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT count(*) AS cnt FROM tours WHERE posted_at >= NOW() - INTERVAL '24 hours'")
        cnt = cur.fetchone()["cnt"]
    await message.answer(f"В базе {cnt} туров за последние 24 часа ✅")


# -------------------- FASTAPI --------------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "tour-bot"}

@app.on_event("startup")
async def on_startup():
    base = os.getenv("RENDER_EXTERNAL_URL", f"http://0.0.0.0:{PORT}")
    webhook_url = f"{base}{WEBHOOK_PATH}"
    await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
    print(f"✅ Webhook set: {webhook_url}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return Response(status_code=403)

    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
