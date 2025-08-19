import os
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from psycopg.rows import dict_row
from db_init import get_conn  # та же функция get_conn, что и в collector.py

from aiogram import F

# ...

@dp.message(F.text)
async def handle_plain_text(message: types.Message):
    query = message.text.strip().lower()

    if not query:
        return

    # читаем туры из Postgres
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT country, city, price, description, source_url, posted_at
            FROM tours
            WHERE (country IS NOT NULL AND lower(country) LIKE %s)
               OR (city IS NOT NULL AND lower(city) LIKE %s)
            ORDER BY posted_at DESC
            LIMIT 5
            """,
            (f"%{query}%", f"%{query}%"),
        )
        results = cur.fetchall()

    if not results:
        await message.answer("❌ Ничего не найдено.")
        return

    response = "🔎 Нашёл такие туры:\n\n"
    for row in results:
        response += f"🌍 {row['country'] or ''} {row['city'] or ''}\n"
        response += f"💰 {row['price']} $\n"
        if row.get("source_url"):
            response += f"🔗 {row['source_url']}\n"
        response += f"📝 {row['description'][:200]}...\n\n"

    await message.answer(response.strip())

# --- ENV ---
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.getenv("PORT", 8080))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

# --- Aiogram ---
bot = Bot(token=TOKEN)
dp = Dispatcher()


# -------------------- HANDLERS --------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет! Я тур-бот 🤖\n"
        "Напиши /tours <страна/город>, и я найду туры из базы.\n\n"
        "Пример: /tours Турция"
    )


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

    # читаем туры из Postgres
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT country, city, price, description, source_url, posted_at
            FROM tours
            WHERE (country IS NOT NULL AND lower(country) LIKE %s)
               OR (city IS NOT NULL AND lower(city) LIKE %s)
            ORDER BY posted_at DESC
            LIMIT 5
            """,
            (f"%{query}%", f"%{query}%"),
        )
        results = cur.fetchall()

    if not results:
        await message.answer("❌ Ничего не найдено.")
        return

    # формируем ответ
    response = "🔎 Нашёл такие туры:\n\n"
    for row in results:
        response += f"🌍 {row['country'] or ''} {row['city'] or ''}\n"
        response += f"💰 {row['price']} $\n"
        if row.get("source_url"):
            response += f"🔗 {row['source_url']}\n"
        response += f"📝 {row['description'][:200]}...\n\n"

    await message.answer(response.strip())


@dp.message(Command("debug"))
async def debug_cmd(message: types.Message):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT count(*) AS cnt FROM tours")
        cnt = cur.fetchone()["cnt"]
    await message.answer(f"В базе сейчас {cnt} туров ✅")


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
