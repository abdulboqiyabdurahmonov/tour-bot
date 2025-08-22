import os
import logging
import asyncio
import httpx
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramForbiddenError

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

bot = Bot(token=TELEGRAM_TOKEN,
          default=DefaultBotProperties(parse_mode="Markdown"))
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
            searches_today INT DEFAULT 0,
            last_search_date DATE,
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

async def increment_search(user_id: int):
    """Счётчик бесплатных поисков (лимит 5/день)."""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT searches_today, last_search_date FROM users WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        today = datetime.utcnow().date()

        if not row:
            cur.execute(
                "INSERT INTO users (user_id, searches_today, last_search_date) VALUES (%s, %s, %s)",
                (user_id, 1, today)
            )
            return 1

        if row["last_search_date"] != today:
            cur.execute("UPDATE users SET searches_today = 1, last_search_date = %s WHERE user_id = %s",
                        (today, user_id))
            return 1
        else:
            new_count = row["searches_today"] + 1
            cur.execute("UPDATE users SET searches_today = %s WHERE user_id = %s",
                        (new_count, user_id))
            return new_count

async def get_latest_tours(query: str = None, limit: int = 5, hours: int = 24, max_price: int = None):
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

    if max_price:
        sql += " AND price <= %s"
        params.append(max_price)

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

# ============ GPT ============
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
    steps = ["🤔 Думаю...", "🔍 Ищу туры...", "📊 Сравниваю варианты...", "✅ Готово!"]
    try:
        msg = await bot.send_message(chat_id=chat_id, text=steps[0])
    except TelegramForbiddenError:
        logging.warning(f"❌ Пользователь {chat_id} заблокировал бота")
        return None
    except Exception as e:
        logging.error(f"Ошибка при отправке прогресса: {e}")
        return None

    for step in steps[1:]:
        await asyncio.sleep(2)
        try:
            await bot.edit_message_text(
                text=step,
                chat_id=chat_id,
                message_id=msg.message_id
            )
        except Exception:
            pass
    return msg

# ============ ФОРМАТ ============
def format_tour(t):
    return (
        f"🌍 *{t['country']} {t['city'] or ''}*\n"
        f"💲 {t['price']} {t['currency']}\n"
        f"🏨 {t['hotel'] or 'Не указан'}\n"
        f"📅 {t['dates'] or 'Не указаны'}\n"
        f"📝 {t['description'] or ''}\n"
        f"🔗 {t['source_url'] or ''}"
    )

# ============ ОБРАБОТЧИКИ ============
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "👋 Привет, путешественник!\n\n"
        "✈️ Я помогу найти туры за последние 24 часа.\n\n"
        "🔓 Бесплатно — до 5 поисков в день\n"
        "💎 Премиум пока недоступен\n\n"
        "Выбирай, и поехали 🌴",
        reply_markup=main_menu(),
    )

@dp.message()
async def handle_plain_text(message: types.Message):
    user_id = message.from_user.id
    query = message.text.strip()
    max_price = None

    if "до" in query and any(x in query.lower() for x in ["usd", "дол", "$"]):
        try:
            parts = query.lower().replace("usd", "").replace("долларов", "").replace("$", "").split("до")
            max_price = int(parts[1].strip().split()[0])
        except Exception:
            pass

    # проверка лимита бесплатных поисков
    count = await increment_search(user_id)
    if count > 5:
        await message.answer(
            "⚠️ Лимит бесплатных поисков (5/день) исчерпан.\n\n"
            "🔑 Премиум пока недоступен.",
            reply_markup=back_menu()
        )
        return

    progress_msg = await show_progress(message.chat.id, bot)
    if not progress_msg:
        return

    tours = await get_latest_tours(query=query if not max_price else None, limit=5, hours=24, max_price=max_price)

    if not tours:
        reply = f"⚠️ За последние 24 часа туров по запросу '{query}' не найдено.\n\n"
        gpt_suggestion = await ask_gpt(f"Альтернативные направления для запроса: {query}")
        reply += gpt_suggestion
        try:
            await bot.edit_message_text(
                text=reply,
                chat_id=message.chat.id,
                message_id=progress_msg.message_id,
                reply_markup=back_menu()
            )
        except Exception:
            pass
        return

    header = "📋 Нашёл такие варианты:\n\n"
    if max_price:
        header = f"💰 Свежие туры до {max_price} USD:\n\n"

    text = "\n\n".join([format_tour(t) for t in tours])

    try:
        await bot.edit_message_text(
            text=header + text,
            chat_id=message.chat.id,
            message_id=progress_msg.message_id,
            reply_markup=back_menu()
        )
    except Exception as e:
        logging.error(f"Ошибка при обновлении сообщения: {e}")

# ============ CALLBACKS ============
@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

@dp.callback_query(F.data == "about")
async def about(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🌐 Мы — часть экосистемы **TripleA**.\n\n"
        "🤖 Автоматизация процессов\n"
        "🏝️ Туристические решения\n"
        "🚀 Новые возможности для роста",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "price")
async def price(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 Подписка TripleA Travel:\n\n"
        "• Бесплатно — до 5 поисков/день\n"
        "• Премиум — пока недоступен",
        reply_markup=back_menu(),
    )

@dp.callback_query(F.data == "find_tour")
async def find_tour(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введи название страны, города или бюджет (например: 'туры до 1000 USD'):",
        reply_markup=back_menu()
    )

@dp.callback_query(F.data == "cheap_tours")
async def cheap_tours(callback: types.CallbackQuery):
    tours = await get_latest_tours(limit=5, hours=24, max_price=500)
    if not tours:
        await callback.message.edit_text("⚠️ Дешёвых туров за последние 24 часа не найдено.", reply_markup=back_menu())
        return

    text = "\n\n".join([format_tour(t) for t in tours])
    await callback.message.edit_text(f"🔥 Свежие дешёвые туры:\n\n{text}", reply_markup=back_menu())

# ============ FASTAPI ============
@app.on_event("startup")
async def on_startup():
    init_db()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()
    logging.info("🛑 Бот выключен")

@app.post("/webhook")
async def webhook_handler(request: Request):
    try:
        update = types.Update.model_validate(await request.json())
        await dp.feed_update(bot, update)
    except Exception as e:
        logging.error(f"Ошибка обработки апдейта: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    return {"ok": True}

@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"status": "ok", "service": "TripleA Travel Bot"}
