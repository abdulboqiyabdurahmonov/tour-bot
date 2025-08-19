import os
import re
import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from psycopg.rows import dict_row
from db_init import get_conn, init_db

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("collector")

# ---------- ENV ----------
API_ID = int(os.environ["TELEGRAM_API_ID"])          # my.telegram.org
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_B64 = os.environ["SESSION_B64"]              # строка StringSession
CHANNELS = [x.strip() for x in os.getenv("CHANNELS", "").split(",") if x.strip()]

# ---------- DB helpers ----------
def upsert_tour(row: dict):
    """
    row = {
      country, city, hotel, price, currency, dates, description,
      source_chat, message_id, posted_at, source_url
    }
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tours (country, city, hotel, price, currency, dates, description,
                               source_chat, message_id, posted_at, source_url)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_chat, message_id)
            DO UPDATE SET
                price = EXCLUDED.price,
                currency = EXCLUDED.currency,
                dates = EXCLUDED.dates,
                description = EXCLUDED.description
        """, (
            row.get("country"), row.get("city"), row.get("hotel"),
            row.get("price"), row.get("currency"), row.get("dates"), row.get("description"),
            row.get("source_chat"), row.get("message_id"),
            row.get("posted_at"), row.get("source_url"),
        ))
        conn.commit()

def get_last_id(chat: str) -> int | None:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT last_id FROM checkpoints WHERE chat=%s", (chat,))
        r = cur.fetchone()
        return r["last_id"] if r else None

def set_last_id(chat: str, last_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO checkpoints(chat, last_id)
            VALUES (%s,%s)
            ON CONFLICT (chat) DO UPDATE SET last_id=EXCLUDED.last_id
        """, (chat, last_id))
        conn.commit()

# ---------- parser ----------
PRICE_RX = re.compile(
    r"(?P<usd>\d{2,7})\s*(\$|usd|долл?)"
    r"|(?P<rub>\d{2,7})\s*(₽|rub|руб)"
    r"|(?P<uzs>\d{3,12})\s*(сум|so['`]?m|uzs)",
    re.I,
)

def parse_post(text: str) -> dict | None:
    """
    Парсим пост: ищем цену (USD, RUB, UZS) + эвристика по странам/городам.
    """
    if not text:
        return None

    m = PRICE_RX.search(text)
    if not m:
        return None

    price = None
    currency = None
    if m.group("usd"):
        price, currency = m.group("usd"), "USD"
    elif m.group("rub"):
        price, currency = m.group("rub"), "RUB"
    elif m.group("uzs"):
        price, currency = m.group("uzs"), "UZS"

    if not price:
        return None

    try:
        price = int(price.replace(" ", ""))   # <<< теперь число
    except:
        return None

    # эвристики по странам/городам
    country = None
    city = None
    low = text.lower()
    if "анталь" in low: country, city = "Турция", "Анталья"
    if "хургад" in low: country, city = "Египет", "Хургада"
    if "дубай"  in low: country, city = "ОАЭ", "Дубай"

    return {
        "country": country,
        "city": city,
        "hotel": None,
        "price": price,
        "currency": currency,
        "dates": None,
        "description": text[:900],
    }

# ---------- обработка поста ----------
async def process_message(msg, chat):
    try:
        text = msg.message or ""
        parsed = parse_post(text)
        if not parsed:
            return

        row = {
            **parsed,
            "source_chat": f"@{getattr(chat, 'username', None)}" if getattr(chat, "username", None) else (getattr(chat, "title", None) or str(chat.id)),
            "message_id": int(msg.id),
            "posted_at": datetime.fromtimestamp(msg.date.timestamp(), tz=timezone.utc),
            "source_url": f"https://t.me/{chat.username}/{msg.id}" if getattr(chat, "username", None) else None,
        }
        upsert_tour(row)
        set_last_id(row["source_chat"], row["message_id"])

        logger.info(f"✨ saved: {row['source_chat']}#{row['message_id']} price={row['price']} {row['currency']}")

    except Exception as e:
        logger.error(f"process_message error: {e}")

# ---------- Telethon ----------
client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNELS if CHANNELS else None))
async def handler(event):
    chat = await event.get_chat()
    await process_message(event.message, chat)

async def catch_up_history():
    """
    При старте подтягиваем историю после last_id для каждого канала.
    """
    for ch in CHANNELS:
        try:
            last_id = get_last_id(ch) or 0
            async for msg in client.iter_messages(ch, limit=200, min_id=last_id):
                chat = await client.get_entity(ch)
                await process_message(msg, chat)
        except Exception as e:
            logger.error(f"history for {ch} error: {e}")

async def main():
    init_db()
    logger.info("Connecting…")
    await client.start()
    logger.info("Connected.")
    await catch_up_history()
    logger.info("Listening…")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
