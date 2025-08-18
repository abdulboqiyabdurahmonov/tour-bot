import os
import re
import asyncio
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from psycopg.rows import dict_row
from db_init import get_conn, init_db

# ---------- ENV ----------
API_ID = int(os.environ["TELEGRAM_API_ID"])          # my.telegram.org
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_B64 = os.environ["SESSION_B64"]              # строка StringSession
# список каналов через запятую: @tour_deals,@agentuz или числовые id
CHANNELS = [x.strip() for x in os.getenv("CHANNELS", "").split(",") if x.strip()]

# ---------- DB helpers ----------
def upsert_tour(row: dict):
    """
    row = {
      country, city, hotel, price, dates, description,
      source_chat, message_id, posted_at, source_url
    }
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tours (country, city, hotel, price, dates, description,
                               source_chat, message_id, posted_at, source_url)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_chat, message_id)
            DO UPDATE SET
                price = EXCLUDED.price,
                dates = EXCLUDED.dates,
                description = EXCLUDED.description
        """, (
            row.get("country"), row.get("city"), row.get("hotel"),
            row.get("price"), row.get("dates"), row.get("description"),
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

# ---------- very simple parser ----------
PRICE_RX = re.compile(r"(\d{2,5})\s*\$|\$\s*(\d{2,5})", re.I)

def parse_post(text: str) -> dict | None:
    """
    Простейший разбор: ищем цену в $, вытаскиваем страну/город по ключевым словам.
    При желании здесь усложним позже.
    """
    if not text:
        return None

    m = PRICE_RX.search(text)
    if not m:
        return None
    price = m.group(1) or m.group(2)
    try:
        price = int(price)
    except:
        return None

    # грубые эвристики
    country = None
    city = None
    if "анталь" in text.lower(): country, city = "Турция", "Анталья"
    if "хургад" in text.lower(): country, city = "Египет", "Хургада"
    if "дубай"  in text.lower(): country, city = "ОАЭ", "Дубай"

    # даты не парсим точно — оставим как весь текст для MVP
    return {
        "country": country,
        "city": city,
        "hotel": None,
        "price": price,
        "dates": None,
        "description": text[:900],  # чтобы не раздувать
    }

# ---------- Telethon ----------
client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNELS if CHANNELS else None))
async def handler(event):
    try:
        msg = event.message
        chat = (await event.get_chat())
        chat_username = getattr(chat, "username", None)
        chat_title = getattr(chat, "title", None)

        text = msg.message or ""
        parsed = parse_post(text)
        if not parsed:
            return

        row = {
            **parsed,
            "source_chat": f"@{chat_username}" if chat_username else (chat_title or str(chat.id)),
            "message_id": int(msg.id),
            "posted_at": datetime.fromtimestamp(msg.date.timestamp(), tz=timezone.utc),
            "source_url": f"https://t.me/{chat_username}/{msg.id}" if chat_username else None,
        }
        upsert_tour(row)

        # передвигаем чекпоинт
        set_last_id(row["source_chat"], row["message_id"])
        print(f"✨ saved: {row['source_chat']}#{row['message_id']} price={row['price']}")

    except Exception as e:
        print("handler error:", e)

async def catch_up_history():
    """
    При старте подтягиваем историю после last_id для каждого канала.
    """
    for ch in CHANNELS:
        try:
            last_id = get_last_id(ch) or 0
            # берём последние 200 сообщений поверхностно
            async for msg in client.iter_messages(ch, limit=200, min_id=last_id):
                fake_event = type("E", (), {"message": msg, "get_chat": lambda: client.get_entity(ch)})
                await handler(fake_event)
        except Exception as e:
            print(f"history for {ch} error:", e)

async def main():
    init_db()
    print("Connecting…")
    await client.start()
    print("Connected.")
    await catch_up_history()
    print("Listening…")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
