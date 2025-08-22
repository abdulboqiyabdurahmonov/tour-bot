import os
import re
import logging
import asyncio
from datetime import datetime, timedelta

from telethon.sessions import StringSession
from telethon import TelegramClient
from psycopg import connect
from psycopg.rows import dict_row

from fastapi import FastAPI
import uvicorn

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ============ ENV ============
API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_B64 = os.getenv("TG_SESSION_B64")
CHANNELS = os.getenv("CHANNELS", "").split(",")  # пример: @tour1,@tour2
DATABASE_URL = os.getenv("DATABASE_URL")

if not API_ID or not API_HASH or not SESSION_B64 or not CHANNELS:
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_SESSION_B64 и CHANNELS в .env")

# ============ FASTAPI ============
app = FastAPI()

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def save_tour(data: dict):
    """Сохраняем тур в PostgreSQL"""
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO tours 
                (country, city, hotel, price, currency, dates, description, source_url, posted_at, message_id, source_chat)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
            """, (
                data.get("country"),
                data.get("city"),
                data.get("hotel"),
                data.get("price"),
                data.get("currency"),
                data.get("dates"),
                data.get("description"),
                data.get("source_url"),
                data.get("posted_at"),
                data.get("message_id"),
                data.get("source_chat"),
            ))
            logging.info(f"💾 Сохранил тур: {data.get('country')} | {data.get('city')} | {data.get('price')} {data.get('currency')}")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении тура: {e}")

def fetch_tours(query: str):
    """Поиск туров только за последние 24 часа"""
    since = datetime.utcnow() - timedelta(hours=24)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT country, city, hotel, price, currency, dates, description, source_url, posted_at
            FROM tours
            WHERE posted_at >= %s
              AND (LOWER(country) LIKE %s OR LOWER(city) LIKE %s OR LOWER(description) LIKE %s)
            ORDER BY posted_at DESC
            LIMIT 10;
        """, (
            since,
            f"%{query.lower()}%",
            f"%{query.lower()}%",
            f"%{query.lower()}%"
        ))
        return cur.fetchall()

# ============ ПАРСЕР ============
MONTHS = {
    "янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "мая": "05",
    "июн": "06", "июл": "07", "авг": "08", "сен": "09", "сент": "09",
    "окт": "10", "ноя": "11", "дек": "12"
}

def parse_dates(text: str):
    """Извлекаем даты из текста"""
    m = re.search(r"(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?", text)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{d1.zfill(2)}.{m1.zfill(2)}.{y1 or datetime.now().year}–{d2.zfill(2)}.{m2.zfill(2)}.{y2 or datetime.now().year}"

    m = re.search(r"(\d{1,2})\s?[–\-]\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        return f"{d1.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}–{d2.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}"

    m = re.search(r"с\s?(\d{1,2})\s?по\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        return f"{d1.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}–{d2.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}"

    return None

def guess_country(city: str):
    mapping = {
        "Нячанг": "Вьетнам",
        "Анталья": "Турция",
        "Пхукет": "Таиланд",
        "Дубай": "ОАЭ",
        "Бали": "Индонезия",
        "Тбилиси": "Грузия"
    }
    return mapping.get(city, None)

def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime):
    """Разбор поста"""
    price_match = re.search(
        r"(?:(\d{2,6})(?:\s?)(USD|EUR|СУМ|сум|руб|\$|€))|(?:(USD|EUR|\$|€)\s?(\d{2,6}))",
        text, re.I
    )
    price, currency = None, None
    if price_match:
        if price_match.group(1) and price_match.group(2):  
            price, currency = price_match.group(1), price_match.group(2)
        elif price_match.group(3) and price_match.group(4):  
            price, currency = price_match.group(4), price_match.group(3)

    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Тбилиси)", text, re.I)
    city = city_match.group(1) if city_match else None

    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", text)
        city = m.group(1) if m else None

    hotel_match = re.search(r"(Hotel|Отель|Resort|Inn|Palace|Hilton|Marriott)\s?[^\n]*", text)
    dates_match = parse_dates(text)

    return {
        "country": guess_country(city) if city else None,
        "city": city,
        "hotel": hotel_match.group(0) if hotel_match else None,
        "price": float(price) if price else None,
        "currency": currency.upper() if currency else None,
        "dates": dates_match,
        "description": text[:500],
        "source_url": link,
        "posted_at": posted_at.replace(tzinfo=None),  # ← реальная дата сообщения
        "message_id": msg_id,
        "source_chat": chat
    }

# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    for channel in CHANNELS:
        if not channel.strip():
            continue
        logging.info(f"📥 Читаю канал: {channel}")
        async for msg in client.iter_messages(channel.strip(), limit=50):
            if not msg.text:
                continue

            data = parse_post(
                msg.text,
                f"https://t.me/{channel.strip('@')}/{msg.id}",
                msg.id,
                channel.strip('@'),
                msg.date
            )
            save_tour(data)

async def run_collector():
    client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)
    await client.start()
    logging.info("✅ Collector запущен")

    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        await asyncio.sleep(900)  # каждые 15 минут

# ============ FASTAPI ENDPOINTS ============
@app.get("/")
async def root():
    return {"status": "ok", "message": "Collector is running"}

@app.get("/search")
async def search_tours(q: str):
    return fetch_tours(q)

# ============ MAIN ============
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(run_collector())
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
