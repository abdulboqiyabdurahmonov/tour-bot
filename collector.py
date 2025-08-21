import os
import re
import logging
import asyncio
from datetime import datetime, timedelta

from telethon import TelegramClient
from psycopg import connect

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ============ ENV ============
API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
BOT_TOKEN = os.getenv("TG_BOT_TOKEN")  # ✅ логинимся как бот
SESSION_NAME = os.getenv("TG_SESSION", "collector_session")
CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "").split(",") if c.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")

if not API_ID or not API_HASH or not BOT_TOKEN or not CHANNELS:
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_BOT_TOKEN и CHANNELS в .env")

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def save_tour(data: dict):
    """Сохраняем тур в PostgreSQL"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tours (country, city, hotel, price, currency, dates, description, source_url, posted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            data.get("posted_at")
        ))

# ============ ПАРСЕР ============
MONTHS = {
    "янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "мая": "05",
    "июн": "06", "июл": "07", "авг": "08", "сен": "09", "сент": "09",
    "окт": "10", "ноя": "11", "дек": "12"
}

def parse_dates(text: str):
    """Извлекаем даты из текста"""
    # 01.09–10.09
    m = re.search(r"(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?", text)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{d1.zfill(2)}.{m1.zfill(2)}.{y1 or datetime.now().year}–{d2.zfill(2)}.{m2.zfill(2)}.{y2 or datetime.now().year}"

    # 15–25 сентября
    m = re.search(r"(\d{1,2})\s?[–\-]\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        return f"{d1.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}–{d2.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}"

    # с 5 по 12 октября
    m = re.search(r"с\s?(\d{1,2})\s?по\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        return f"{d1.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}–{d2.zfill(2)}.{MONTHS[mon[:3].lower()]}.{datetime.now().year}"

    return None

def parse_post(text: str, link: str):
    """Разбор поста (цена, город, отель, валюта, даты)"""
    price_match = re.search(r"(\d{2,6})\s?(USD|EUR|СУМ|сум|руб)", text, re.I)
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Тбилиси)", text)
    hotel_match = re.search(r"(Hotel|Отель|Resort|Inn|Palace|Hilton|Marriott)\s?[^\n]*", text)
    dates_match = parse_dates(text)

    return {
        "country": guess_country(city_match.group(1)) if city_match else None,
        "city": city_match.group(1) if city_match else None,
        "hotel": hotel_match.group(0) if hotel_match else None,
        "price": float(price_match.group(1)) if price_match else None,
        "currency": price_match.group(2).upper() if price_match else None,
        "dates": dates_match,
        "description": text[:500],
        "source_url": link,
        "posted_at": datetime.utcnow()
    }

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

# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    """Один прогон сбора туров"""
    since = datetime.utcnow() - timedelta(hours=24)

    for channel in CHANNELS:
        logging.info(f"📥 Читаю канал: {channel}")
        async for msg in client.iter_messages(channel, limit=50):
            if not msg.text:
                continue
            if msg.date.replace(tzinfo=None) < since:
                break

            data = parse_post(msg.text, f"https://t.me/{channel.strip('@')}/{msg.id}")
            save_tour(data)
            logging.info(f"💾 Сохранил тур: {data}")

async def run_collector():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)  # ✅ запускаем как бот
    logging.info("✅ Collector started")

    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        await asyncio.sleep(900)  # каждые 15 минут

if __name__ == "__main__":
    asyncio.run(run_collector())
