import os
import re
import logging
import asyncio
from datetime import datetime, timedelta, UTC

from telethon import TelegramClient
from psycopg import connect

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ============ ENV ============
API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_B64 = os.getenv("TG_SESSION_B64")
DATABASE_URL = os.getenv("DATABASE_URL")

CHANNELS = os.getenv("CHANNELS", "@mangotour,@CentralTur_uz,@talismantour").split(",")

if not API_ID or not API_HASH or not SESSION_B64 or not DATABASE_URL:
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_SESSION_B64 и DATABASE_URL в .env")

# ============ КЛИЕНТ ============
client = TelegramClient("collector", API_ID, API_HASH)
client.start()

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tours (
                id SERIAL PRIMARY KEY,
                source TEXT,
                title TEXT,
                price NUMERIC,
                currency TEXT,
                posted_at TIMESTAMP
            );
        """)

init_db()

# ============ УТИЛИТЫ ============
def parse_title_and_price(text: str):
    """
    Парсим название тура и цену
    """
    if not text:
        return None, None, None

    # --- Парсим цену ---
    price_match = re.search(r"(\d{2,6})\s?(USD|\$|EUR|€|СУМ|сум|UZS|₽|руб)", text, re.IGNORECASE)
    price, currency = None, None
    if price_match:
        price = float(price_match.group(1))
        currency = price_match.group(2).upper()
        # Нормализация
        if currency in ["$", "USD"]:
            currency = "USD"
        elif currency in ["€", "EUR"]:
            currency = "EUR"
        elif currency in ["СУМ", "СУМС", "UZS"]:
            currency = "UZS"
        elif currency in ["₽", "РУБ", "RUB"]:
            currency = "RUB"

    # --- Парсим заголовок (название направления) ---
    title = None
    if price_match:
        idx = price_match.start()
        title = text[:idx].strip(" ,.-\n")
    if not title:  # fallback
        lines = text.split("\n")
        title = lines[0][:50] if lines else None

    return title, price, currency

# ============ ОСНОВНОЙ ЦИКЛ ============
async def collect():
    async with client:
        logging.info("✅ Collector запущен")
        since = datetime.now(UTC) - timedelta(hours=24)

        for channel in CHANNELS:
            channel = channel.strip()
            if not channel:
                continue

            logging.info(f"📥 Читаю канал: {channel}")
            async for msg in client.iter_messages(channel, limit=50, offset_date=since):
                if not msg.text:
                    continue

                title, price, currency = parse_title_and_price(msg.text)

                with get_conn() as conn, conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tours (source, title, price, currency, posted_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (channel, title, price, currency, datetime.now(UTC)))

                logging.info(f"💾 Сохранил тур: {title} | {price} {currency}")

        logging.info("♻️ Сборка завершена")

async def scheduler():
    while True:
        try:
            await collect()
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        await asyncio.sleep(900)  # каждые 15 минут

if __name__ == "__main__":
    asyncio.run(scheduler())
