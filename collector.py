import os
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient, events
from psycopg import connect

# ===== Логирование =====
logging.basicConfig(level=logging.INFO)

# ===== ENV =====
API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNELS = os.getenv("CHANNELS", "").split(",")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")

# ===== БД =====
def get_conn():
    return connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
        autocommit=True
    )

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tours (
            id SERIAL PRIMARY KEY,
            channel TEXT,
            country TEXT,
            city TEXT,
            price NUMERIC,
            currency TEXT,
            message TEXT,
            date TIMESTAMP
        )
        """)
    logging.info("✅ Таблица tours инициализирована")

# ===== Обработка сообщений =====
async def handle_message(event, channel):
    text = event.message.message
    date = event.message.date

    # парсим цену
    price, currency = None, None
    for word in text.split():
        if word.isdigit():
            price = int(word)
        if word.upper() in ["USD", "EUR", "SUM", "СУМ"]:
            currency = word.upper()

    # ищем страну и город
    country, city = None, None
    for kw in ["Турция", "Египет", "Таиланд", "ОАЭ", "Вьетнам", "Узбекистан"]:
        if kw.lower() in text.lower():
            country = kw
    for kw in ["Стамбул", "Анталия", "Хургада", "Нячанг", "Бангкок", "Дубай"]:
        if kw.lower() in text.lower():
            city = kw

    # сохраняем в БД
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tours(channel, country, city, price, currency, message, date)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (channel, country, city, price, currency, text, date))

    logging.info(f"💾 Сохранили тур из {channel}: {country} {city} {price}{currency}")

# ===== Основной процесс =====
async def main():
    init_db()
    client = TelegramClient("collector_bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch.strip())
            @client.on(events.NewMessage(chats=entity))
            async def handler(event, ch_name=ch):
                await handle_message(event, ch_name)
            logging.info(f"✅ Подключились к каналу {ch}")
        except Exception as e:
            logging.error(f"❌ Ошибка при подключении к {ch}: {e}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
