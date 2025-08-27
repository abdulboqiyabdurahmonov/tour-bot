import os
import re
import logging
import asyncio
from datetime import datetime

from telethon.sessions import StringSession
from telethon import TelegramClient
from psycopg import connect

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

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def save_tour(data: dict):
    """Сохраняем тур в PostgreSQL (без фото)"""
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO tours
                (country, city, hotel, price, currency, dates, description, source_url, posted_at, message_id, source_chat)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (message_id, source_chat) DO NOTHING
                RETURNING id;
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
            inserted = cur.fetchone()
            if inserted:
                logging.info(
                    f"💾 Сохранил тур: {data.get('country')} | {data.get('city')} | {data.get('price')} {data.get('currency')}"
                )
            else:
                logging.info(
                    f"⚠️ Дубликат тура: {data.get('city')} | {data.get('price')} {data.get('currency')} (message_id={data.get('message_id')})"
                )
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении тура: {e}")

# ============ ПАРСЕР ============
MONTHS = {
    "янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "мая": "05",
    "июн": "06", "июл": "07", "авг": "08", "сен": "09", "сент": "09",
    "окт": "10", "ноя": "11", "дек": "12"
}

def _norm_year(y: str | None) -> int:
    if not y:
        return datetime.now().year
    y = int(y)
    if y < 100:
        y += 2000 if y < 70 else 1900
    return y

def _mk_date(d, m, y) -> str:
    return f"{int(d):02d}.{int(m):02d}.{_norm_year(y):04d}"

def parse_dates(text: str) -> str | None:
    text = text.strip()

    # dd.mm(.yy|yyyy)?–dd.mm(.yy|yyyy)?
    m = re.search(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?", text)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{_mk_date(d1, m1, y1)}–{_mk_date(d2, m2, y2 or y1)}"

    # dd–dd mon
    m = re.search(r"(\d{1,2})\s?[–\-]\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = MONTHS[mon[:3].lower()]
        y = datetime.now().year
        return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

    # с d по d mon
    m = re.search(r"с\s?(\d{1,2})\s?по\s?(\d{1,2})\s?(янв|фев|мар|апр|мая|май|июн|июл|авг|сен|сент|окт|ноя|дек)\w*", text, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = MONTHS[mon[:3].lower()]
        y = datetime.now().year
        return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

    return None

def clean_text_basic(s: str | None) -> str | None:
    if not s:
        return s
    s = re.sub(r'[*_`]+', '', s)
    s = s.replace('|', ' ')
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

def strip_trailing_price_from_hotel(s: str | None) -> str | None:
    if not s:
        return s
    return re.sub(
        r'[\s–-]*(?:от\s*)?\d[\d\s.,]*\s*(?:USD|EUR|UZS|RUB|\$|€)\b.*$',
        '',
        s,
        flags=re.I
    ).strip()

def guess_country(city: str | None):
    if not city:
        return None
    mapping = {
        "Нячанг": "Вьетнам", "Анталья": "Турция", "Пхукет": "Таиланд",
        "Паттайя": "Таиланд", "Самуи": "Таиланд", "Краби": "Таиланд",
        "Бангкок": "Таиланд", "Дубай": "ОАЭ", "Бали": "Индонезия",
        "Тбилиси": "Грузия",
    }
    return mapping.get(city, None)

def _amount_to_float(s: str | None) -> float | None:
    if not s:
        return None
    s = s.replace(' ', '').replace('\xa0', '')
    # запятая как десятичный
    if s.count(',') == 1 and s.count('.') == 0:
        s = s.replace(',', '.')
    else:
        s = s.replace(',', '')
    try:
        return float(s)
    except Exception:
        return None

def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime):
    """Разбор поста (без картинок)"""
    # цена + валюта: "от 799$", "799 USD", "$ 799", "EUR 1 099"
    price_match = re.search(
        r'(?:(?:от\s*)?(\d[\d\s.,]{2,}))\s*(USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€)\b|(?:(USD|EUR|\$|€)\s*(\d[\d\s.,]{2,}))',
        text, re.I
    )
    price, currency = None, None
    if price_match:
        if price_match.group(1) and price_match.group(2):
            price, currency = price_match.group(1), price_match.group(2)
        elif price_match.group(3) and price_match.group(4):
            currency, price = price_match.group(3), price_match.group(4)

    # нормализация валюты
    if currency:
        cu = currency.strip().upper()
        if cu in {"$", "US$", "USD$"}:
            currency = "USD"
        elif cu in {"€", "EUR€"}:
            currency = "EUR"
        elif cu in {"UZS", "СУМ", "СУМЫ", "СУМ."}:
            currency = "UZS"
        elif cu in {"RUB", "РУБ", "РУБ."}:
            currency = "RUB"
        else:
            currency = cu
    else:
        low = text.lower()
        if "сум" in low or "uzs" in low:
            currency = "UZS"
        elif "eur" in low or "€" in low:
            currency = "EUR"
        elif "usd" in low or "$" in low:
            currency = "USD"

    # город/отель/даты
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
        "hotel": strip_trailing_price_from_hotel(hotel_match.group(0)) if hotel_match else None,
        "price": _amount_to_float(price),
        "currency": currency,
        "dates": dates_match,
        "description": text[:500],
        "source_url": link,
        "posted_at": posted_at.replace(tzinfo=None),
        "message_id": msg_id,
        "source_chat": chat
    }

# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    for channel in CHANNELS:
        channel = channel.strip()
        if not channel:
            continue
        logging.info(f"📥 Читаю канал: {channel}")
        async for msg in client.iter_messages(channel, limit=50):
            if not msg.text:
                continue
            data = parse_post(
                msg.text,
                f"https://t.me/{channel.strip('@')}/{msg.id}",
                msg.id,
                channel,
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

if __name__ == "__main__":
    asyncio.run(run_collector())
