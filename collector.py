import os
import re
import logging
import asyncio
from datetime import datetime

from telethon.sessions import StringSession
from telethon import TelegramClient
from psycopg import connect

# >>> SAN: imports
from utils.sanitazer import (
    San, TourDraft, build_tour_key,
    safe_run, RetryPolicy
)
# <<< SAN: imports

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

# >>> SAN: UPSERT с устойчивостью + stable_key
SQL_UPSERT_TOUR = """
INSERT INTO tours(
    country, city, hotel, price, currency, dates, description,
    source_url, posted_at, message_id, source_chat, stable_key
)
VALUES (%(country)s, %(city)s, %(hotel)s, %(price)s, %(currency)s, %(dates)s, %(description)s,
        %(source_url)s, %(posted_at)s, %(message_id)s, %(source_chat)s, %(stable_key)s)
ON CONFLICT (message_id, source_chat) DO UPDATE SET
    country     = EXCLUDED.country,
    city        = EXCLUDED.city,
    hotel       = EXCLUDED.hotel,
    price       = EXCLUDED.price,
    currency    = EXCLUDED.currency,
    dates       = EXCLUDED.dates,
    description = EXCLUDED.description,
    source_url  = EXCLUDED.source_url,
    posted_at   = EXCLUDED.posted_at,
    stable_key  = EXCLUDED.stable_key;
"""

def save_tour(data: dict):
    """Сохраняем тур в PostgreSQL (без фото), устойчиво и идемпотентно."""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(SQL_UPSERT_TOUR, data)
            logging.info(
                f"💾 Сохранил/обновил тур: {data.get('city')} | {data.get('price')} {data.get('currency')} "
                f"(msg={data.get('message_id')}, key={data.get('stable_key')})"
            )
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении тура: {e}")
# <<< SAN: UPSERT


# ============ ПАРСЕР (твои хелперы оставил; встроил San/TourDraft) ============
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


# >>> SAN: единый парсинг через San/TourDraft + твои эвристики
def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime):
    """Разбор поста (без картинок), устойчивый к мусору и форматам."""
    raw = text or ""
    # 1) Жёсткая чистка, чтобы regex-ы не сыпались
    cleaned = San.clean_text(raw)

    # 2) Базовые поля через наш универсальный парсер
    draft = TourDraft.from_raw(cleaned)

    # 3) Город/отель/даты: дополняем твоими правилами
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Тбилиси)", cleaned, re.I)
    city = city_match.group(1) if city_match else None
    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", cleaned)
        city = m.group(1) if m else None

    hotel_match = re.search(r"(Hotel|Отель|Resort|Inn|Palace|Hilton|Marriott)\s?[^\n]*", cleaned)
    hotel = strip_trailing_price_from_hotel(hotel_match.group(0)) if hotel_match else None

    # Если наш простой словарь дат дал пусто — попробуем твою функцию
    dates = draft.dates or parse_dates(cleaned)

    # 4) Цена/валюта — оставляем из draft, при необходимости fallback на старый паттерн
    price, currency = draft.price, draft.currency
    if price is None or currency is None:
        price_match = re.search(
            r'(?:(?:от\s*)?(\d[\d\s.,]{2,}))\s*(USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€)\b|(?:(USD|EUR|\$|€)\s*(\d[\d\s.,]{2,}))',
            cleaned, re.I
        )
        if price_match:
            if price_match.group(1) and price_match.group(2):
                price, currency = _amount_to_float(price_match.group(1)), price_match.group(2)
            elif price_match.group(3) and price_match.group(4):
                currency, price = price_match.group(3), _amount_to_float(price_match.group(4))

    # Нормализуем валюту
    if currency:
        cu = str(currency).strip().upper()
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
        low = cleaned.lower()
        if "сум" in low or "uzs" in low:
            currency = "UZS"
        elif "eur" in low or "€" in low:
            currency = "EUR"
        elif "usd" in low or "$" in low:
            currency = "USD"

    # 5) Стабильный ключ — чтобы не ловить дубли и не падать на редактированиях
    stable_key = build_tour_key(
        source_chat=chat,
        message_id=msg_id,
        city=city or draft.city or "",
        hotel=hotel or draft.hotel or "",
        price=(price, currency) if price else None
    )

    return {
        "country": guess_country(city) if city else None,
        "city": city,
        "hotel": hotel or draft.hotel,
        "price": price,
        "currency": currency,
        "dates": dates,
        "description": cleaned[:500],
        "source_url": link,
        "posted_at": posted_at.replace(tzinfo=None),
        "message_id": msg_id,
        "source_chat": chat,
        "stable_key": stable_key,
    }
# <<< SAN: единый парсинг


# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    for channel in CHANNELS:
        channel = channel.strip()
        if not channel:
            continue
        logging.info(f"📥 Читаю канал: {channel}")
        # >>> SAN: устойчивый прогон по сообщениям
        async for msg in client.iter_messages(channel, limit=50):
            if not msg.text:
                continue
            async def _store_one():
                data = parse_post(
                    msg.text,
                    f"https://t.me/{channel.strip('@')}/{msg.id}",
                    msg.id,
                    channel,
                    msg.date
                )
                save_tour(data)

            # ретраи с backoff: если внезапно БД дернулась — проглотим и пойдём дальше
            await safe_run(_store_one, RetryPolicy(attempts=5, base_delay=0.25, max_delay=3.0))
        # <<< SAN: устойчивый прогон

async def run_collector():
    client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)
    await client.start()
    logging.info("✅ Collector запущен")
    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        # >>> SAN: джиттер чтобы не биться с rate-limit и кроноподобными задачами
        await asyncio.sleep(900 + int(10 * (os.getpid() % 3)))
        # <<< SAN: джиттер

if __name__ == "__main__":
    asyncio.run(run_collector())
