import os
import re
import logging
import asyncio
from datetime import datetime, timedelta

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
CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "").split(",") if c.strip()]  # пример: @tour1,@tour2
DATABASE_URL = os.getenv("DATABASE_URL")

# >>> SAN: настройки актуальности / нагрузки
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "80"))                 # сколько сообщений на канал за проход
MAX_POST_AGE_DAYS = int(os.getenv("MAX_POST_AGE_DAYS", "45"))     # игнорировать посты старше N дней
REQUIRE_PRICE = os.getenv("REQUIRE_PRICE", "1") == "1"            # если True — без цены не сохраняем
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))                   # размер батча для upsert
SLEEP_BASE = int(os.getenv("SLEEP_BASE_SEC", "900"))              # базовый интервал между проходами
# <<< SAN: настройки

if not API_ID or not API_HASH or not SESSION_B64 or not CHANNELS:
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_SESSION_B64 и CHANNELS в .env")

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

# >>> SAN: UPSERT (named params) + bulk
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

def save_tours_bulk(rows: list[dict]):
    """Батч-апсерты: быстрее и устойчивее под нагрузкой."""
    if not rows:
        return
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.executemany(SQL_UPSERT_TOUR, rows)
        logging.info(f"💾 Сохранил/обновил батч: {len(rows)} шт.")
    except Exception as e:
        # На всякий случай fallback по одному — чтобы не потерять всё из-за одного кривого поста
        logging.warning(f"⚠️ Bulk upsert failed, fallback to single. Reason: {e}")
        for r in rows:
            try:
                with get_conn() as conn, conn.cursor() as cur:
                    cur.execute(SQL_UPSERT_TOUR, r)
            except Exception as ee:
                logging.error(f"❌ Ошибка при сохранении тура (msg={r.get('message_id')}): {ee}")
# <<< SAN: UPSERT


# ============ ПАРСЕР (улучшенные даты RU/UZ) ============
# RU: сентябрь/сент., UZ (кирилл): сентябр/сент, UZ (лат): sentabr/sent.
MONTHS_MAP = {
    # RU краткие
    "янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "мая": "05",
    "июн": "06", "июл": "07", "авг": "08", "сен": "09", "сент": "09", "окт": "10", "ноя": "11", "дек": "12",
    # RU полные/падежи
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04", "июнь": "06", "июль": "07",
    "август": "08", "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
    # UZ кириллица (основы)
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04", "май": "05", "июн": "06", "июл": "07",
    "август": "08", "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12",
    # UZ латиница
    "yanv": "01", "fevral": "02", "mart": "03", "aprel": "04", "may": "05",
    "iyun": "06", "iyul": "07", "avgust": "08", "sentabr": "09", "sent": "09",
    "oktabr": "10", "noyabr": "11", "dekabr": "12",
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

def _month_to_mm(token: str | None) -> str | None:
    if not token:
        return None
    t = token.strip().lower()
    # режем до первых 5 символов чтобы матчить «сентябр/сентября/sentabr»
    for k, mm in MONTHS_MAP.items():
        if t.startswith(k):
            return mm
    return None

def parse_dates_strict(text: str) -> str | None:
    """Более строгий разбор: поддержка RU/UZ (кирил/лат), интервалов и смешанных форматов."""
    t = text.strip()

    # 1) dd.mm(.yy|yyyy)?–dd.mm(.yy|yyyy)?
    m = re.search(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?", t)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{_mk_date(d1, m1, y1)}–{_mk_date(d2, m2, y2 or y1)}"

    # 2) dd–dd mon (ru/uz)
    m = re.search(r"(\d{1,2})\s?[–\-]\s?(\d{1,2})\s+([A-Za-zА-Яа-яЁёўғқҳ]+)\w*", t, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = _month_to_mm(mon)
        if mm:
            y = datetime.now().year
            return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

    # 3) с d по d mon (ru/uz)
    m = re.search(r"(?:с|бу)\s?(\d{1,2})\s?(?:по|то)\s?(\d{1,2})\s+([A-Za-zА-Яа-яЁёўғқҳ]+)\w*", t, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = _month_to_mm(mon)
        if mm:
            y = datetime.now().year
            return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

    # 4) одиночная дата dd.mm(.yy|yyyy)? или dd mon
    m = re.search(r"\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\b", t)
    if m:
        d, mth, y = m.groups()
        return _mk_date(d, mth, y)
    m = re.search(r"\b(\d{1,2})\s+([A-Za-zА-Яа-яЁёўғқҳ]+)\w*", t)
    if m:
        d, mon = m.groups()
        mm = _month_to_mm(mon)
        if mm:
            y = datetime.now().year
            return _mk_date(d, mm, y)

    return None


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
    if s.count(',') == 1 and s.count('.') == 0:
        s = s.replace(',', '.')
    else:
        s = s.replace(',', '')
    try:
        return float(s)
    except Exception:
        return None


# >>> SAN: единый парсинг через San/TourDraft + строгие даты + фильтры актуальности
def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime):
    """Разбор поста (без картинок), устойчивый к мусору и форматам."""
    raw = text or ""
    cleaned = San.clean_text(raw)

    draft = TourDraft.from_raw(cleaned)

    # Город/отель — твои эвристики
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Тбилиси)", cleaned, re.I)
    city = city_match.group(1) if city_match else None
    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", cleaned)
        city = m.group(1) if m else None

    hotel_match = re.search(r"(Hotel|Отель|Resort|Inn|Palace|Hilton|Marriott)\s?[^\n]*", cleaned)
    hotel = strip_trailing_price_from_hotel(hotel_match.group(0)) if hotel_match else None

    # Даты: строгий разбор RU/UZ
    dates = parse_dates_strict(cleaned) or draft.dates

    # Цена/валюта: первое — из draft, иначе fallback
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

    # Валюта — нормализуем
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

    stable_key = build_tour_key(
        source_chat=chat,
        message_id=msg_id,
        city=city or draft.city or "",
        hotel=hotel or draft.hotel or "",
        price=(price, currency) if price else None
    )

    payload = {
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
    return payload
# <<< SAN: единый парсинг


# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    """Один проход по всем каналам с батч-сохранением и фильтрами актуальности."""
    now = datetime.utcnow()
    cutoff = now - timedelta(days=MAX_POST_AGE_DAYS)

    for channel in CHANNELS:
        logging.info(f"📥 Читаю канал: {channel}")
        batch: list[dict] = []

        async for msg in client.iter_messages(channel, limit=FETCH_LIMIT):
            if not msg.text:
                continue

            # игнорируем слишком старые посты
            if msg.date and msg.date.replace(tzinfo=None) < cutoff:
                continue

            def _make():
                data = parse_post(
                    msg.text,
                    f"https://t.me/{channel.strip('@')}/{msg.id}",
                    msg.id,
                    channel,
                    msg.date
                )
                # если требуется цена — отбрасываем без цены
                if REQUIRE_PRICE and (data.get("price") is None or data.get("currency") is None):
                    return None
                return data

            # парсинг и отбракованные посты не останавливают поток
            data = _make()
            if data:
                batch.append(data)

            # батч-сброс
            if len(batch) >= BATCH_SIZE:
                await safe_run(lambda: asyncio.to_thread(save_tours_bulk, batch.copy()),
                               RetryPolicy(attempts=4, base_delay=0.25, max_delay=2.0))
                batch.clear()

        # остатки батча после канала
        if batch:
            await safe_run(lambda: asyncio.to_thread(save_tours_bulk, batch.copy()),
                           RetryPolicy(attempts=4, base_delay=0.25, max_delay=2.0))
            batch.clear()

async def run_collector():
    client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)
    await client.start()
    logging.info("✅ Collector запущен")
    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        # лёгкий джиттер, чтобы не попадать в ровные минуты и разойтись с другими процессами
        await asyncio.sleep(SLEEP_BASE + int(10 * (os.getpid() % 3)))

if __name__ == "__main__":
    asyncio.run(run_collector())
