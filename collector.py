# -*- coding: utf-8 -*-
"""
collector.py — улучшенная версия с точным извлечением отелей/цен/дат из «свалочного» текста.

Главное:
- Жёсткий анти-шум по гео: не путаем остров/город/регион с отелем.
- N‑gram по заглавным словам + суффиксы-«маркеры отелей» + бренд‑хинты.
- Поддержка списков: "Rixos Premium, Titanic Deluxe, Concorde ..." → несколько отелей.
- Строгий парсинг дат для RU/UZ (кириллица/латиница) и форматов ("12–19 сент", "12.09–19.09", "с 12 по 19 сент").
- Аккуратное извлечение цены/валюты, нормализация валют.
- Батч‑апсерты и фильтры актуальности сохранены.

Интеграция в существующий бот не требует изменений БД (схема из db_init.py подходит).
"""

from __future__ import annotations
import os
import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

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


# ============ СЛОВАРИ/РЕГЕКСЫ ДЛЯ ТОЧНОГО ИЗВЛЕЧЕНИЯ ============
# Суффиксы/маркеры отельных названий (ru/uz/en)
WHITELIST_SUFFIXES = [
    # EN
    " hotel", " resort", " inn", " lodge", " suites", " villa", " villas", " bungalow", " bungalows",
    " palace", " spa", " beach", " residence", " residences", " apartments", " apart", " apart-hotel", " aparthotel",
    " guesthouse", " boutique", " camp", " deluxe", " premium",
    # RU
    " отель", " гостиница", " санаторий", " пансионат", " вилла", " резиденс", " резорт",
    # UZ/TR
    " mehmonxona", " otel", " oteli",
]

# Бренды/цепочки — усиливают уверенность
BRAND_HINTS = [
    "rixos", "titanic", "voyage", "miracle", "concorde", "arcanus", "adam & eve", "maxx royal",
    "barut", "limak", "granada", "akra", "cornelia", "gloria", "susesi",
    "delphin", "alva donna", "paloma", "ic hotels", "kaya", "swandor", "regnum", "seginus",
    "hilton", "marriott", "sheraton", "radisson", "novotel", "mercure", "fairmont", "four seasons",
]

# Токены, которые не должны трактоваться как отели (гео/города/страны/общие слова)
BLACKLIST_TOKENS = [
    # Гео-общее
    "island", "atoll", "archipelago", "peninsula", "bay", "gulf", "lagoon",
    # RU/UZ популярные гео
    "остров", "атолл", "залив", "лагуна", "полуостров", "курорт", "пляж", "побережье",
    "турция", "египет", "оаэ", "оае", "таиланд", "узбекистан", "малдивы", "малдив", "черногория",
    "анталия", "алания", "бодрум", "кемер", "сиде", "белек", "шарм", "хургада", "дахааб", "марса алам",
    # EN популярные острова/курорты
    "bali", "phuket", "samui", "lombok", "zanzibar", "goa", "antalya", "alanya", "kemer", "bodrum",
    # Общие
    "центр", "парк", "аэропорт", "рынок", "молл", "набережная", "downtown", "airport",
]

# Небольшой справочник «опасных» однословных гео — выключаем как одиночные отели
KNOWN_GAZETTEER = {
    "bali", "phuket", "samui", "zanzibar", "goa", "antalya", "alanya", "kemer", "side", "belek",
    "dubai", "sharm", "hurghada", "dahab", "bodrum", "istanbul", "izmir", "batumi",
    "tashkent", "samarkand", "bukhara",
}

# Паттерны
PRICE_RE = re.compile(r"(?P<cur>\$|usd|eur|€|сом|сум|uzs|руб|₽|aed|د\.إ)\s*(?P<amt>[\d\s.,]{2,})|(?P<amt2>[\d\s.,]{2,})\s*(?P<cur2>\$|usd|eur|€|сом|сум|uzs|руб|₽|aed)", re.I)
NIGHTS_RE = re.compile(r"(?P<n>\d{1,2})\s*(ноч[еи]|ni[gh]hts?|kun|gece|gecesi)", re.I)
BOARD_RE = re.compile(r"\b(ai|uai|all\s*inclusive|bb|hb|fb|ro|ob|ultra\s*all)\b", re.I)
DATE_RE = re.compile(r"(\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b|\b\d{1,2}\s*(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\w*\b)", re.I)
SPLIT_RE = re.compile(r"[,/\n•;|]\s*")

# ============ ДАТЫ (RU/UZ) ============
MONTHS_MAP = {
    # RU краткие
    "янв": "01", "фев": "02", "мар": "03", "апр": "04", "май": "05", "мая": "05",
    "июн": "06", "июл": "07", "авг": "08", "сен": "09", "сент": "09", "окт": "10", "ноя": "11", "дек": "12",
    # RU полные/падежи
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04", "июнь": "06", "июль": "07",
    "август": "08", "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
    # UZ кириллица
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04", "май": "05", "июн": "06", "июл": "07",
    "август": "08", "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12",
    # UZ латиница
    "yanv": "01", "fevral": "02", "mart": "03", "aprel": "04", "may": "05",
    "iyun": "06", "iyul": "07", "avgust": "08", "sentabr": "09", "sent": "09",
    "oktabr": "10", "noyabr": "11", "dekabr": "12",
}

def _norm_year(y: Optional[str]) -> int:
    if not y:
        return datetime.now().year
    y = int(y)
    if y < 100:
        y += 2000 if y < 70 else 1900
    return y


def _mk_date(d, m, y) -> str:
    return f"{int(d):02d}.{int(m):02d}.{_norm_year(y):04d}"


def _month_to_mm(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    t = token.strip().lower()
    for k, mm in MONTHS_MAP.items():
        if t.startswith(k):
            return mm
    return None


def parse_dates_strict(text: str) -> Optional[str]:
    """Поддержка: "12–19 сент", "12.09–19.09", "с 12 по 19 сент", одиночные даты."""
    t = text.strip()
    m = re.search(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?", t)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{_mk_date(d1, m1, y1)}–{_mk_date(d2, m2, y2 or y1)}"

    m = re.search(r"(\d{1,2})\s?[–\-]\s?(\d{1,2})\s+([A-Za-zА-Яа-яЁёўғқҳ]+)\w*", t, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = _month_to_mm(mon)
        if mm:
            y = datetime.now().year
            return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

    m = re.search(r"(?:с|бу)\s?(\d{1,2})\s?(?:по|то)\s?(\d{1,2})\s+([A-Za-zА-Яа-яЁёўғқҳ]+)\w*", t, re.I)
    if m:
        d1, d2, mon = m.groups()
        mm = _month_to_mm(mon)
        if mm:
            y = datetime.now().year
            return f"{_mk_date(d1, mm, y)}–{_mk_date(d2, mm, y)}"

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

# ============ ХЕЛПЕРЫ ============

def _amount_to_float(s: Optional[str]) -> Optional[float]:
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


def _is_blacklisted(token: str) -> bool:
    t = token.lower()
    return t in KNOWN_GAZETTEER or any(t == b or t.endswith(b) or b in t for b in BLACKLIST_TOKENS)


def _score_hotel_candidate(text: str) -> float:
    """Скоринг: маркер-суффикс, бренд-хинты, заглавные буквы, штраф за blacklist."""
    t = text.lower()
    score = 0.0
    for suf in WHITELIST_SUFFIXES:
        if t.endswith(suf):
            score += 0.55
            break
    for bh in BRAND_HINTS:
        if bh in t:
            score += 0.25
            break
    if len(text) >= 4 and any(ch.isupper() for ch in text):
        score += 0.1
    toks = re.findall(r"[\w'-]+", t)
    if any(_is_blacklisted(tok) for tok in toks):
        score -= 0.6
    return max(0.0, min(1.0, score))


def _enum_ngrams(line: str, max_len: int = 5) -> List[str]:
    """N‑gram по заглавным словам: 'Rixos Premium Belek', 'Gloria Serenity Resort' и т.п."""
    tokens = re.findall(r"[\w'&.-]+", line)
    caps = [(tok, i) for i, tok in enumerate(tokens) if tok[:1].isupper() or tok.isupper()]
    spans = []
    for _, i in caps:
        for j in range(i + 1, min(i + 1 + max_len, len(tokens) + 1)):
            spans.append(" ".join(tokens[i:j]))
    return spans


def _split_candidates(raw: str) -> List[str]:
    parts = SPLIT_RE.split(raw)
    clean = [re.sub(r"\(.*?\)|\[.*?\]", "", p).strip() for p in parts]
    return [c for c in clean if c]


def strip_trailing_price_from_hotel(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return re.sub(
        r'[\s–-]*(?:от\s*)?\d[\d\s.,]*\s*(?:USD|EUR|UZS|RUB|\$|€)\b.*$',
        '', s, flags=re.I
    ).strip()


def _extract_prices(text: str) -> Tuple[Optional[float], Optional[str]]:
    for m in PRICE_RE.finditer(text):
        g = m.groupdict()
        cur = g.get("cur") or g.get("cur2")
        amt = g.get("amt") or g.get("amt2")
        val = _amount_to_float(amt)
        if val:
            cu = (cur or '').upper()
            if cu in {"$", "US$", "USD$"}:
                cu = "USD"
            elif cu in {"€", "EUR€"}:
                cu = "EUR"
            elif cu in {"UZS", "СУМ", "СУМЫ", "СУМ."}:
                cu = "UZS"
            elif cu in {"РУБ", "РУБ."}:
                cu = "RUB"
            return val, (cu or None)
    return None, None


def _extract_board(text: str) -> Optional[str]:
    m = BOARD_RE.search(text)
    return m.group(0).upper().replace(" ", "") if m else None


# ============ ГЕО/СТРАНА ============
CITY2COUNTRY = {
    "Нячанг": "Вьетнам", "Анталья": "Турция", "Пхукет": "Таиланд",
    "Паттайя": "Таиланд", "Самуи": "Таиланд", "Краби": "Таиланд",
    "Бангкок": "Таиланд", "Дубай": "ОАЭ", "Бали": "Индонезия",
    "Тбилиси": "Грузия", "Шарм": "Египет", "Хургада": "Египет",
}


def guess_country(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    return CITY2COUNTRY.get(city)


# ============ ПАРСИНГ ПОСТА ============

def _extract_hotels(cleaned: str) -> List[str]:
    """Достаём список вероятных отелей из неструктурированного текста.
    Алгоритм: режем по списковым разделителям → n‑gram по заглавным → скорим → фильтруем.
    """
    hotels: List[str] = []
    for block in SPLIT_RE.split(cleaned):
        block = block.strip()
        if not block:
            continue
        ngrams = _enum_ngrams(block)
        candidates = []
        for span in ngrams:
            span_norm = strip_trailing_price_from_hotel(span)
            if not span_norm:
                continue
            score = _score_hotel_candidate(span_norm)
            if score >= 0.6:
                candidates.append((score, span_norm))
        candidates.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
        if candidates:
            top = candidates[0][1]
            toks = re.findall(r"[\w'-]+", top.lower())
            if not any(tok in KNOWN_GAZETTEER for tok in toks):
                hotels.append(top)
    seen = set()
    uniq: List[str] = []
    for h in hotels:
        k = h.lower()
        if k not in seen:
            seen.add(k)
            uniq.append(h)
    return uniq[:5]


def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: "datetime"):
    """Разбор поста (без картинок), устойчивый к мусору и форматам."""
    raw = text or ""
    cleaned = San.clean_text(raw)
    draft = TourDraft.from_raw(cleaned)

    # Город (эвристика)
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Паттайя|Краби|Тбилиси|Шарм|Хургада)", cleaned, re.I)
    city = city_match.group(1) if city_match else (draft.city if getattr(draft, 'city', None) else None)
    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", cleaned)
        city = m.group(1) if m else None

    # Отели (мульти-извлечение)
    hotels = _extract_hotels(cleaned)
    hotel = hotels[0] if hotels else (strip_trailing_price_from_hotel(draft.hotel) if draft.hotel else None)

    # Даты: строгий разбор RU/UZ
    dates = parse_dates_strict(cleaned) or draft.dates

    # Цена/валюта: сначала из draft, иначе fallback
    price, currency = draft.price, draft.currency
    if price is None or currency is None:
        price, currency = _extract_prices(cleaned)

    # Валюта — нормализация, если пустая, пробуем по контексту
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

    # Стабильный ключ (уточним позже для каждого отеля)
    payload_base = {
        "country": guess_country(city) if city else None,
        "city": city,
        "price": price,
        "currency": currency,
        "dates": dates,
        "description": cleaned[:500],
        "source_url": link,
        "posted_at": posted_at.replace(tzinfo=None),
        "message_id": msg_id,
        "source_chat": chat,
    }

    return payload_base, (hotels if hotels else [hotel] if hotel else [])


# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    """Один проход по всем каналам с батч-сохранением и фильтрами актуальности."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MAX_POST_AGE_DAYS)

    for channel in CHANNELS:
        logging.info(f"📥 Читаю канал: {channel}")
        batch: list[dict] = []

        async for msg in client.iter_messages(channel, limit=FETCH_LIMIT):
            if not msg.text:
                continue

            # игнорируем слишком старые посты
            if msg.date and msg.date.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
                continue

            def _make_rows() -> List[dict]:
                base, hotels = parse_post(
                    msg.text,
                    f"https://t.me/{channel.strip('@')}/{msg.id}",
                    msg.id,
                    channel,
                    msg.date
                )
                rows: List[dict] = []
                for h in hotels:
                    if not h:
                        continue
                    row = {
                        **base,
                        "hotel": h,
                    }
                    row["stable_key"] = build_tour_key(
                        source_chat=base["source_chat"],
                        message_id=base["message_id"],
                        city=base.get("city") or "",
                        hotel=h,
                        price=(base.get("price"), base.get("currency")) if base.get("price") else None,
                    )
                    rows.append(row)
                if not rows and not REQUIRE_PRICE:
                    rows.append({**base, "hotel": None, "stable_key": build_tour_key(base["source_chat"], base["message_id"], base.get("city") or "", "", None)})
                return rows

            rows = _make_rows()
            if REQUIRE_PRICE:
                rows = [r for r in rows if (r.get("price") is not None and r.get("currency") is not None)]

            if rows:
                batch.extend(rows)

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
