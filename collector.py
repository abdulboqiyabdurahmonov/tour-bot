# -*- coding: utf-8 -*-
"""
collector.py — надёжный коллектор постов из телеграм-каналов тур-операторов с
поддержкой редактированных сообщений (MessageEdited).

Что делает:
- Читает каналы через Telethon (StringSession пользователя).
- Парсит «свалочный» текст: отели (n-gram по заглавным), даты (RU/UZ), цену+валюту, питание (board), "включено" (includes).
- Фильтрует «опасные» гео/топонимы, не путая их с отелями.
- Пишет в таблицу tours (upsert) + создаёт недостающие колонки (board/includes), индексы и чекпоинты.
- Чекпоинты по каналам (collect_checkpoints) — обрабатывает только новые сообщения.
- Батч-апсерты (executemany) + устойчивые ретраи (safe_run/RetryPolicy).
- ⚡ Новое: ловит edits (events.MessageEdited), перепарсивает и бережно обновляет запись.

ENV (обязательные):
  DATABASE_URL
  TG_API_ID
  TG_API_HASH
  TG_SESSION_B64         # StringSession пользователя
  CHANNELS=@ch1,@ch2     # через запятую (можно t.me/..., можно @...)

ENV (опциональные):
  FETCH_LIMIT=200
  MAX_POST_AGE_DAYS=60
  REQUIRE_PRICE=1        # 0 — сохранять и без цены (не рекомендуется)
  BATCH_SIZE=50
  SLEEP_BASE_SEC=900
"""

from __future__ import annotations

import os
import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict

from telethon.sessions import StringSession
from telethon import TelegramClient, events
from psycopg import connect
from psycopg.rows import dict_row

# внешние утилиты проекта
from utils.sanitazer import (
    San, TourDraft, build_tour_key,
    safe_run, RetryPolicy
)

# ======================= ЛОГИ =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ======================= ENV =======================
API_ID = int(os.getenv("TG_API_ID", "0") or 0)
API_HASH = os.getenv("TG_API_HASH") or ""
SESSION_B64 = os.getenv("TG_SESSION_B64") or ""
DATABASE_URL = os.getenv("DATABASE_URL") or ""
CHANNELS_RAW = os.getenv("CHANNELS", "")

FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "80"))
MAX_POST_AGE_DAYS = int(os.getenv("MAX_POST_AGE_DAYS", "45"))
REQUIRE_PRICE = os.getenv("REQUIRE_PRICE", "1") == "1"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
SLEEP_BASE = int(os.getenv("SLEEP_BASE_SEC", "900"))

if not (API_ID and API_HASH and SESSION_B64 and DATABASE_URL and CHANNELS_RAW.strip()):
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_SESSION_B64, DATABASE_URL и CHANNELS в .env")

def _normalize_channel(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    s = s.replace("https://t.me/", "@").replace("t.me/", "@")
    if not s.startswith("@") and s.isalnum():
        s = "@" + s
    return s

CHANNELS: List[str] = [_normalize_channel(c) for c in CHANNELS_RAW.split(",") if _normalize_channel(c)]

# Для корректной обработки edits держим соответствия chat_id <-> '@username'
CH_ID2NAME: Dict[int, str] = {}
CH_NAME2ID: Dict[str, int] = {}

# ======================= БД =======================
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def ensure_schema_and_indexes():
    """Гарантируем всё нужное в БД один раз при запуске."""
    with get_conn() as conn, conn.cursor() as cur:
        # tours: дополнительные колонки
        cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS board TEXT;")
        cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS includes TEXT;")
        # уникальность по источнику+сообщению
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS tours_src_msg_uidx
            ON tours (source_chat, message_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS tours_posted_at_idx
            ON tours (posted_at DESC);
        """)
        # чекпоинты по каналам
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collect_checkpoints (
                source_chat TEXT PRIMARY KEY,
                last_msg_id BIGINT NOT NULL DEFAULT 0,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

def _get_cp(chat: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT last_msg_id FROM collect_checkpoints WHERE source_chat=%s;", (chat,))
        row = cur.fetchone()
        return int(row["last_msg_id"]) if row and row["last_msg_id"] else 0

def _set_cp(chat: str, last_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO collect_checkpoints(source_chat, last_msg_id)
            VALUES (%s, %s)
            ON CONFLICT (source_chat) DO UPDATE SET
                last_msg_id=EXCLUDED.last_msg_id,
                updated_at=now();
        """, (chat, int(last_id)))

# ======================= UPSERT/SELECT =======================
SQL_UPSERT_TOUR = """
INSERT INTO tours(
    country, city, hotel, price, currency, dates, description,
    source_url, posted_at, message_id, source_chat, stable_key,
    board, includes
)
VALUES (
    %(country)s, %(city)s, %(hotel)s, %(price)s, %(currency)s, %(dates)s, %(description)s,
    %(source_url)s, %(posted_at)s, %(message_id)s, %(source_chat)s, %(stable_key)s,
    %(board)s, %(includes)s
)
ON CONFLICT (message_id, source_chat) DO UPDATE SET
    country     = COALESCE(EXCLUDED.country, tours.country),
    city        = COALESCE(EXCLUDED.city, tours.city),
    hotel       = COALESCE(EXCLUDED.hotel, tours.hotel),
    price       = COALESCE(EXCLUDED.price, tours.price),
    currency    = COALESCE(EXCLUDED.currency, tours.currency),
    dates       = COALESCE(EXCLUDED.dates, tours.dates),
    description = COALESCE(EXCLUDED.description, tours.description),
    source_url  = COALESCE(EXCLUDED.source_url, tours.source_url),
    posted_at   = COALESCE(EXCLUDED.posted_at, tours.posted_at),
    stable_key  = COALESCE(EXCLUDED.stable_key, tours.stable_key),
    board       = COALESCE(EXCLUDED.board, tours.board),
    includes    = COALESCE(EXCLUDED.includes, tours.includes);
"""

def save_tours_bulk(rows: list[dict]):
    """Батч-апсерты: быстрее и устойчивее под нагрузкой."""
    if not rows:
        return
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.executemany(SQL_UPSERT_TOUR, rows)
        logging.info("💾 Сохранил/обновил батч: %d шт.", len(rows))
    except Exception as e:
        logging.warning("⚠️ Bulk upsert failed, fallback to single. Reason: %s", e)
        for r in rows:
            try:
                with get_conn() as conn, conn.cursor() as cur:
                    cur.execute(SQL_UPSERT_TOUR, r)
            except Exception as ee:
                logging.error("❌ Ошибка при сохранении тура (msg_id=%s chat=%s): %s",
                              r.get("message_id"), r.get("source_chat"), ee)

def get_existing_row(source_chat: str, message_id: int) -> Optional[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT country, city, hotel, price, currency, dates, description,
                   source_url, posted_at, message_id, source_chat, stable_key,
                   board, includes
            FROM tours
            WHERE source_chat=%s AND message_id=%s
            LIMIT 1
        """, (source_chat, message_id))
        return cur.fetchone()

# ======================= СЛОВАРИ/РЕГЕКС =======================
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

BRAND_HINTS = [
    "rixos", "titanic", "voyage", "miracle", "concorde", "arcanus", "adam & eve", "maxx royal",
    "barut", "limak", "granada", "akra", "cornelia", "gloria", "susesi",
    "delphin", "alva donna", "paloma", "ic hotels", "kaya", "swandor", "regnum", "seginus",
    "hilton", "marriott", "sheraton", "radisson", "novotel", "mercure", "fairmont", "four seasons",
]

BLACKLIST_TOKENS = [
    # гео-общее
    "island", "atoll", "archipelago", "peninsula", "bay", "gulf", "lagoon",
    # RU/UZ
    "остров", "атолл", "залив", "лагуна", "полуостров", "курорт", "пляж", "побережье",
    "турция", "египет", "оаэ", "оае", "таиланд", "узбекистан", "малдивы", "малдив", "черногория",
    "анталия", "алания", "бодрум", "кемер", "сиде", "белек", "шарм", "хургада", "дахааб", "марса алам",
    # EN популярные курорты
    "bali", "phuket", "samui", "lombok", "zanzibar", "goa", "antalya", "alanya", "kemer", "bodrum",
    # общее
    "центр", "парк", "аэропорт", "рынок", "молл", "набережная", "downtown", "airport",
]

KNOWN_GAZETTEER = {
    "bali", "phuket", "samui", "zanzibar", "goa", "antalya", "alanya", "kemer", "side", "belek",
    "dubai", "sharm", "hurghada", "dahab", "bodrum", "istanbul", "izmir", "batumi",
    "tashkent", "samarkand", "bukhara",
}

PRICE_RE = re.compile(
    r"(?P<cur>\$|usd|eur|€|сом|сум|uzs|руб|₽|aed|د\.إ)\s*(?P<amt>[\d\s.,]{2,})|"
    r"(?P<amt2>[\d\s.,]{2,})\s*(?P<cur2>\$|usd|eur|€|сом|сум|uzs|руб|₽|aed)",
    re.I
)
BOARD_RE = re.compile(r"\b(ai|uai|all\s*inclusive|bb|hb|fb|ro|ob|ultra\s*all)\b", re.I)
SPLIT_RE = re.compile(r"[,/\n•;|]\s*")

# ======================= ДАТЫ (RU/UZ) =======================
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
    """Поддержка: '12–19 сент', '12.09–19.09', 'с 12 по 19 сент', одиночные даты."""
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

# ======================= ПАРС ХЕЛПЕРЫ =======================
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
    """N-gram по заглавным словам: 'Rixos Premium Belek', 'Gloria Serenity Resort' и т.п."""
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
            elif cu in {"RUB", "РУБ", "РУБ."}:
                cu = "RUB"
            elif cu == "AED":
                cu = "AED"
            return val, cu
    return None, None

def _extract_board(text: str) -> Optional[str]:
    m = BOARD_RE.search(text)
    if not m:
        return None
    token = m.group(0).lower().replace(" ", "")
    if token in {"ai", "allinclusive"}:
        return "AI"
    if token in {"uai", "ultraall"}:
        return "UAI"
    if token == "bb":
        return "BB"
    if token == "hb":
        return "HB"
    if token == "fb":
        return "FB"
    if token in {"ro", "ob"}:
        return token.upper()
    return token.upper()

def _extract_includes(text: str) -> Optional[str]:
    """Простая агрегация того, что часто пишут как «включено»."""
    low = text.lower()
    flags = []
    if re.search(r"\bперел[её]т|авиа\b|flight|air", low):       flags.append("перелёт")
    if re.search(r"\bтрансфер|transfer\b", low):               flags.append("трансфер")
    if re.search(r"\bстраховк|insurance\b", low):              flags.append("страховка")
    if re.search(r"\bвиза|visa\b", low):                       flags.append("виза")
    if re.search(r"\bэкскурс(ия|ии)|excursion\b", low):        flags.append("экскурсии")
    if re.search(r"\bналоги|tax(es)?\b", low):                 flags.append("налоги")
    if re.search(r"\bбагаж|luggage|baggage\b", low):           flags.append("багаж")
    if not flags:
        return None
    return ", ".join(dict.fromkeys(flags))[:120]

# ======================= ГЕО =======================
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

# ======================= ПАРС ПОСТА =======================
def _extract_hotels(cleaned: str) -> List[str]:
    """Достаём список вероятных отелей из неструктурированного текста."""
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
    # уникализация
    seen = set()
    uniq: List[str] = []
    for h in hotels:
        k = h.lower()
        if k not in seen:
            seen.add(k)
            uniq.append(h)
    return uniq[:5]

def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime):
    """Разбор поста (без картинок), устойчивый к мусору и форматам."""
    raw = text or ""
    cleaned = San.clean_text(raw)
    draft = TourDraft.from_raw(cleaned)

    # город (эвристика)
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Паттайя|Краби|Тбилиси|Шарм|Хургада)", cleaned, re.I)
    city = city_match.group(1) if city_match else (draft.city if getattr(draft, 'city', None) else None)
    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", cleaned)
        city = m.group(1) if m else None

    # отели (мульти-извлечение)
    hotels = _extract_hotels(cleaned)
    hotel = hotels[0] if hotels else (strip_trailing_price_from_hotel(draft.hotel) if draft.hotel else None)

    # даты
    dates = parse_dates_strict(cleaned) or draft.dates

    # цена/валюта
    price, currency = draft.price, draft.currency
    if price is None or currency is None:
        price, currency = _extract_prices(cleaned)

    if currency:
        cu = str(currency).strip().upper()
        if cu in {"$", "US$", "USD$"}:   currency = "USD"
        elif cu in {"€", "EUR€"}:        currency = "EUR"
        elif cu in {"UZS", "СУМ", "СУМЫ", "СУМ."}: currency = "UZS"
        elif cu in {"RUB", "РУБ", "РУБ."}:          currency = "RUB"
        elif cu == "AED":                 currency = "AED"
        else:                             currency = cu
    else:
        low = cleaned.lower()
        if "сум" in low or "uzs" in low:  currency = "UZS"
        elif "eur" in low or "€" in low:  currency = "EUR"
        elif "usd" in low or "$" in low:  currency = "USD"
        elif "aed" in low:                currency = "AED"

    board = _extract_board(cleaned)
    includes = _extract_includes(cleaned)

    payload_base = {
        "country": guess_country(city) if city else None,
        "city": city,
        "price": price,
        "currency": currency,
        "dates": dates,
        "description": cleaned[:500],
        "source_url": link,
        # TIMESTAMPTZ: Telethon даёт aware-время; нормализуем к UTC
        "posted_at": posted_at.astimezone(timezone.utc),
        "message_id": msg_id,
        "source_chat": chat,
        "board": board,
        "includes": includes,
    }

    return payload_base, (hotels if hotels else [hotel] if hotel else [])

# ======================= COLLECT ONCE =======================
async def collect_once(client: TelegramClient):
    """Один проход по всем каналам с батч-сохранением и фильтрами актуальности."""
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=MAX_POST_AGE_DAYS)

    for channel in CHANNELS:
        logging.info("📥 Канал: %s", channel)
        batch: list[dict] = []
        last_id = _get_cp(channel)
        max_seen = last_id

        # читаем только новее чекпоинта, в прямом порядке (старые -> новые)
        async for msg in client.iter_messages(channel, min_id=last_id, reverse=True, limit=FETCH_LIMIT):
            text = (msg.text or "").strip()
            if not text:
                continue

            # слишком старые посты скипаем
            if msg.date and msg.date < cutoff:
                continue

            def _make_rows() -> List[dict]:
                link = f"https://t.me/{channel.lstrip('@')}/{msg.id}"
                base, hotels = parse_post(
                    text, link, msg.id, channel,
                    msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc),
                )
                rows: List[dict] = []
                for h in hotels:
                    if not h:
                        continue
                    row = {**base, "hotel": h}
                    row["stable_key"] = build_tour_key(
                        source_chat=base["source_chat"],
                        message_id=base["message_id"],
                        city=base.get("city") or "",
                        hotel=h,
                        price=(base.get("price"), base.get("currency")) if base.get("price") else None,
                    )
                    rows.append(row)
                # если отель не определился и разрешено без цены — сохраним пустой hotel
                if not rows and not REQUIRE_PRICE:
                    rows.append({
                        **base,
                        "hotel": None,
                        "stable_key": build_tour_key(
                            base["source_chat"], base["message_id"], base.get("city") or "", "", None
                        )
                    })
                return rows

            rows = _make_rows()
            if REQUIRE_PRICE:
                rows = [r for r in rows if (r.get("price") is not None and r.get("currency") is not None)]

            if rows:
                batch.extend(rows)
                if msg.id and msg.id > max_seen:
                    max_seen = msg.id

            # батч-сброс
            if len(batch) >= BATCH_SIZE:
                await safe_run(
                    lambda: asyncio.to_thread(save_tours_bulk, batch.copy()),
                    RetryPolicy(attempts=4, base_delay=0.25, max_delay=2.0)
                )
                batch.clear()

        # остаток батча по каналу
        if batch:
            await safe_run(
                lambda: asyncio.to_thread(save_tours_bulk, batch.copy()),
                RetryPolicy(attempts=4, base_delay=0.25, max_delay=2.0)
            )
            batch.clear()

        if max_seen > last_id:
            _set_cp(channel, max_seen)
            logging.info("⏩ %s чекпоинт обновлён: %s → %s", channel, last_id, max_seen)
        else:
            logging.info("⏸ %s без новых сообщений", channel)

# ======================= EDIT HANDLER =======================
async def _build_channel_maps(client: TelegramClient):
    """Заполняем CH_ID2NAME/CH_NAME2ID для корректного сопоставления edits."""
    CH_ID2NAME.clear()
    CH_NAME2ID.clear()
    for ch in CHANNELS:
        try:
            ent = await client.get_entity(ch)
            # если публичный канал — берём username, иначе оставим исходное имя из ENV
            name = f"@{ent.username}" if getattr(ent, "username", None) else ch
            CH_ID2NAME[int(ent.id)] = name
            CH_NAME2ID[name] = int(ent.id)
        except Exception as e:
            logging.warning("Не удалось получить entity для %s: %s", ch, e)

def _merge_with_existing_preserve_nulls(new_row: dict) -> dict:
    """Если новое значение отсутствует, сохраняем старое из БД (бережный апдейт)."""
    existing = get_existing_row(new_row["source_chat"], new_row["message_id"])
    if not existing:
        return new_row
    merged = {**existing}
    for k in ("country", "city", "hotel", "price", "currency", "dates",
              "description", "source_url", "posted_at", "stable_key", "board", "includes"):
        v = new_row.get(k)
        if v is not None and v != "":
            merged[k] = v
    # поля, которых нет в select, но требуются для апсерта
    merged["message_id"] = new_row["message_id"]
    merged["source_chat"] = new_row["source_chat"]
    return merged

async def handle_edit_event(event: events.MessageEdited.Event):
    """Обработчик редактирования: перепарс и бережный апдейт записи."""
    chat_id = event.chat_id
    channel = CH_ID2NAME.get(int(chat_id))
    if not channel:
        # не наш канал — игнор
        return

    text = (event.text or "").strip()
    if not text:
        return

    # формируем ссылку (если username есть)
    try:
        ent = await event.get_chat()
        if getattr(ent, "username", None):
            link = f"https://t.me/{ent.username}/{event.message.id}"
        else:
            link = f"https://t.me/{channel.lstrip('@')}/{event.message.id}" if channel.startswith("@") else ""
    except Exception:
        link = f"https://t.me/{channel.lstrip('@')}/{event.message.id}" if channel.startswith("@") else ""

    base, hotels = parse_post(
        text, link, event.message.id, channel,
        event.message.date if event.message.date.tzinfo else event.message.date.replace(tzinfo=timezone.utc)
    )

    # выбираем одну запись для апдейта (у нас уникальность по (message_id, source_chat))
    hotel = hotels[0] if hotels else None
    row = {
        **base,
        "hotel": hotel,
        "stable_key": build_tour_key(
            source_chat=base["source_chat"],
            message_id=base["message_id"],
            city=base.get("city") or "",
            hotel=hotel or "",
            price=(base.get("price"), base.get("currency")) if base.get("price") else None,
        )
    }

    # если включён REQUIRE_PRICE и новая правка без цены — не затираем существующую
    if REQUIRE_PRICE and (row.get("price") is None or row.get("currency") is None):
        row = _merge_with_existing_preserve_nulls(row)
    else:
        # в любом случае применяем «бережный» merge, чтобы пустыми значениями не стирать прежние
        row = _merge_with_existing_preserve_nulls(row)

    save_tours_bulk([row])
    logging.info("✏️ Edit обновил %s #%s", channel, event.message.id)

# ======================= RUN =======================
async def run_collector():
    ensure_schema_and_indexes()
    client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)
    await client.start()
    await _build_channel_maps(client)

    # Подписываемся на edits глобально и фильтруем по нашим каналам внутри.
    client.add_event_handler(handle_edit_event, events.MessageEdited())

    logging.info("✅ Collector запущен. Каналы: %s", ", ".join(CHANNELS))

    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error("❌ Ошибка в коллекторе: %s", e)
        # лёгкий джиттер, чтобы не совпадать с другими процессами по минутам
        await asyncio.sleep(SLEEP_BASE + int(10 * (os.getpid() % 3)))

if __name__ == "__main__":
    asyncio.run(run_collector())
