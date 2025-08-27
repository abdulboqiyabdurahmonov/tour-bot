"""
TripleA Sanitizer & Resilience Toolkit (v1.7)
Автор: ChatGPT x Дружище
Назначение:
— Нормализация сырых текстов из телеграм‑каналов туроператоров
— Вычистка ссылок, эмодзи, HTML, лишних пробелов, повторов
— Единый формат цен, дат, телефонов (UZ/RU), валют
— Утилиты для устойчивости: безопасные ретраи, backoff, семафоры

Как использовать:
from sanitazer import San, safe_run, limiter, RetryPolicy

clean = San.clean_text(raw)
price = San.parse_price(raw)
phone = San.parse_phone_uz(raw)

# Внутри aiogram-хэндлеров:
@dp.message(F.text)
async def handler(msg: Message):
    async with limiter.user_slot(msg.from_user.id):
        data = await safe_run(my_task, RetryPolicy())
        ...
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from hashlib import blake2b
from html import unescape
from typing import Iterable, Optional, Tuple

logger = logging.getLogger(__name__)

# =====================================================
#                   REGEX ХАБ
# =====================================================
RX_HTML_TAG = re.compile(r"<[^>]+>")
RX_URL = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
RX_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# широкий срез эмодзи + пиктограммы
RX_EMOJI = re.compile(
    r"[\U0001F1E6-\U0001F1FF]|"     # флаги
    r"[\U0001F300-\U0001F5FF]|"     # символы и пиктограммы
    r"[\U0001F600-\U0001F64F]|"     # смайлы
    r"[\U0001F680-\U0001F6FF]|"     # транспорт
    r"[\U0001F700-\U0001F77F]|"     # алхимия
    r"[\U0001F780-\U0001F7FF]|"     # геометрия
    r"[\U0001F800-\U0001F8FF]|"
    r"[\U0001F900-\U0001F9FF]|"
    r"[\U0001FA00-\U0001FAFF]|"
    r"[\U00002700-\U000027BF]|"      # Dingbats
    r"[\U00002600-\U000026FF]"       # разное
    , re.UNICODE)
RX_WHITESPACE = re.compile(r"\s+")
RX_DUPS = re.compile(r"(\b\w+\b)(?:\s+\1){1,}", re.IGNORECASE)

# Цены/валюты: 1 234 567 сум / so'm / uzs / $ / usd
RX_PRICE = re.compile(
    r"(?P<amount>(?:\d{1,3}(?:[\s.,]\d{3})+|\d+)(?:[\s.,]\d{1,2})?)\s*(?P<currency>(?:uzs|сум|сўм|so['’`\-]?m|som|usd|\$|eur|€))",
    re.IGNORECASE,
)

# Телефоны Узбекистана: +998 9xx xxx xx xx, 9xx xxx xx xx, 8-9xx-...
RX_PHONE = re.compile(
    r"(?:(?:\+?998|8)\D*)?(?P<p1>9\d{2})\D*(?P<p2>\d{3})\D*(?P<p3>\d{2})\D*(?P<p4>\d{2})"
)

# Даты — грубо (ДД.ММ, ДД/ММ, ДД.ММ.ГГ, слова типа 'с 1 по 7 сентября')
RX_DATE_SIMPLE = re.compile(
    r"\b(?:(?:с\s*)?(\d{1,2})\s*(?:-|по|до|/|\.)\s*(\d{1,2})(?:\.(\d{2,4}))?)\b|\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?",
    re.IGNORECASE,
)

# =====================================================
#                 НОРМАЛИЗАЦИЯ ТЕКСТА
# =====================================================
class San:
    """ Главный фасад для чистки и нормализации. """

    @staticmethod
    def to_nfkc(s: str) -> str:
        return unicodedata.normalize("NFKC", s)

    @staticmethod
    def strip_html(s: str) -> str:
        s = unescape(s)
        return RX_HTML_TAG.sub(" ", s)

    @staticmethod
    def remove_urls_emails_emoji(s: str) -> str:
        s = RX_URL.sub(" ", s)
        s = RX_EMAIL.sub(" ", s)
        s = RX_EMOJI.sub(" ", s)
        return s

    @staticmethod
    def normalize_punct(s: str) -> str:
        # кавычки, тире, апострофы -> стандартные
        s = s.replace("“", '"').replace("”", '"').replace("«", '"').replace("»", '"')
        s = s.replace("’", "'").replace("`", "'")
        s = s.replace("–", "-").replace("—", "-")
        return s

    @staticmethod
    def collapse_space(s: str) -> str:
        s = RX_WHITESPACE.sub(" ", s)
        return s.strip()

    @staticmethod
    def dedupe_words(s: str) -> str:
        return RX_DUPS.sub(r"\1", s)

    @staticmethod
    def clean_text(s: Optional[str]) -> str:
        if not s:
            return ""
        s = San.to_nfkc(s)
        s = San.strip_html(s)
        s = San.remove_urls_emails_emoji(s)
        s = San.normalize_punct(s)
        s = San.collapse_space(s)
        s = San.dedupe_words(s)
        return s

    # -----------------------------
    # ЦЕНЫ
    # -----------------------------
    @staticmethod
    def parse_price(s: str) -> Optional[Tuple[float, str]]:
        """ Возвращает (amount, currency). amount — в базовой валюте text, не конвертируем. """
        if not s:
            return None
        m = RX_PRICE.search(San.to_nfkc(s))
        if not m:
            return None
        raw = m.group("amount")
        cur = m.group("currency").lower()
        # unify currency
        cur_map = {
            "сум": "UZS", "сўм": "UZS", "som": "UZS", "so'm": "UZS", "so’m": "UZS", "uzs": "UZS",
            "$": "USD", "usd": "USD", "eur": "EUR", "€": "EUR",
        }
        currency = cur_map.get(cur, cur.upper())
        # amount: убрать пробелы‑тысячники, заменить запятую на точку
        num = raw.replace(" ", "").replace("\xa0", "").replace(",", ".")
        # также формат 1.234.567,89 -> убрать тысячные точки, оставить последнюю
        parts = num.split(".")
        if len(parts) > 2:
            num = "".join(parts[:-1]) + "." + parts[-1]
        try:
            amount = float(num)
            return amount, currency
        except ValueError:
            return None

    # -----------------------------
    # ТЕЛЕФОНЫ (UZ)
    # -----------------------------
    @staticmethod
    def parse_phone_uz(s: str) -> Optional[str]:
        if not s:
            return None
        m = RX_PHONE.search(s)
        if not m:
            return None
        p1, p2, p3, p4 = m.group("p1", "p2", "p3", "p4")
        return f"+998{p1}{p2}{p3}{p4}"

    # -----------------------------
    # ДАТЫ (очень приблизительно)
    # -----------------------------
    @staticmethod
    def extract_dates_simple(s: str) -> Optional[Tuple[str, Optional[str]]]:
        """ Возвращает кортеж строк (start, end) в формате DD.MM[.YYYY] если нашлось. """
        if not s:
            return None
        m = RX_DATE_SIMPLE.search(s)
        if not m:
            return None
        # группа 1-3 или 4-6
        if m.group(1) and m.group(2):
            d1, d2, y = m.group(1), m.group(2), m.group(3)
            yy = f".{y}" if y else ""
            return (f"{d1}.{d2}{yy}", None)
        else:
            d, mth, y = m.group(4), m.group(5), m.group(6)
            yy = f".{y}" if y else ""
            return (f"{d}.{mth}{yy}", None)

    # -----------------------------
    # КЛЮЧИ/ХЭШИ (idempotency)
    # -----------------------------
    @staticmethod
    def stable_key(*chunks: Iterable[str]) -> str:
        """ Стейбл‑хэш для дедупликации (например: source_chat, message_id, city, hotel, price). """
        h = blake2b(digest_size=16)
        for part in chunks:
            if part is None:
                continue
            if isinstance(part, (list, tuple, set)):
                for x in part:
                    h.update(str(x).encode("utf-8", "ignore"))
                    h.update(b"|")
            else:
                h.update(str(part).encode("utf-8", "ignore"))
                h.update(b"|")
        return h.hexdigest()

# =====================================================
#             БЕЗОПАСНЫЕ РЕТРАИ / БЭКОФФЫ
# =====================================================
@dataclass
class RetryPolicy:
    attempts: int = 5
    base_delay: float = 0.3  # секунды
    max_delay: float = 5.0
    jitter: float = 0.2      # +- 20%
    swallow: bool = True     # глотать исключение и вернуть None на последней попытке

async def async_sleep(sec: float):
    await asyncio.sleep(sec)

async def safe_run(coro_func, policy: RetryPolicy, *args, **kwargs):
    """ Выполнить корутину с экспоненциальным бэкоффом + джиттером. """
    delay = policy.base_delay
    for i in range(1, policy.attempts + 1):
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 — в системном коде логируем всё
            logger.warning("safe_run attempt %s/%s failed: %s", i, policy.attempts, e)
            if i >= policy.attempts:
                if policy.swallow:
                    return None
                raise
            # экспонента + джиттер
            jitter = 1 + random.uniform(-policy.jitter, policy.jitter)
            sleep_for = min(policy.max_delay, delay * jitter)
            await async_sleep(sleep_for)
            delay *= 2

# =====================================================
#            ЛИМИТЫ И ПАРАЛЛЕЛИЗМ (SEMAPHORE)
# =====================================================
class _Limiter:
    """ Глобальный семафор + per-user слоты. """

    def __init__(self, global_limit: int = 8, per_user: int = 2):
        self._global = asyncio.Semaphore(global_limit)
        self._user_locks: dict[int, asyncio.Semaphore] = {}
        self._per_user = per_user
        self._lock = asyncio.Lock()

    async def user_slot(self, user_id: int):
        class _AsyncCtx:
            def __init__(self, outer: _Limiter, uid: int):
                self.outer = outer
                self.uid = uid
                self.user_sem: Optional[asyncio.Semaphore] = None

            async def __aenter__(self):
                await self.outer._global.acquire()
                async with self.outer._lock:
                    if self.uid not in self.outer._user_locks:
                        self.outer._user_locks[self.uid] = asyncio.Semaphore(self.outer._per_user)
                    self.user_sem = self.outer._user_locks[self.uid]
                await self.user_sem.acquire()
                return self

            async def __aexit__(self, exc_type, exc, tb):
                try:
                    if self.user_sem:
                        self.user_sem.release()
                finally:
                    self.outer._global.release()
        return _AsyncCtx(self, user_id)

limiter = _Limiter(global_limit=8, per_user=2)

# =====================================================
#           БЕЗОПАСНЫЕ ВСТАВКИ В БД (UPSERT)
# =====================================================
def build_tour_key(source_chat: str, message_id: int, city: str|None, hotel: str|None, price: Tuple[float,str]|None) -> str:
    price_part = f"{price[0]}{price[1]}" if price else ""
    return San.stable_key(source_chat, message_id, city or "", hotel or "", price_part)

SQL_CREATE_INDEXES = """
-- Уникальность по (source_chat, message_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_tours_source_msg ON tours(source_chat, message_id);
-- Доп. уникальность по стабильному ключу на случай редактирования постов/перепостов
ALTER TABLE tours ADD COLUMN IF NOT EXISTS stable_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_tours_stable_key ON tours(stable_key);
"""

SQL_UPSERT_TOUR = """
INSERT INTO tours(country, city, hotel, price, currency, dates, description, source_chat, message_id, stable_key)
VALUES (%(country)s, %(city)s, %(hotel)s, %(price)s, %(currency)s, %(dates)s, %(description)s, %(source_chat)s, %(message_id)s, %(stable_key)s)
ON CONFLICT (source_chat, message_id) DO UPDATE SET
    country = EXCLUDED.country,
    city = EXCLUDED.city,
    hotel = EXCLUDED.hotel,
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    dates = EXCLUDED.dates,
    description = EXCLUDED.description,
    stable_key = EXCLUDED.stable_key;
"""

# =====================================================
#           ПРИМЕР: ПАЙПЛАЙН ПАРСИНГА ТУРА
# =====================================================
@dataclass
class TourDraft:
    country: Optional[str] = None
    city: Optional[str] = None
    hotel: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    dates: Optional[str] = None
    phone: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_raw(cls, text: str) -> "TourDraft":
        t = San.clean_text(text)
        price = San.parse_price(t)
        phone = San.parse_phone_uz(t)
        dates = None
        d = San.extract_dates_simple(t)
        if d:
            dates = " — ".join([x for x in d if x])
        return cls(
            price=price[0] if price else None,
            currency=price[1] if price else None,
            dates=dates,
            phone=phone,
            description=t,
        )

# =====================================================
#   ХЕЛПЕРЫ ДЛЯ AIROGRAM / FASTAPI (пример интеграции)
# =====================================================
async def parse_and_store(db_exec, *, raw_text: str, source_chat: str, message_id: int, country: str|None=None, city: str|None=None, hotel: str|None=None):
    draft = TourDraft.from_raw(raw_text)
    key = build_tour_key(source_chat, message_id, city or draft.city or "", hotel or draft.hotel or "", (draft.price, draft.currency) if draft.price else None)
    payload = {
        "country": country,
        "city": city,
        "hotel": hotel,
        "price": draft.price,
        "currency": draft.currency,
        "dates": draft.dates,
        "description": draft.description,
        "source_chat": source_chat,
        "message_id": message_id,
        "stable_key": key,
    }
    await db_exec(SQL_UPSERT_TOUR, payload)
    return payload

# =====================================================
#        ПРИМЕР БЕЗОПАСНОГО HTTP-ЗАПРОСА С РЕТРАЯМИ
# =====================================================
import httpx

async def http_get_json(url: str, *, timeout: float = 8.0, policy: RetryPolicy | None = None):
    policy = policy or RetryPolicy()

    async def _go():
        async with httpx.AsyncClient(timeout=timeout) as cli:
            r = await cli.get(url)
            r.raise_for_status()
            return r.json()

    return await safe_run(_go, policy)

# =====================================================
#     ПРИМЕР DB EXECUTOR (async) ДЛЯ psycopg[pool]/asyncpg
# =====================================================
class SimpleAsyncDB:
    """Мини‑обёртка. Замените на ваш пул (asyncpg/psycopg_pool)."""
    def __init__(self, pool):
        self.pool = pool

    async def exec(self, sql: str, args: dict):
        # пример под asyncpg
        async with self.pool.acquire() as conn:
            await conn.execute(sql, args)

# =====================================================
#                БЫСТРЫЕ САМООТЕСТЫ
# =====================================================
if __name__ == "__main__":
    sample = """
    🔥🔥🔥 LAST MINUTE! 1 499 000 сум / на человека
    Турция, Анталья. Miracle Resort 5* — 7 ночей, с 12/09.
    Тел: +998 (90) 123-45-67. Бронируй: https://t.me/somelink 💥
    """
    print("clean:", San.clean_text(sample))
    print("price:", San.parse_price(sample))
    print("phone:", San.parse_phone_uz(sample))
    print("dates:", San.extract_dates_simple(sample))
