import os
import re
import logging
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
from html import escape
from collections import defaultdict
import secrets
from zoneinfo import ZoneInfo  # ⬅️ локальная таймзона

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row

import httpx
from db_init import init_db  # твоя инициализация БД

# ================= ЛОГИ =================
logging.basicConfig(level=logging.INFO)

# ================= ENV =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", os.getenv("WEBHOOK_URL", "https://tour-bot-rxi8.onrender.com"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не найден в переменных окружения!")

# ================= КОНСТАНТЫ =================
TZ = ZoneInfo("Asia/Tashkent")  # локальная зона для отображения времени
PAGER_STATE: dict[str, dict] = {}  # память пагинации
PAGER_TTL_SEC = 3600  # 1 час живёт подборка

# ================= БОТ / APP =================
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
app = FastAPI()

# ================= БД =================
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

# ================= КЛАВИАТУРЫ =================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎒 Найти туры"), KeyboardButton(text="🤖 Спросить GPT")],
        [KeyboardButton(text="🔔 Подписка"), KeyboardButton(text="⚙️ Настройки")],
    ],
    resize_keyboard=True,
)

def filters_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Актуальные 72ч", callback_data="tours_recent")],
            [
                InlineKeyboardButton(text="🌴 Турция", callback_data="country:Турция"),
                InlineKeyboardButton(text="🇦🇪 ОАЭ", callback_data="country:ОАЭ"),
            ],
            [
                InlineKeyboardButton(text="🇹🇭 Таиланд", callback_data="country:Таиланд"),
                InlineKeyboardButton(text="🇻🇳 Вьетнам", callback_data="country:Вьетнам"),
            ],
            # 💸 бюджет по USD
            [
                InlineKeyboardButton(text="💸 ≤ $500", callback_data="budget:USD:500"),
                InlineKeyboardButton(text="💸 ≤ $800", callback_data="budget:USD:800"),
                InlineKeyboardButton(text="💸 ≤ $1000", callback_data="budget:USD:1000"),
            ],
            [InlineKeyboardButton(text="↕️ Сортировка по цене", callback_data="sort:price_asc")],
            [InlineKeyboardButton(text="➕ Ещё фильтры скоро", callback_data="noop")],
        ]
    )

def sources_kb(
    rows: List[dict],
    *,
    start_index: int = 1,
    back_to: str = "back_filters",
    token: Optional[str] = None,
    next_offset: Optional[int] = None,
) -> InlineKeyboardMarkup:
    buttons = []
    for idx, t in enumerate(rows, start=start_index):
        url = (t.get("source_url") or "").strip()
        if url:
            buttons.append([InlineKeyboardButton(text=f"🔗 Открыть #{idx}", url=url)])

    # Показать ещё (если передан токен и рассчитан следующий offset)
    if token and next_offset is not None:
        buttons.append([InlineKeyboardButton(text="➡️ Показать ещё", callback_data=f"more:{token}:{next_offset}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ================= УТИЛИТЫ ПАГИНАЦИИ =================
def _new_token() -> str:
    # короткий токен для callback_data
    return secrets.token_urlsafe(6).rstrip("=-_")

def _cleanup_pager_state():
    now = time.monotonic()
    to_del = []
    for k, v in PAGER_STATE.items():
        ts = v.get("ts", now)
        if now - ts > PAGER_TTL_SEC:
            to_del.append(k)
    for k in to_del:
        PAGER_STATE.pop(k, None)

def _touch_state(token: str):
    st = PAGER_STATE.get(token)
    if st:
        st["ts"] = time.monotonic()

# ================= ПОМОЩНИКИ ВЫВОДА =================
async def show_typing(message: Message, text: str = "🤔 Думаю... Ищу варианты для тебя"):
    try:
        await bot.send_chat_action(message.chat.id, "typing")
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка show_typing: {e}")

def fmt_price(price, currency) -> str:
    if price is None:
        return "—"
    try:
        p = int(float(price))
    except Exception:
        return escape(f"{price} {currency or ''}".strip())

    cur = (currency or "").strip()
    cur_up = cur.upper()
    # нормализация символов
    if cur_up in {"$", "US$", "USD$", "USD"}:
        cur_up = "USD"
    elif cur_up in {"€", "EUR€", "EUR"}:
        cur_up = "EUR"
    elif cur_up in {"UZS", "СУМ", "СУМ.", "СУМЫ", "СУМОВ", "СОМ", "СУМ", "СУММ", "СУММ." , "СУМ." , "СУМЫ.", "СУМ." , "СУММЫ"}:
        cur_up = "UZS"
    elif cur_up in {"СУМ", "сум"}:
        cur_up = "UZS"
    return escape(f"{p:,} {cur_up}".replace(",", " "))

def safe(s: Optional[str]) -> str:
    return escape(s or "—")

def clean_text_basic(s: Optional[str]) -> str:
    """Убирает markdown-мусор и лишние пробелы"""
    if not s:
        return "—"
    s = re.sub(r'[*_`]+', '', s)
    s = s.replace('|', ' ')
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

def strip_trailing_price_from_hotel(s: Optional[str]) -> Optional[str]:
    """Срезает '– от 767 USD', ' - 1207$ 🥂' и т.п. в конце строки."""
    if not s:
        return s
    return re.sub(
        r'[\s\u00A0–—-]*'               # тире/пробелы/nbsp
        r'(?:от\s*)?'                   # опционально 'от'
        r'\d[\d\s\u00A0.,]*'            # число
        r'\s*(?:USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€).*$',  # валюта и ЛЮБОЙ хвост
        '',
        s,
        flags=re.I
    ).strip()

def normalize_dates_for_display(s: Optional[str]) -> str:
    """
    Нормализует строку вида 04.25.2025–04.25.10 -> 25.04.2025–10.04.2025.
    Если формат не совпадает, возвращает как есть (экранированно).
    """
    if not s:
        return "—"
    s = s.strip()
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})\s*[–-]\s*(\d{1,2})\.(\d{1,2})\.(\d{2,4})", s)
    if not m:
        return escape(s)

    d1, m1, y1, d2, m2, y2 = m.groups()

    def _norm(d, mo, y):
        d = int(d); mo = int(mo); y = int(y)
        if y < 100:
            y += 2000 if y < 70 else 1900
        # если "месяц" > 12, а "день" <= 12 — вероятно, поменяли местами
        if mo > 12 and d <= 12:
            d, mo = mo, d
        return f"{d:02d}.{mo:02d}.{y:04d}"

    return f"{_norm(d1, m1, y1)}–{_norm(d2, m2, y2)}"

def compile_tours_text(rows: List[dict], header: str, start_index: int = 1) -> str:
    lines = []
    for idx, t in enumerate(rows, start=start_index):
        posted = t.get("posted_at")
        # локализуем время в Tashkent
        posted_str = ""
        if isinstance(posted, datetime):
            try:
                posted_local = posted if posted.tzinfo else posted.replace(tzinfo=ZoneInfo("UTC"))
                posted_local = posted_local.astimezone(TZ)
                posted_str = f"🕒 {posted_local.strftime('%d.%m.%Y %H:%M')} (TST)\n"
            except Exception:
                posted_str = f"🕒 {posted.strftime('%d.%m.%Y %H:%M')}\n"

        price_str = fmt_price(t.get("price"), t.get("currency"))
        src = (t.get("source_url") or "").strip()

        hotel_raw = t.get("hotel")
        hotel_clean = clean_text_basic(strip_trailing_price_from_hotel(hotel_raw))
        dates_norm = normalize_dates_for_display(t.get("dates"))

        card = (
            f"#{idx}\n"
            f"🌍 {safe(t.get('country'))} — {safe(t.get('city'))}\n"
            f"🏨 {safe(hotel_clean)}\n"
            f"💵 {price_str}\n"
            f"📅 {dates_norm}\n"
            f"{posted_str}"
        )
        if src:
            card += f'🔗 <a href="{escape(src)}">Источник</a>'
        lines.append(card.strip())

    body = "\n\n".join(lines) if lines else "Пока пусто. Попробуй сменить фильтр."
    return f"<b>{escape(header)}</b>\n\n{body}"

def split_telegram(text: str, limit: int = 3500) -> List[str]:
    parts: List[str] = []
    t = text
    while len(t) > limit:
        cut = t.rfind("\n\n", 0, limit)
        if cut == -1 or cut < int(limit * 0.6):
            cut = limit
        parts.append(t[:cut].rstrip())
        t = t[cut:].lstrip()
    if t:
        parts.append(t)
    return parts

# ================= ПОИСК ТУРОВ =================
async def fetch_tours(
    query: Optional[str] = None,
    *,
    country: Optional[str] = None,
    currency_eq: Optional[str] = None,
    max_price: Optional[float] = None,
    hours: int = 72,
    limit_recent: int = 10,
    limit_fallback: int = 5,
) -> Tuple[List[dict], bool]:
    """Возвращает (rows, is_recent). Поддерживает фильтры валюты и цены."""
    try:
        where_clauses = []
        params = []

        if query:
            where_clauses.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s)")
            params += [f"%{query}%", f"%{query}%", f"%{query}%"]
        if country:
            where_clauses.append("country ILIKE %s")
            params.append(f"%{country}%")  # ⬅️ «человечный» поиск
        if currency_eq:
            where_clauses.append("currency = %s")
            params.append(currency_eq)
        if max_price is not None:
            where_clauses.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # если есть фильтр по цене — сортируем сначала дешевле
        order_clause = "ORDER BY price ASC NULLS LAST, posted_at DESC" if max_price is not None else "ORDER BY posted_at DESC"

        with get_conn() as conn, conn.cursor() as cur:
            # recent
            sql_recent = f"""
                SELECT country, city, hotel, price, currency, dates, source_url, posted_at
                FROM tours
                {where_sql} {('AND' if where_sql else 'WHERE')} posted_at >= %s
                {order_clause}
                LIMIT %s
            """
            cur.execute(sql_recent, params + [cutoff, limit_recent])
            rows = cur.fetchall()
            if rows:
                return rows, True

            # fallback
            sql_fb = f"""
                SELECT country, city, hotel, price, currency, dates, source_url, posted_at
                FROM tours
                {where_sql}
                {order_clause}
                LIMIT %s
            """
            cur.execute(sql_fb, params + [limit_fallback])
            fb_rows = cur.fetchall()
            return fb_rows, False
    except Exception as e:
        logging.error(f"Ошибка при fetch_tours: {e}")
        return [], False

async def fetch_tours_page(
    query: Optional[str] = None,
    *,
    country: Optional[str] = None,
    currency_eq: Optional[str] = None,
    max_price: Optional[float] = None,
    hours: Optional[int] = None,      # если задано — фильтр по свежести posted_at
    order_by_price: bool = False,     # для бюджетных
    limit: int = 10,
    offset: int = 0,
) -> List[dict]:
    try:
        where_clauses = []
        params: List = []

        if query:
            where_clauses.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s)")
            params += [f"%{query}%", f"%{query}%", f"%{query}%"]
        if country:
            where_clauses.append("country ILIKE %s")
            params.append(f"%{country}%")  # ⬅️ «человечный» поиск
        if currency_eq:
            where_clauses.append("currency = %s")
            params.append(currency_eq)
        if max_price is not None:
            where_clauses.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)
        if hours is not None:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            where_clauses.append("posted_at >= %s")
            params.append(cutoff)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        order_clause = "ORDER BY price ASC NULLS LAST, posted_at DESC" if order_by_price else "ORDER BY posted_at DESC"

        sql = f"""
            SELECT country, city, hotel, price, currency, dates, source_url, posted_at
            FROM tours
            {where_sql}
            {order_clause}
            LIMIT %s OFFSET %s
        """

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params + [limit, offset])
            rows = cur.fetchall()
            return rows
    except Exception as e:
        logging.error(f"Ошибка fetch_tours_page: {e}")
        return []

# ================= GPT =================
last_gpt_call = defaultdict(float)  # per-user cooldown

async def ask_gpt(prompt: str, *, user_id: int, premium: bool = False) -> List[str]:
    now = time.monotonic()
    if now - last_gpt_call[user_id] < 12.0:
        return ["😮‍💨 Подожди пару секунд — я ещё обрабатываю твой предыдущий запрос."]

    last_gpt_call[user_id] = now

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты — AI-консультант по путешествиям из экосистемы TripleA. "
                    "Отвечай дружелюбно и конкретно. Держись тематики: туры, отели, сезоны, визы, цены, лайфхаки."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.6,
        "max_tokens": 700,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(3):
                r = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                if r.status_code == 200:
                    data = r.json()
                    msg = (data.get("choices") or [{}])[0].get("message", {}).get("content")
                    if not msg:
                        logging.error(f"OpenAI no choices/message: {data}")
                        break
                    answer = msg.strip()
                    if premium:
                        answer += "\n\n🔗 Источник тура: https://t.me/triplea_channel"
                    else:
                        answer += "\n\n✨ Хочешь прямые ссылки на источники туров? Подключи Premium доступ TripleA."
                    MAX_LEN = 3800
                    return [answer[i : i + MAX_LEN] for i in range(0, len(answer), MAX_LEN)]
                elif r.status_code == 429:
                    await asyncio.sleep(1.5**attempt)
                    continue
                else:
                    logging.error(f"OpenAI error {r.status_code}: {r.text[:400]}")
                    break
    except Exception as e:
        logging.exception(f"GPT call failed: {e}")

    return [
        "⚠️ Сервер ИИ перегружен. Попробуй ещё раз чуть позже — а пока загляни в «🎒 Найти туры» для готовых вариантов."
    ]

# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "🌍 Привет! Я — <b>TripleA Travel Bot</b> ✈️\n\n"
        "Выбери действие ниже. «🎒 Найти туры» — быстрая актуалка из базы.\n"
        "«🤖 Спросить GPT» — умные ответы про сезоны, бюджеты и лайфхаки.\n"
    )
    await message.answer(text, reply_markup=main_kb)

@dp.message(F.text == "🎒 Найти туры")
async def entry_find_tours(message: Message):
    await message.answer("Выбери быстрый фильтр:", reply_markup=filters_inline_kb())

@dp.message(F.text == "🤖 Спросить GPT")
async def entry_gpt(message: Message):
    await message.answer("Спроси что угодно про путешествия (отели, сезоны, визы, бюджеты).")

@dp.message(F.text == "🔔 Подписка")
async def entry_sub(message: Message):
    await message.answer("Скоро: подписка по странам/бюджету/датам. Пока в разработке 💡")

@dp.message(F.text == "⚙️ Настройки")
async def entry_settings(message: Message):
    await message.answer("Скоро: язык/валюта/бюджет по умолчанию. Пока в разработке ⚙️")

@dp.callback_query(F.data == "tours_recent")
async def cb_recent(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    rows, is_recent = await fetch_tours(None, hours=72, limit_recent=10, limit_fallback=10)
    header = "🔥 Актуальные за 72 часа" if is_recent else "ℹ️ Свежих 72ч мало — показываю последние"
    text = compile_tours_text(rows, header, start_index=1)

    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": None,
        "currency_eq": None,
        "max_price": None,
        "hours": 72 if is_recent else None,
        "order_by_price": False,
        "ts": time.monotonic(),
    }

    try:
        for chunk in split_telegram(text):
            await call.message.answer(
                chunk,
                disable_web_page_preview=True,
                reply_markup=sources_kb(rows, start_index=1, token=token, next_offset=len(rows)),
            )
    except Exception as e:
        logging.error("Send HTML failed (recent): %s", e)
        await call.message.answer("Не удалось отрендерить карточки. Попробуй ещё раз.", reply_markup=filters_inline_kb())

@dp.callback_query(F.data.startswith("country:"))
async def cb_country(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    country = call.data.split(":", 1)[1]
    rows, is_recent = await fetch_tours(None, country=country, hours=120, limit_recent=10, limit_fallback=10)
    header = f"🇺🇳 Страна: {country} — актуальные" if is_recent else f"🇺🇳 Страна: {country} — последние найденные"
    text = compile_tours_text(rows, header, start_index=1)

    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": country,
        "currency_eq": None,
        "max_price": None,
        "hours": 120 if is_recent else None,
        "order_by_price": False,
        "ts": time.monotonic(),
    }

    try:
        for chunk in split_telegram(text):
            await call.message.answer(
                chunk,
                disable_web_page_preview=True,
                reply_markup=sources_kb(rows, start_index=1, token=token, next_offset=len(rows)),
            )
    except Exception as e:
        logging.error("Send HTML failed (country): %s", e)
        await call.message.answer(
            f"Не удалось показать подборку по стране {escape(country)}. Попробуй ещё раз.",
            reply_markup=filters_inline_kb(),
        )

@dp.callback_query(F.data.startswith("budget:"))
async def cb_budget(call: CallbackQuery):
    # формат: budget:<CUR>:<LIMIT>
    _, cur, limit_str = call.data.split(":", 2)
    try:
        limit_val = float(limit_str)
    except Exception:
        limit_val = None

    await bot.send_chat_action(call.message.chat.id, "typing")

    rows, is_recent = await fetch_tours(
        None,
        currency_eq=cur,
        max_price=limit_val,
        hours=120,
        limit_recent=12,
        limit_fallback=12
    )
    hdr = f"💸 Бюджет: ≤ {int(limit_val)} {cur} — актуальные" if is_recent else f"💸 Бюджет: ≤ {int(limit_val)} {cur} — последние найденные"
    text = compile_tours_text(rows, hdr, start_index=1)

    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": None,
        "currency_eq": cur,
        "max_price": limit_val,
        "hours": 120 if is_recent else None,
        "order_by_price": True,
        "ts": time.monotonic(),
    }

    try:
        for chunk in split_telegram(text):
            await call.message.answer(
                chunk,
                disable_web_page_preview=True,
                reply_markup=sources_kb(rows, start_index=1, token=token, next_offset=len(rows)),
            )
    except Exception as e:
        logging.error("Send HTML failed (budget): %s", e)
        await call.message.answer("Не удалось отрендерить карточки по бюджету. Попробуй ещё раз.", reply_markup=filters_inline_kb())

@dp.callback_query(F.data == "sort:price_asc")
async def cb_sort_price_asc(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    # Берём только свежие 72ч и сортируем по цене
    rows = await fetch_tours_page(
        hours=72,
        order_by_price=True,
        limit=10,
        offset=0,
    )
    header = "↕️ Актуальные за 72ч — дешевле → дороже"
    text = compile_tours_text(rows, header, start_index=1)

    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": None,
        "currency_eq": None,
        "max_price": None,
        "hours": 72,
        "order_by_price": True,
        "ts": time.monotonic(),
    }

    try:
        for chunk in split_telegram(text):
            await call.message.answer(
                chunk,
                disable_web_page_preview=True,
                reply_markup=sources_kb(rows, start_index=1, token=token, next_offset=len(rows)),
            )
    except Exception as e:
        logging.error("Send HTML failed (sort price): %s", e)
        await call.message.answer("Не удалось показать отсортированные туры.", reply_markup=filters_inline_kb())

@dp.callback_query(F.data.startswith("more:"))
async def cb_more(call: CallbackQuery):
    try:
        _, token, offset_str = call.data.split(":", 2)
        offset = int(offset_str)
    except Exception:
        await call.answer("Что-то пошло не так с пагинацией 🥲", show_alert=False)
        return

    _cleanup_pager_state()

    state = PAGER_STATE.get(token)
    if not state or state.get("chat_id") != call.message.chat.id:
        await call.answer("Эта подборка уже неактивна.", show_alert=False)
        return

    rows = await fetch_tours_page(
        query=state.get("query"),
        country=state.get("country"),
        currency_eq=state.get("currency_eq"),
        max_price=state.get("max_price"),
        hours=state.get("hours"),
        order_by_price=state.get("order_by_price", False),
        limit=10,
        offset=offset,
    )

    if not rows:
        await call.answer("Это всё на сегодня ✨", show_alert=False)
        return

    header = "Продолжаю подборку"
    start_index = offset + 1
    text = compile_tours_text(rows, header, start_index=start_index)
    next_offset = offset + len(rows)

    _touch_state(token)

    for chunk in split_telegram(text):
        await call.message.answer(
            chunk,
            disable_web_page_preview=True,
            reply_markup=sources_kb(rows, start_index=start_index, token=token, next_offset=next_offset),
        )

@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer("Скоро добавим детальные фильтры 🤝", show_alert=False)

@dp.callback_query(F.data == "back_filters")
async def cb_back_filters(call: CallbackQuery):
    await call.message.answer("Вернулся к фильтрам:", reply_markup=filters_inline_kb())

@dp.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    await call.message.answer("Главное меню:", reply_markup=main_kb)

# --- Смарт-роутер текста: короткие запросы -> поиск, длинные -> GPT
@dp.message(F.text & ~F.text.in_({"🎒 Найти туры", "🤖 Спросить GPT", "🔔 Подписка", "⚙️ Настройки"}))
async def smart_router(message: Message):
    user_text = message.text.strip()
    await bot.send_chat_action(message.chat.id, "typing")

    if len(user_text) <= 40:
        rows, is_recent = await fetch_tours(user_text, hours=72)
        if rows:
            header = "🔥 Нашёл актуальные за 72 часа:" if is_recent else "ℹ️ Свежих 72ч нет — вот последние варианты:"
            text = compile_tours_text(rows, header)
            try:
                for chunk in split_telegram(text):
                    await message.answer(chunk, disable_web_page_preview=True, reply_markup=sources_kb(rows))
            except Exception as e:
                logging.error("Send HTML failed (smart_router): %s", e)
                await message.answer("Не удалось отрендерить карточки. Попробуй ещё раз.", reply_markup=filters_inline_kb())
            return

    # иначе GPT
    premium_users = {123456789}
    is_premium = message.from_user.id in premium_users
    replies = await ask_gpt(user_text, user_id=message.from_user.id, premium=is_premium)
    for part in replies:
        await message.answer(part, parse_mode=None)  # без парсинга, чтобы не падать на разметке

# ================= WEBHOOK =================
@app.get("/")
async def root():
    return {"status": "ok", "message": "TripleA Travel Bot is running!"}

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        await asyncio.sleep(0)
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    return JSONResponse({"status": "ok"})

# ================= START/STOP =================
@app.on_event("startup")
async def on_startup():
    try:
        init_db()
    except Exception as e:
        logging.error(f"Ошибка init_db(): {e}")

    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}")
    else:
        logging.warning("WEBHOOK_URL не указан — бот не получит апдейты.")

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'tours'
                ORDER BY ordinal_position
            """)
            cols = [r["column_name"] for r in cur.fetchall()]
            logging.info(f"🎯 Колонки в таблице tours: {cols}")
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке колонок: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()
