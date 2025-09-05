import os
import re
import logging
import asyncio
import random
import time
import json, base64
# ДОБАВЬ рядом с остальными импортами
from payments import (
    create_order, build_checkout_link, activate_after_payment,
    click_handle_callback, payme_handle_callback
)
from google.oauth2 import service_account
import gspread
from typing import Optional, Tuple, List, Dict
from html import escape
from collections import defaultdict
import secrets
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone

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
from aiogram.filters import Command  # aiogram v3.x
from aiogram.client.default import DefaultBotProperties

from psycopg import connect
from psycopg.rows import dict_row

import httpx
from db_init import init_db, get_config, set_config  # конфиг из БД

# ===== ПАМЯТЬ ДИАЛОГА =====
LAST_RESULTS: dict[int, list[dict]] = {}   # user_id -> последние показанные туры
LAST_QUERY_AT: dict[int, float] = {}       # user_id -> ts последнего показа
LAST_PREMIUM_HINT_AT: dict[int, float] = {}  # user_id -> ts последней плашки "премиум"
LAST_QUERY_TEXT: dict[int, str] = {}       # user_id -> последний смысловой запрос

# Синонимы/алиасы гео (минимальный словарик)
ALIASES = {
    "фукуок": ["фукуок", "phu quoc", "phuquoc", "phú quốc"],
    "шарм": ["шарм", "sharm", "sharm el sheikh", "sharm-el-sheikh", "шарм-эль-шейх"],
    "дубай": ["дубай", "dubai", "dxб"],
    "нячанг": ["нячанг", "nha trang", "nhatrang"],
}
def _expand_query(q: str) -> list[str]:
    low = q.lower().strip()
    for k, arr in ALIASES.items():
        if low in arr:
            return arr
    return [q]

def _should_hint_premium(user_id: int, cooldown_sec: int = 6*3600) -> bool:
    now = time.monotonic()
    ts = LAST_PREMIUM_HINT_AT.get(user_id, 0.0)
    if now - ts >= cooldown_sec:
        LAST_PREMIUM_HINT_AT[user_id] = now
        return True
    return False

def _remember_query(user_id: int, q: str):
    q = (q or "").strip()
    if q:
        LAST_QUERY_TEXT[user_id] = q

def _guess_query_from_link_phrase(text: str) -> Optional[str]:
    """
    Выдёргиваем смысл из фраз типа:
    'пришли ссылки на туры в Шарм Эль Шейх', 'ссылку на источник по Египту' и т.п.
    """
    if not text:
        return None
    # возьмём фрагмент после на/в/во, иначе весь текст
    m = re.search(r"(?:на|в|во)\s+([A-Za-zА-Яа-яЁё\- \t]{3,})", text, flags=re.I)
    frag = m.group(1) if m else text
    frag = re.sub(
        r"\b(ссылк\w*|источник\w*|пришл\w*|отправ\w*|мне|эти|на|в|во|по|про|отыщи|найди|покажи|туры?|тур)\b",
        "",
        frag,
        flags=re.I
    )
    frag = re.sub(r"[.,;:!?]+$", "", frag).strip()
    return frag or None

# ================= ЛОГИ =================
logging.basicConfig(level=logging.INFO)

# ================= ENV =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", os.getenv("WEBHOOK_URL", "https://tour-bot-rxi8.onrender.com"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

LEADS_CHAT_ID_ENV = (os.getenv("LEADS_CHAT_ID") or "").strip()
LEADS_TOPIC_ID = int(os.getenv("LEADS_TOPIC_ID", "0") or 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0)

# Google Sheets ENV
SHEETS_CREDENTIALS_B64 = (os.getenv("SHEETS_CREDENTIALS_B64") or "").strip()
SHEETS_SPREADSHEET_ID = (os.getenv("SHEETS_SPREADSHEET_ID") or "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Заявки")  # имя вкладки для лидов
KB_SHEET_NAME = os.getenv("KB_SHEET_NAME", "KB")        # лист для актуальных фактов

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не найден в переменных окружения!")

# ================= КОНСТАНТЫ =================
TZ = ZoneInfo("Asia/Tashkent")
PAGER_STATE: Dict[str, Dict] = {}
PAGER_TTL_SEC = 3600
WANT_STATE: Dict[int, Dict] = {}

# --- Динамическая проверка колонок схемы
SCHEMA_COLS: set[str] = set()

def _has_cols(*names: str) -> bool:
    return all(n in SCHEMA_COLS for n in names)

def _select_tours_clause() -> str:
    """
    Возвращает список полей для SELECT по tours с безопасными фоллбэками,
    если колонок board/includes нет в текущей схеме.
    """
    base = "id, country, city, hotel, price, currency, dates, source_url, posted_at, photo_url, description"
    extras = []
    extras.append("board" if _has_cols("board") else "NULL AS board")
    extras.append("includes" if _has_cols("includes") else "NULL AS includes")
    return f"{base}, {', '.join(extras)}"

# ================= БОТ / APP =================
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
app = FastAPI()

# ================= БД =================
def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def ensure_pending_wants_table():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_wants (
                user_id BIGINT PRIMARY KEY,
                tour_id INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

def ensure_leads_schema():
    """
    Создаёт таблицу leads (если её нет) и добавляет недостающие колонки.
    Схема согласована с create_lead(full_name, phone, tour_id, note).
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id BIGSERIAL PRIMARY KEY,
                full_name TEXT NOT NULL DEFAULT '',
                phone TEXT,
                tour_id INTEGER,
                note TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                user_id BIGINT
            );
        """)
        # догоним недостающие колонки (на случай старой схемы)
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS full_name TEXT NOT NULL DEFAULT '';")
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone TEXT;")
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS tour_id INTEGER;")
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS note TEXT;")
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();")
        cur.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS user_id BIGINT;")
        cur.execute("CREATE INDEX IF NOT EXISTS leads_created_at_idx ON leads(created_at);")

# ============== GOOGLE SHEETS (robust init + KB + Leads) ==============
_gs_client = None

def _get_gs_client():
    """Ленивая инициализация gspread-клиента. Поддержка base64 JSON или прямого JSON."""
    global _gs_client
    if _gs_client is not None:
        return _gs_client
    if not (SHEETS_CREDENTIALS_B64 and SHEETS_SPREADSHEET_ID):
        logging.info("GS: credentials or spreadsheet id not set")
        _gs_client = None
        return None
    try:
        # Пытаемся как base64 (валидируем). Если не base64 — пробуем как обычный JSON.
        try:
            decoded = base64.b64decode(SHEETS_CREDENTIALS_B64, validate=True)
            info = json.loads(decoded.decode("utf-8"))
        except Exception:
            info = json.loads(SHEETS_CREDENTIALS_B64)

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        _gs_client = gspread.authorize(creds)
        logging.info("✅ Google Sheets авторизация успешно выполнена")
        return _gs_client
    except Exception as e:
        logging.error(f"GS init failed: {e}")
        _gs_client = None
        return None

def _ensure_ws(spreadsheet, title: str, header: list[str]) -> gspread.Worksheet:
    """Гарантируем наличие листа с именем title. Если нет — создаём и ставим шапку."""
    try:
        ws = spreadsheet.worksheet(title)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        pass

    # листа нет — создаём
    try:
        ws = spreadsheet.add_worksheet(title=title, rows=500, cols=max(12, len(header) + 2))
        if header:
            ws.append_row(header, value_input_option="USER_ENTERED")
        logging.info(f"GS: created worksheet '{title}'")
        return ws
    except Exception as e:
        logging.error(f"GS: failed to create worksheet '{title}': {e}")
        raise

def _ensure_header(ws, header: list[str]) -> None:
    """Обновляет первую строку: добавляет недостающие колонки справа."""
    try:
        current = ws.row_values(1)
    except Exception:
        current = []
    new = list(current)
    changed = False
    for h in header:
        if h not in current:
            new.append(h)
            changed = True
    if not changed:
        return
    need = len(new) - ws.col_count
    if need > 0:
        ws.add_cols(need)
    ws.update('1:1', [new])
    logging.info(f"GS: header updated -> {new}")

async def load_kb_context(max_rows: int = 60) -> str:
    """Читает KB-лист и собирает факты в короткий текст для подмешивания в GPT."""
    try:
        gc = _get_gs_client()
        if not gc:
            return ""
        sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
        try:
            ws = sh.worksheet(KB_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return ""
        rows = ws.get_all_records()  # list[dict]
        lines = []
        for r in rows[:max_rows]:
            topic = (r.get("topic") or r.get("Тема") or r.get("topic/country") or "").strip()
            fact = (r.get("fact") or r.get("Факт") or r.get("note") or "").strip()
            if not fact:
                continue
            if topic:
                lines.append(f"- [{topic}] {fact}")
            else:
                lines.append(f"- {fact}")
        return "\n".join(lines)
    except Exception as e:
        logging.warning(f"KB load failed: {e}")
        return ""

async def load_recent_tours_context(max_rows: int = 12, hours: int = 120) -> str:
    """
    Короткий контекст для ИИ: последние туры из БД (чтобы ответы были "про сегодня").
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT country, city, hotel, COALESCE(board, '') AS board, COALESCE(includes, '') AS includes,
                       price, currency, dates, posted_at
                FROM tours
                WHERE posted_at >= %s
                ORDER BY posted_at DESC
                LIMIT %s
            """, (cutoff, max_rows))
            rows = cur.fetchall()
        lines = []
        for r in rows:
            when = localize_dt(r.get("posted_at"))  # уже «🕒 DD.MM.YYYY HH:MM (TST)»
            price = fmt_price(r.get("price"), r.get("currency"))
            hotel = clean_text_basic(strip_trailing_price_from_hotel(r.get("hotel") or "Пакетный тур"))
            board = (r.get("board") or "").strip()
            inc = (r.get("includes") or "").strip()
            extra = []
            if board: extra.append(f"питание: {board}")
            if inc:   extra.append(f"включено: {inc}")
            extra_txt = f" ({'; '.join(extra)})" if extra else ""
            lines.append(f"- {r.get('country')} — {r.get('city')}, {hotel}, {price}, даты: {r.get('dates') or '—'}{extra_txt}. {when}")
        return "\n".join(lines)
    except Exception as e:
        logging.warning(f"Recent context load failed: {e}")
        return ""

def append_lead_to_sheet(lead_id: int, user, phone: str, t: dict):
    """Добавляет заявку в лист (WORKSHEET_NAME, по умолчанию «Заявки»)."""
    try:
        gc = _get_gs_client()
        if not gc:
            return

        sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
        header = [
            "created_utc", "lead_id", "username", "full_name", "phone",
            "country", "city", "hotel", "price", "currency", "dates",
            "source_url", "posted_local", "board", "includes",
        ]
        ws = _ensure_ws(sh, WORKSHEET_NAME, header)
        _ensure_header(ws, header)

        full_name = f"{(getattr(user, 'first_name', '') or '').strip()} {(getattr(user, 'last_name', '') or '').strip()}".strip()
        username = f"@{user.username}" if getattr(user, "username", None) else ""
        posted_local = localize_dt(t.get("posted_at"))
        hotel_text = t.get("hotel") or derive_hotel_from_description(t.get("description")) or "Пакетный тур"
        hotel_clean = clean_text_basic(strip_trailing_price_from_hotel(hotel_text))

        ws.append_row([
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            int(lead_id),
            username,
            full_name,
            phone,
            t.get("country") or "",
            t.get("city") or "",
            hotel_clean,
            t.get("price") or "",
            (t.get("currency") or "").upper(),
            t.get("dates") or "",
            t.get("source_url") or "",
            posted_local,
            (t.get("board") or ""),
            (t.get("includes") or ""),
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"append_lead_to_sheet failed: {e}")

# ================= УТИЛИТЫ КОНФИГА =================
def resolve_leads_chat_id() -> int:
    val = get_config("LEADS_CHAT_ID", LEADS_CHAT_ID_ENV)
    try:
        return int(val) if val else 0
    except Exception:
        return 0

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
            [
                InlineKeyboardButton(text="💸 ≤ $500", callback_data="budget:USD:500"),
                InlineKeyboardButton(text="💸 ≤ $800", callback_data="budget:USD:800"),
                InlineKeyboardButton(text="💸 ≤ $1000", callback_data="budget:USD:1000"),
            ],
            [InlineKeyboardButton(text="↕️ Сортировка по цене", callback_data="sort:price_asc")],
            [InlineKeyboardButton(text="➕ Ещё фильтры скоро", callback_data="noop")],
        ]
    )

def more_kb(token: str, next_offset: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➡️ Показать ещё", callback_data=f"more:{token}:{next_offset}")],
            [InlineKeyboardButton(text="⬅️ Назад к фильтрам", callback_data="back_filters")],
        ]
    )

def want_contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📲 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
    )

# ================= ПАГИНАЦИЯ =================
def _new_token() -> str:
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

# ================= ФОРМАТЫ =================
def fmt_price(price, currency) -> str:
    if price is None:
        return "—"
    try:
        p = int(float(price))
    except Exception:
        return escape(f"{price} {currency or ''}".strip())
    cur = (currency or "").strip().upper()
    if cur in {"$", "US$", "USD$", "USD"}:
        cur = "USD"
    elif cur in {"€", "EUR€", "EUR"}:
        cur = "EUR"
    elif cur in {"UZS", "СУМ", "СУМ.", "СУМЫ", "СУМОВ", "СУММ", "СУММЫ", "СОМ", "СУМ"}:
        cur = "UZS"
    return escape(f"{p:,} {cur}".replace(",", " "))

def safe(s: Optional[str]) -> str:
    return escape(s or "—")

# ================= ПОГОДА (Open-Meteo) =================
WEATHER_CACHE: Dict[str, Tuple[float, Dict]] = {}
WEATHER_TTL = 900  # 15 минут

WMO_RU = {
    0: "Ясно ☀️", 1: "Преимущественно ясно 🌤", 2: "Переменная облачность ⛅️", 3: "Облачно ☁️",
    45: "Туман 🌫", 48: "Гололёдный туман 🌫❄️",
    51: "Морось слабая 🌦", 53: "Морось умеренная 🌦", 55: "Морось сильная 🌧",
    61: "Дождь слабый 🌦", 63: "Дождь умеренный 🌧", 65: "Дождь сильный 🌧",
    66: "Ледяной дождь слабый 🌧❄️", 67: "Ледяной дождь сильный 🌧❄️",
    71: "Снег слабый ❄️", 73: "Снег умеренный ❄️", 75: "Снег сильный ❄️",
    77: "Снежная крупа 🌨",
    80: "Ливни слабые 🌦", 81: "Ливни умеренные 🌧", 82: "Ливни сильные 🌧",
    85: "Снегопад слабый 🌨", 86: "Снегопад сильный 🌨",
    95: "Гроза ⛈", 96: "Гроза с градом ⛈🧊", 99: "Сильная гроза с градом ⛈🧊",
}

def _cleanup_weather_cache():
    now = time.time()
    for k, (ts, _) in list(WEATHER_CACHE.items()):
        if now - ts > WEATHER_TTL:
            WEATHER_CACHE.pop(k, None)

def _extract_place_from_weather_query(q: str) -> Optional[str]:
    """
    Берём локацию после предлогов: 'на', 'в', 'во', 'по'.
    Примеры: 'Какая погода на Бали сегодня?' -> 'Бали'
             'погода в Стамбуле' -> 'Стамбул'
    """
    txt = q.strip()
    # убираем служебные слова
    txt = re.sub(r"(сегодня|сейчас|завтра|пожалуйста|pls|please)", "", txt, flags=re.I)
    m = re.search(r"(?:на|в|во|по)\s+([A-Za-zА-Яа-яЁё\-\s]+)", txt, flags=re.I)
    if not m:
        # fallback: после слова "погода"
        m = re.search(r"погод[ауые]\s+([A-Za-zА-Яа-яЁё\-\s]+)", txt, flags=re.I)
    if not m:
        return None
    place = m.group(1)
    place = re.sub(r"[?!.,:;]+$", "", place).strip()
    # уберём хвост типа "сегодня", если остался
    place = re.sub(r"\b(сегодня|завтра|сейчас)\b", "", place, flags=re.I).strip()
    # короткие слова-приставки
    place = re.sub(r"^остров[аеуы]?\s+", "", place, flags=re.I)
    return place or None

async def get_weather_text(place: str) -> str:
    """
    Ищем координаты через geocoding.open-meteo.com, затем текущую погоду через api.open-meteo.com
    Возвращаем готовый текст для отправки в чат.
    """
    if not place:
        return "Напиши город/место: например, «погода в Стамбуле» или «погода на Бали»."

    key = place.lower().strip()
    _cleanup_weather_cache()
    if key in WEATHER_CACHE:
        _, cached = WEATHER_CACHE[key]
        return cached["text"]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 1) Геокод
            geo_r = await client.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": place, "count": 1, "language": "ru"}
            )
            if geo_r.status_code != 200 or not geo_r.json().get("results"):
                return f"Не нашёл локацию «{escape(place)}». Попробуй иначе (город/остров/страна)."

            g = geo_r.json()["results"][0]
            lat, lon = g["latitude"], g["longitude"]
            label_parts = [g.get("name")]
            if g.get("admin1"): label_parts.append(g["admin1"])
            if g.get("country"): label_parts.append(g["country"])
            label = ", ".join([p for p in label_parts if p])

            # 2) Погода
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
                "hourly": "precipitation_probability",
                "timezone": "auto",
            }
            w_r = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            if w_r.status_code != 200:
                return f"Не удалось получить погоду для «{escape(label)}». Попробуй позже."

            data = w_r.json()
            cur = data.get("current", {})
            code = int(cur.get("weather_code", 0))
            desc = WMO_RU.get(code, "Погода")
            t = cur.get("temperature_2m")
            feels = cur.get("apparent_temperature")
            rh = cur.get("relative_humidity_2m")
            wind = cur.get("wind_speed_10m")
            # вероятность осадков за сегодня (по локальному времени локации)
            prob = None
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            probs = hourly.get("precipitation_probability", [])
            if times and probs:
                today = (datetime.now(timezone.utc).astimezone()).strftime("%Y-%m-%d")
                prob = max((p for t, p in zip(times, probs) if t.startswith(today)), default=None)

            parts = [f"Погода: <b>{escape(label)}</b>", desc]
            if t is not None:
                tmp = f"{t:.0f}°C"
                if feels is not None and abs(feels - t) >= 1:
                    tmp += f" (ощущается как {feels:.0f}°C)"
                parts.append(f"Сейчас: {tmp}")
            if rh is not None:
                parts.append(f"Влажность: {int(rh)}%")
            if wind is not None:
                parts.append(f"Ветер: {wind:.1f} м/с")
            if prob is not None:
                parts.append(f"Вероятность осадков сегодня: {int(prob)}%")

            txt = " | ".join(parts)
            WEATHER_CACHE[key] = (time.time(), {"text": txt})
            return txt
    except Exception as e:
        logging.warning(f"get_weather_text failed: {e}")
        return "Не удалось получить данные о погоде. Попробуй ещё раз чуть позже."

def clean_text_basic(s: Optional[str]) -> str:
    if not s:
        return "—"
    s = re.sub(r'[*_`]+', '', s)
    s = s.replace('|', ' ')
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

def strip_trailing_price_from_hotel(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return re.sub(
        r'[\s\u00A0–—-]*(?:от\s*)?\d[\d\s\u00A0.,]*\s*(?:USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€).*$',
        '',
        s,
        flags=re.I
    ).strip()

def normalize_dates_for_display(s: Optional[str]) -> str:
    if not s:
        return "—"
    s = s.strip()
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})\s*[–-]\s*(\d{1,2})\.(\d{1,2})\.(\d{2,4})", s)
    if not m:
        return escape(s)
    d1, m1, y1, d2, m2, y2 = m.groups()
    def _norm(d, mo, y):
        d = int(d); mo = int(mo); y = int(y)
        if y < 100: y += 2000 if y < 70 else 1900
        if mo > 12 and d <= 12: d, mo = mo, d
        return f"{d:02d}.{mo:02d}.{y:04d}"
    return f"{_norm(d1, m1, y1)}–{_norm(d2, m2, y2)}"

def localize_dt(dt: Optional[datetime]) -> str:
    if not isinstance(dt, datetime):
        return ""
    try:
        dt_local = dt if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC"))
        dt_local = dt_local.astimezone(TZ)
        return f"🕒 {dt_local.strftime('%d.%m.%Y %H:%M')} (TST)"
    except Exception:
        return f"🕒 {dt.strftime('%d.%m.%Y %H:%M')}"

# ====== ДОП. ФОЛЛБЭКИ ======
CONTACT_STOP_WORDS = (
    "заброниров", "брониров", "звоните", "тел:", "телефон", "whatsapp", "вацап",
    "менеджер", "директ", "адрес", "@", "+998", "+7", "+380", "call-центр", "колл-центр"
)

def derive_hotel_from_description(desc: Optional[str]) -> Optional[str]:
    if not desc:
        return None
    for raw in desc.splitlines():
        line = raw.strip(" •–—-")
        if not line or len(line) < 6:
            continue
        low = line.lower()
        if any(sw in low for sw in CONTACT_STOP_WORDS):
            break
        if re.search(r"\b(\d{3,5}\s?(usd|eur|uzs)|\d+д|\d+н|all ?inclusive|ai|hb|bb|fb)\b", low, re.I):
            pass
        line = re.sub(r"^[\W_]{0,3}", "", line).strip()
        return line[:80]
    return None

def extract_meal(text_a: Optional[str], text_b: Optional[str] = None) -> Optional[str]:
    joined = " ".join([t or "" for t in (text_a, text_b)]).lower()
    if re.search(r"\buai\b|ultra\s*all", joined): return "UAI (ultra)"
    if re.search(r"\bai\b|all\s*inclusive|всё включено|все включено", joined): return "AI (всё включено)"
    if re.search(r"\bhb\b|полупанси", joined): return "HB (полупансион)"
    if re.search(r"\bbb\b|завтра(к|ки)", joined): return "BB (завтраки)"
    if re.search(r"\bfb\b|полный\s*панс", joined): return "FB (полный)"
    return None

# ================= ДБ-ХЕЛПЕРЫ =================
def is_favorite(user_id: int, tour_id: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM favorites WHERE user_id=%s AND tour_id=%s LIMIT 1;", (user_id, tour_id))
        return cur.fetchone() is not None

def set_favorite(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO favorites(user_id, tour_id) VALUES (%s, %s)
            ON CONFLICT (user_id, tour_id) DO NOTHING;
        """, (user_id, tour_id))

def unset_favorite(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM favorites WHERE user_id=%s AND tour_id=%s;", (user_id, tour_id))

def create_lead(tour_id: int, phone: Optional[str], full_name: str, note: Optional[str] = None):
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO leads (full_name, phone, tour_id, note)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """, (full_name, phone, tour_id, note))
            row = cur.fetchone()
            return row["id"] if row else None
    except Exception as e:
        logging.error(f"create_lead failed: {e}")
        return None

def _tours_has_cols(*cols: str) -> Dict[str, bool]:
    """Проверяем, существуют ли колонки в таблице tours (на любом инстансе БД)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'tours'
        """)
        have = {r["column_name"] for r in cur.fetchall()}
    return {c: (c in have) for c in cols}

async def load_recent_context(limit: int = 6) -> str:
    """
    Собирает короткий текст по последним турам для подмешивания в GPT.
    Работает даже если нет колонок board/includes.
    """
    try:
        flags = _tours_has_cols("board", "includes", "price", "currency", "dates", "hotel", "city", "country")
        select_parts = ["country", "city", "COALESCE(hotel,'') AS hotel"]
        select_parts.append("price" if flags["price"] else "NULL::numeric AS price")
        select_parts.append("currency" if flags["currency"] else "NULL::text AS currency")
        select_parts.append("COALESCE(dates,'') AS dates" if flags["dates"] else "'' AS dates")
        select_parts.append("COALESCE(board,'') AS board" if flags["board"] else "'' AS board")
        select_parts.append("COALESCE(includes,'') AS includes" if flags["includes"] else "'' AS includes")

        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM tours
            ORDER BY posted_at DESC NULLS LAST
            LIMIT %s
        """
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

        lines = []
        for r in rows:
            price = fmt_price(r.get("price"), r.get("currency")) if r.get("price") is not None else "цена уточняется"
            hotel = clean_text_basic(strip_trailing_price_from_hotel(r.get("hotel"))) if r.get("hotel") else "пакетный тур"
            parts = [
                f"{r.get('country') or '—'} — {r.get('city') or '—'}",
                f"{hotel}",
                f"{price}",
            ]
            if r.get("dates"):     parts.append(f"даты: {normalize_dates_for_display(r.get('dates'))}")
            if r.get("board"):     parts.append(f"питание: {r.get('board')}")
            if r.get("includes"):  parts.append(f"включено: {r.get('includes')}")
            lines.append(" • ".join(parts))
        return "\n".join(lines)
    except Exception as e:
        logging.warning(f"Recent context load failed: {e}")
        return ""

def set_pending_want(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pending_wants(user_id, tour_id) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET tour_id = EXCLUDED.tour_id, created_at = now();
        """, (user_id, tour_id))

def get_pending_want(user_id: int) -> Optional[int]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT tour_id FROM pending_wants WHERE user_id=%s;", (user_id,))
        row = cur.fetchone()
        return row["tour_id"] if row else None

def del_pending_want(user_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM pending_wants WHERE user_id=%s;", (user_id,))

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
    try:
        where_clauses = []
        params = []

        if query:
            where_clauses.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s OR description ILIKE %s)")
            params += [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%"]
        if country:
            where_clauses.append("country ILIKE %s")
            params.append(f"%{country}%")
        if currency_eq:
            where_clauses.append("currency = %s")
            params.append(currency_eq)
        if max_price is not None:
            where_clauses.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        order_clause = "ORDER BY price ASC NULLS LAST, posted_at DESC" if max_price is not None else "ORDER BY posted_at DESC"

        select_list = _select_tours_clause()

        with get_conn() as conn, conn.cursor() as cur:
            sql_recent = f"""
                SELECT {select_list}
                FROM tours
                {where_sql} {('AND' if where_sql else 'WHERE')} posted_at >= %s
                {order_clause}
                LIMIT %s
            """
            cur.execute(sql_recent, params + [cutoff, limit_recent])
            rows = cur.fetchall()
            if rows:
                return rows, True

            sql_fb = f"""
                SELECT {select_list}
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
    hours: Optional[int] = None,
    order_by_price: bool = False,
    limit: int = 10,
    offset: int = 0,
) -> List[dict]:
    try:
        where_clauses: List[str] = []
        params: List = []

        if query:
            where_clauses.append(
                "(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s OR description ILIKE %s)"
            )
            params += [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%"]

        if country:
            where_clauses.append("country ILIKE %s")
            params.append(f"%{country}%")

        if currency_eq:
            where_clauses.append("currency = %s")
            params.append(currency_eq)

        if max_price is not None:
            where_clauses.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)

        if hours is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            where_clauses.append("posted_at >= %s")
            params.append(cutoff)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        order_clause = (
            "ORDER BY price ASC NULLS LAST, posted_at DESC"
            if order_by_price
            else "ORDER BY posted_at DESC"
        )

        select_list = _select_tours_clause()
        sql = f"""
            SELECT {select_list}
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
last_gpt_call = defaultdict(float)

async def ask_gpt(prompt: str, *, user_id: int, premium: bool = False) -> List[str]:
    now = time.monotonic()
    if now - last_gpt_call[user_id] < 12.0:
        return ["😮‍💨 Подожди пару секунд — я ещё обрабатываю твой предыдущий запрос."]

    last_gpt_call[user_id] = now

    kb_text = await load_kb_context(max_rows=80)
    recent_text = await load_recent_tours_context(max_rows=12, hours=120)

    system_text = (
        "Ты — AI-консультант по путешествиям из экосистемы TripleA. "
        "Отвечай дружелюбно, коротко и по делу. Держись тематики: туры, отели, сезоны, визы, цены, лайфхаки. "
        f"Считай текущую дату/время: {datetime.now(TZ).strftime('%d.%m.%Y %H:%M %Z')}. "
        "Если есть блоки «АКТУАЛЬНЫЕ ФАКТЫ» и/или «СВЕЖИЕ ТУРЫ», в первую очередь опирайся на них. "
        "Не упоминай дату среза обучения модели; отвечай по текущему контексту."
    )

    blocks = []
    if kb_text:
        blocks.append(f"АКТУАЛЬНЫЕ ФАКТЫ:\n{kb_text}")
    if recent_text:
        blocks.append(f"СВЕЖИЕ ТУРЫ (последние):\n{recent_text}")
    user_content = "\n\n".join(blocks) + f"\n\nВОПРОС ПОЛЬЗОВАТЕЛЯ:\n{prompt}" if blocks else prompt

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.5,
        "max_tokens": 750,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for attempt in range(5):
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
                    hint = ""
                    if premium:
                        hint = "\n\n🔗 Источники доступны: канал(ы) партнёров и база свежих объявлений."
                    else:
                        if _should_hint_premium(user_id):
                            hint = "\n\n✨ Нужны прямые ссылки на посты-источники? Подключи Premium доступ TripleA."
                    answer += hint

                    MAX_LEN = 3800
                    return [answer[i:i+MAX_LEN] for i in range(0, len(answer), MAX_LEN)]

                if r.status_code in (429, 500, 502, 503, 504):
                    delay = min(20.0, (2 ** attempt) + random.random())
                    await asyncio.sleep(delay)
                    continue

                logging.error(f"OpenAI error {r.status_code}: {r.text[:400]}")
                break
    except Exception as e:
        logging.exception(f"GPT call failed: {e}")

    return [
        "⚠️ ИИ сейчас перегружен. Попробуй ещё раз — а пока загляни в «🎒 Найти туры»: там только свежие предложения за последние 72 часа."
    ]

# ================= КАРТОЧКИ/УВЕДОМЛЕНИЯ =================
def tour_inline_kb(t: dict, is_fav: bool) -> InlineKeyboardMarkup:
    open_btn = []
    url = (t.get("source_url") or "").strip()
    if url:
        open_btn = [InlineKeyboardButton(text="🔗 Открыть", url=url)]

    fav_btn = InlineKeyboardButton(
        text=("❤️ В избранном" if is_fav else "🤍 В избранное"),
        callback_data=f"fav:{'rm' if is_fav else 'add'}:{t['id']}"
    )
    want_btn = InlineKeyboardButton(text="📝 Хочу этот тур", callback_data=f"want:{t['id']}")

    rows = []
    if open_btn:
        rows.append(open_btn)
    rows.append([fav_btn, want_btn])
    rows.append([InlineKeyboardButton(text="⬅️ К фильтрам", callback_data="back_filters")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_card_text(t: dict) -> str:
    price_str = fmt_price(t.get("price"), t.get("currency"))

    # hotel: из БД, если нет — из описания, иначе fallback
    hotel_text = t.get("hotel") or derive_hotel_from_description(t.get("description"))
    hotel_clean = clean_text_basic(strip_trailing_price_from_hotel(hotel_text)) if hotel_text else "Пакетный тур"

    # питание: сначала из БД (collector), если нет — эвристика
    board = (t.get("board") or "").strip()
    if not board:
        board = extract_meal(t.get("hotel"), t.get("description")) or ""

    # что включено: из БД (collector)
    includes = (t.get("includes") or "").strip()

    dates_norm = normalize_dates_for_display(t.get("dates"))
    time_str = localize_dt(t.get("posted_at"))
    url = (t.get("source_url") or "").strip()

    parts = [
        f"🌍 {safe(t.get('country'))} — {safe(t.get('city'))}",
        f"🏨 {safe(hotel_clean)}",
        f"💵 {price_str}",
        f"📅 {dates_norm}",
    ]
    if board:
        parts.append(f"🍽 Питание: <b>{escape(board)}</b>")
    if includes:
        parts.append(f"✅ Включено: {escape(includes)}")
    if not url:
        parts.append("ℹ️ Источник без прямой ссылки. Могу прислать краткую справку по посту.")
    if time_str:
        parts.append(time_str)

    return "\n".join(parts)

async def send_tour_card(chat_id: int, user_id: int, t: dict):
    fav = is_favorite(user_id, t["id"])
    kb = tour_inline_kb(t, fav)
    caption = build_card_text(t)
    await bot.send_message(chat_id, caption, reply_markup=kb, disable_web_page_preview=True)

async def send_batch_cards(chat_id: int, user_id: int, rows: List[dict], token: str, next_offset: int):
    for t in rows:
        await send_tour_card(chat_id, user_id, t)
        await asyncio.sleep(0)
    # запоминаем результаты для "дай ссылку"
    LAST_RESULTS[user_id] = rows
    LAST_QUERY_AT[user_id] = time.monotonic()

    await bot.send_message(chat_id, "Продолжить подборку?", reply_markup=more_kb(token, next_offset))

async def notify_leads_group(t: dict, *, lead_id: int, user, phone: str, pin: bool = False):
    """Отправляет карточку лида в группу заявок (поддерживает темы). Не показываем user_id."""
    chat_id = resolve_leads_chat_id()
    if not chat_id:
        logging.warning("notify_leads_group: LEADS_CHAT_ID не задан")
        return

    try:
        price_str = fmt_price(t.get("price"), t.get("currency"))
        hotel_text = t.get("hotel") or derive_hotel_from_description(t.get("description"))
        hotel_clean = clean_text_basic(strip_trailing_price_from_hotel(hotel_text)) if hotel_text else "Пакетный тур"
        dates_norm = normalize_dates_for_display(t.get("dates"))
        time_str = localize_dt(t.get("posted_at"))

        # 👇 ТОЛЬКО username (или Имя Фамилия, если username нет)
        if getattr(user, "username", None):
            user_label = f"@{user.username}"
        else:
            user_label = f"{(user.first_name or '')} {(user.last_name or '')}".strip() or "Гость"

        src = (t.get("source_url") or "").strip()
        src_line = f'\n🔗 <a href="{escape(src)}">Источник</a>' if src else ""

        board = (t.get("board") or "").strip()
        includes = (t.get("includes") or "").strip()

        board_line = f"\n🍽 Питание: {escape(board)}" if board else ""
        incl_line = f"\n✅ Включено: {escape(includes)}" if includes else ""

        text = (
            f"🆕 <b>Заявка №{lead_id}</b>\n"
            f"👤 {escape(user_label)}\n"
            f"📞 {escape(phone)}\n"
            f"🌍 {safe(t.get('country'))} — {safe(t.get('city'))}\n"
            f"🏨 {safe(hotel_clean)}\n"
            f"💵 {price_str}\n"
            f"📅 {dates_norm}\n"
            f"{time_str}"
            f"{board_line}{incl_line}"
            f"{src_line}"
        ).strip()

        kwargs = {}
        if LEADS_TOPIC_ID:
            kwargs["message_thread_id"] = LEADS_TOPIC_ID

        photo = (t.get("photo_url") or "").strip()
        if photo:
            short = text if len(text) <= 1000 else (text[:990].rstrip() + "…")
            msg = await bot.send_photo(chat_id, photo=photo, caption=short, parse_mode="HTML", **kwargs)
        else:
            msg = await bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True, **kwargs)

        if pin:
            try:
                await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
            except Exception as e:
                logging.warning(f"pin failed: {e}")
    except Exception as e:
        logging.error(f"notify_leads_group failed: {e}")

# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "🌍 Привет! Я — <b>TripleA Travel Bot</b> ✈️\n\n"
        "Нажимай «🎒 Найти туры» — покажу карточки с кнопками.\n"
        "«🤖 Спросить GPT» — умные ответы про сезоны, бюджеты и лайфхаки.\n"
    )
    await message.answer(text, reply_markup=main_kb)

@dp.message(F.text.regexp(r"насколько.*актуал", flags=re.I))
async def faq_actual(message: Message):
    txt = (
        "Актуальность:\n"
        "• В подборках показываем свежие туры за последние 72 часа (в карточке есть время публикации).\n"
        "• ИИ использует базу последних объявлений и лист KB — поэтому отвечает «на сегодня», без старых справок.\n"
        "Если нужно — напиши страну и бюджет, соберу варианты ✈️"
    )
    await message.answer(txt)

@dp.message(Command("chatid"))
async def cmd_chatid(message: Message):
    await message.reply(f"chat_id: {message.chat.id}\nthread_id: {getattr(message, 'message_thread_id', None)}")

@dp.message(Command("setleadgroup"))
async def cmd_setleadgroup(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.reply("Недостаточно прав.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Использование: /setleadgroup -100xxxxxxxxxx")
        return
    new_id = parts[1].strip()
    try:
        int(new_id)
    except:
        await message.reply("Неверный chat_id.")
        return
    set_config("LEADS_CHAT_ID", new_id)
    await message.reply(f"LEADS_CHAT_ID обновлён: {new_id}")

@dp.message(Command("leadstest"))
async def cmd_leadstest(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.reply("Недостаточно прав.")
        return
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours ORDER BY posted_at DESC LIMIT 1;")
        t = cur.fetchone()
    if not t:
        await message.reply("В базе нет туров для теста.")
        return
    fake_lead_id = 9999
    await notify_leads_group(t, lead_id=fake_lead_id, user=message.from_user, phone="+99890XXXXXXX", pin=False)
    await message.reply("Тестовая заявка отправлена в группу.")

@dp.message(F.text == "🎒 Найти туры")
async def entry_find_tours(message: Message):
    await message.answer("Выбери быстрый фильтр:", reply_markup=filters_inline_kb())

@dp.message(F.text == "🤖 Спросить GPT")
async def entry_gpt(message: Message):
    await message.answer("Спроси что угодно про путешествия (отели, сезоны, визы, бюджеты).")

@dp.message(F.text == "🔔 Подписка")
async def entry_sub(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Click (автопродление)", callback_data="sub:click:recurring"),
            InlineKeyboardButton(text="💳 Payme (автопродление)", callback_data="sub:payme:recurring"),
        ],
        [
            InlineKeyboardButton(text="Разовая оплата через Click", callback_data="sub:click:oneoff"),
            InlineKeyboardButton(text="Разовая оплата через Payme", callback_data="sub:payme:oneoff"),
        ],
        [
            InlineKeyboardButton(text="ℹ️ Подробнее о тарифах", callback_data="sub:info")
        ]
    ])
    await message.answer(
        "Выбери способ оплаты и тариф (по умолчанию — <b>Basic 49 000 UZS / 30 дней</b>):",
        reply_markup=kb
    )

@dp.message(F.text == "⚙️ Настройки")
async def entry_settings(message: Message):
    await message.answer("Скоро: язык/валюта/бюджет по умолчанию. Пока в разработке ⚙️")

@dp.callback_query(F.data == "tours_recent")
async def cb_recent(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    rows, is_recent = await fetch_tours(None, hours=72, limit_recent=6, limit_fallback=6)
    header = "🔥 Актуальные за 72 часа" if is_recent else "ℹ️ Свежих 72ч мало — показываю последние"
    await call.message.answer(f"<b>{header}</b>")

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

    _remember_query(call.from_user.id, "актуальные за 72ч")
    next_offset = len(rows)
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, next_offset)

@dp.callback_query(F.data.startswith("country:"))
async def cb_country(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    country = call.data.split(":", 1)[1]
    rows, is_recent = await fetch_tours(None, country=country, hours=120, limit_recent=6, limit_fallback=6)
    header = f"🇺🇳 Страна: {country} — актуальные" if is_recent else f"🇺🇳 Страна: {country} — последние найденные"
    await call.message.answer(f"<b>{escape(header)}</b>")

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

    _remember_query(call.from_user.id, country)
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, len(rows))

@dp.callback_query(F.data.startswith("sub:"))
async def cb_sub(call: CallbackQuery):
    _, provider, kind = call.data.split(":", 2)   # provider: click|payme; kind: recurring|oneoff
    plan_code = "basic_m"                         # можно дать выбор планов по кнопкам
    order_id = create_order(call.from_user.id, provider=provider, plan_code=plan_code, kind=kind)
    url = build_checkout_link(provider, order_id, plan_code)

    txt = (
        f"🔐 Заказ №{order_id}\n"
        f"Провайдер: <b>{'Click' if provider=='click' else 'Payme'}</b>\n"
        f"Тариф: <b>Basic</b> (30 дней)\n\n"
        "Нажми, чтобы оплатить. Окно откроется прямо в Telegram."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть оплату", url=url)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ])
    await call.message.answer(txt, reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "sub:info")
async def cb_sub_info(call: CallbackQuery):
    await call.message.answer(
        "Тарифы:\n"
        "• Basic — 49 000 UZS/мес: доступ к свежим турам и умным ответам\n"
        "• Pro — 99 000 UZS/мес: приоритет и расширенные источники\n\n"
        "Оплата через Click/Payme. Автопродление можно отключить в любой момент."
    )
    await call.answer()

@dp.callback_query(F.data.startswith("budget:"))
async def cb_budget(call: CallbackQuery):
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
        limit_recent=6,
        limit_fallback=6
    )
    hdr = f"💸 Бюджет: ≤ {int(limit_val)} {cur} — актуальные" if is_recent else f"💸 Бюджет: ≤ {int(limit_val)} {cur} — последние найденные"
    await call.message.answer(f"<b>{escape(hdr)}</b>")

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

    _remember_query(call.from_user.id, f"≤ {int(limit_val) if limit_val is not None else ''} {cur}".strip())
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, len(rows))

@dp.callback_query(F.data == "sort:price_asc")
async def cb_sort_price_asc(call: CallbackQuery):
    await bot.send_chat_action(call.message.chat.id, "typing")
    rows = await fetch_tours_page(hours=72, order_by_price=True, limit=6, offset=0)
    await call.message.answer("<b>↕️ Актуальные за 72ч — дешевле → дороже</b>")

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

    _remember_query(call.from_user.id, "актуальные за 72ч (сорт. по цене)")
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, len(rows))

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
        limit=6,
        offset=offset,
    )
    if not rows:
        await call.answer("Это всё на сегодня ✨", show_alert=False)
        return

    _touch_state(token)
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, offset + len(rows))

@dp.callback_query(F.data.startswith("fav:add:"))
async def cb_fav_add(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[2])
    except Exception:
        await call.answer("Ошибка избранного.", show_alert=False); return
    set_favorite(call.from_user.id, tour_id)
    await call.answer("Добавлено в избранное ❤️", show_alert=False)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours WHERE id=%s;", (tour_id,))
        t = cur.fetchone()
    if t:
        await call.message.edit_reply_markup(reply_markup=tour_inline_kb(t, True))

@dp.callback_query(F.data.startswith("fav:rm:"))
async def cb_fav_rm(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[2])
    except Exception:
        await call.answer("Ошибка избранного.", show_alert=False); return
    unset_favorite(call.from_user.id, tour_id)
    await call.answer("Убрано из избранного 🤍", show_alert=False)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours WHERE id=%s;", (tour_id,))
        t = cur.fetchone()
    if t:
        await call.message.edit_reply_markup(reply_markup=tour_inline_kb(t, False))

@dp.callback_query(F.data.startswith("want:"))
async def cb_want(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[1])
    except Exception:
        await call.answer("Ошибка заявки.", show_alert=False); return

    WANT_STATE[call.from_user.id] = {"tour_id": tour_id}
    try:
        set_pending_want(call.from_user.id, tour_id)
    except Exception as e:
        logging.warning(f"set_pending_want failed: {e}")

    await call.message.answer(
        "Окей! Отправь контакт, чтобы менеджер связался. Нажми кнопку ниже 👇",
        reply_markup=want_contact_kb()
    )
    await call.answer()

@dp.message(Command("weather"))
async def cmd_weather(message: Message):
    ask = (message.text or "").partition(" ")[2].strip()
    place = ask or "Ташкент"
    await message.answer("Секунду, уточняю погоду…")
    txt = await get_weather_text(place)
    await message.answer(txt, disable_web_page_preview=True)

@dp.message(F.contact)
async def on_contact(message: Message):
    st = WANT_STATE.pop(message.from_user.id, None)
    if not st:
        logging.info(f"Contact came without pending want (user_id={message.from_user.id})")
        await message.answer("Контакт получен. Если нужен подбор, нажми «🎒 Найти туры».", reply_markup=main_kb)
        return

    phone = message.contact.phone_number
    tour_id = st["tour_id"]

    # Безопасно формируем ФИО
    full_name = (getattr(message.from_user, "full_name", "") or "").strip()
    if not full_name:
        parts = [(message.from_user.first_name or ""), (message.from_user.last_name or "")]
        full_name = " ".join(p for p in parts if p).strip() or \
                    (f"@{message.from_user.username}" if message.from_user.username else "Telegram user")

    lead_id = create_lead(tour_id, phone, full_name, note="from contact share")

    # подтянем тур и отправим в группу заявок
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT {_select_tours_clause()}
            FROM tours WHERE id=%s;
        """, (tour_id,))
        t = cur.fetchone()

    if t and lead_id:
        await notify_leads_group(t, lead_id=lead_id, user=message.from_user, phone=phone, pin=False)
        append_lead_to_sheet(lead_id, message.from_user, phone, t)
        await message.answer(f"Принято! Заявка №{lead_id}. Менеджер скоро свяжется 📞", reply_markup=main_kb)
    else:
        await message.answer("Контакт получен, но не удалось создать заявку. Попробуй ещё раз или напиши менеджеру.", reply_markup=main_kb)

@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer("Скоро добавим детальные фильтры 🤝", show_alert=False)

@dp.callback_query(F.data == "back_filters")
async def cb_back_filters(call: CallbackQuery):
    await call.message.answer("Вернулся к фильтрам:", reply_markup=filters_inline_kb())

@dp.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    await call.message.answer("Главное меню:", reply_markup=main_kb)

# --- Смарт-роутер текста
@dp.message(F.text & ~F.text.in_({"🎒 Найти туры", "🤖 Спросить GPT", "🔔 Подписка", "⚙️ Настройки"}))
async def smart_router(message: Message):
    user_text = (message.text or "").strip()
    await bot.send_chat_action(message.chat.id, "typing")

    # --- 1) "дай ссылку", "источник", "ссылку на источник"
    if re.search(r"\b((дай\s+)?ссылк\w*|источник\w*|link)\b", user_text, flags=re.I):
        last = LAST_RESULTS.get(message.from_user.id) or []
        premium_users = {123456789}
        is_premium = message.from_user.id in premium_users

        # если карточек нет — пробуем реконструировать запрос и найти их сейчас
        if not last:
            guess = _guess_query_from_link_phrase(user_text) or LAST_QUERY_TEXT.get(message.from_user.id)
            if guess:
                rows, _is_recent = await fetch_tours(guess, hours=168, limit_recent=6, limit_fallback=6)
                if rows:
                    LAST_RESULTS[message.from_user.id] = rows
                    last = rows

        if not last:
            q_hint = LAST_QUERY_TEXT.get(message.from_user.id)
            hint_txt = f"По последнему запросу «{escape(q_hint)}» ничего свежего не нашёл." if q_hint else "Не вижу последних карточек."
            await message.answer(
                f"{hint_txt} Нажми «🎒 Найти туры» и выбери вариант — тогда пришлю источник.",
                reply_markup=filters_inline_kb()
            )
            return

        shown = 0
        for t in last[:3]:
            src = (t.get("source_url") or "").strip()
            if is_premium and src:
                text = f'🔗 Источник: <a href="{escape(src)}">перейти к посту</a>'
                await message.answer(text, disable_web_page_preview=True)
            else:
                ch = (t.get("source_chat") or "").lstrip("@")
                when = localize_dt(t.get("posted_at"))
                label = f"Источник: {escape(ch) or 'тур-канал'}, {when or 'дата неизвестна'}"
                hint = " • В Premium покажу прямую ссылку."
                await message.answer(f"{label}{hint}")
            shown += 1

        if shown == 0:
            await message.answer("Для этого набора источников прямых ссылок нет. Попробуй свежие туры через фильтры.")
        return

    # --- 2) Быстрый маршрут: погода
    if re.search(r"\bпогод", user_text, flags=re.I):
        place = _extract_place_from_weather_query(user_text)
        await message.answer("Секунду, уточняю погоду…")
        reply = await get_weather_text(place or "")
        await message.answer(reply, disable_web_page_preview=True)
        return

    # --- 3) Быстрый маршрут: "<что-то> интересует"
    m_interest = re.search(r"^(?:мне\s+)?(.+?)\s+интересует(?:\s*!)?$", user_text, flags=re.I)
    if m_interest or (len(user_text) <= 30):
        q = m_interest.group(1) if m_interest else user_text
        queries = _expand_query(q)
        rows_all: List[dict] = []

        for qx in queries:
            rows, _is_recent = await fetch_tours(qx, hours=72, limit_recent=6, limit_fallback=0)
            rows_all.extend(rows)

        if not rows_all:
            rows_all, _ = await fetch_tours(user_text, hours=168, limit_recent=0, limit_fallback=6)

        if rows_all:
            _remember_query(message.from_user.id, q)
            header = "Нашёл варианты по запросу: " + escape(q)
            await message.answer(f"<b>{header}</b>")
            token = _new_token()
            PAGER_STATE[token] = {
                "chat_id": message.chat.id,
                "query": q,
                "country": None,
                "currency_eq": None,
                "max_price": None,
                "hours": 72,
                "order_by_price": False,
                "ts": time.monotonic(),
            }
            await send_batch_cards(message.chat.id, message.from_user.id, rows_all[:6], token, len(rows_all[:6]))
            return

    # --- 4) Короткие запросы: быстрый поиск по базе за 72 часа
    if len(user_text) <= 40:
        rows, is_recent = await fetch_tours(user_text, hours=72)
        if rows:
            _remember_query(message.from_user.id, user_text)
            header = "🔥 Нашёл актуальные за 72 часа:" if is_recent else "ℹ️ Свежих 72ч нет — вот последние варианты:"
            await message.answer(f"<b>{header}</b>")
            token = _new_token()
            PAGER_STATE[token] = {
                "chat_id": message.chat.id,
                "query": user_text,
                "country": None,
                "currency_eq": None,
                "max_price": None,
                "hours": 72 if is_recent else None,
                "order_by_price": False,
                "ts": time.monotonic(),
            }
            await send_batch_cards(message.chat.id, message.from_user.id, rows, token, len(rows))
            return

    # --- 5) Фолбэк: GPT
    _remember_query(message.from_user.id, user_text)
    premium_users = {123456789}
    is_premium = message.from_user.id in premium_users
    replies = await ask_gpt(user_text, user_id=message.from_user.id, premium=is_premium)
    for part in replies:
        await message.answer(part, parse_mode=None)

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

    try:
        ensure_pending_wants_table()
        ensure_leads_schema()
    except Exception as e:
        logging.error(f"Schema ensure failed: {e}")

    # --- GS warmup (подготовим таблицу и лист "Заявки")
    try:
        gc = _get_gs_client()
        if not gc:
            logging.info("GS warmup skipped: client is None (нет кредов или ID)")
        else:
            sid = SHEETS_SPREADSHEET_ID or "(empty)"
            logging.info(f"GS warmup: trying open spreadsheet id='{sid}'")
            sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
            logging.info(f"GS warmup: opened spreadsheet title='{sh.title}'")

            # логируем существующие листы — удобно для диагностики
            try:
                titles = [ws.title for ws in sh.worksheets()]
                logging.info(f"GS warmup: worksheets={titles}")
            except Exception as e_list:
                logging.warning(f"GS: cannot list worksheets: {e_list}")

            header = [
                "created_utc", "lead_id", "username", "full_name", "phone",
                "country", "city", "hotel", "price", "currency", "dates",
                "source_url", "posted_local", "board", "includes",
            ]
            ws = _ensure_ws(sh, os.getenv("WORKSHEET_NAME", "Заявки"), header)
            _ensure_header(ws, header)
            logging.info(f"✅ GS warmup: лист '{ws.title}' готов (rows={ws.row_count}, cols={ws.col_count})")
    except gspread.SpreadsheetNotFound as e:
        logging.error(f"GS warmup failed: spreadsheet not found by id='{SHEETS_SPREADSHEET_ID}': {e}")
    except gspread.exceptions.APIError as e:
        logging.error(f"GS warmup failed (APIError): {e}")
    except Exception as e:
        logging.error(f"GS warmup failed (generic): {e}")

    # гарантируем новые колонки (безопасно, IF NOT EXISTS)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS board TEXT;")
            cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS includes TEXT;")
    except Exception as e:
        logging.warning(f"Ensure tours columns failed: {e}")

    # --- Снимем фактическую схему tours и DSN
    try:
        with get_conn() as conn, conn.cursor() as cur:
            info = conn.info
            logging.info(f"🗄 DB DSN: host={info.host} db={info.dbname} user={info.user} port={info.port}")
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'tours'
                ORDER BY ordinal_position
            """)
            cols = [r["column_name"] for r in cur.fetchall()]
            global SCHEMA_COLS
            SCHEMA_COLS = set(cols)
            logging.info(f"🎯 Колонки в таблице tours: {cols}")
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке колонок: {e}")

    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}")
    else:
        logging.warning("WEBHOOK_URL не указан — бот не получит апдейты.")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()

from fastapi import Form

@app.post("/click/callback")
async def click_cb(request: Request):
    form = dict(await request.form())
    ok, msg, order_id, trx = click_handle_callback(form)
    if ok and order_id:
        try:
            activate_after_payment(order_id)
            # уведомим пользователя
            o = get_order_safe(order_id)
            if o:
                await bot.send_message(o["user_id"], f"✔️ Оплата принята. Подписка активна до {fmt_sub_until(o['user_id'])}")
        except Exception as e:
            pass
    return JSONResponse({"status": "ok" if ok else "error", "message": msg})

@app.post("/payme/callback")
async def payme_cb(request: Request):
    # Payme Hosted обычно шлёт form-data; если JSON — поменяй на await request.json()
    form = dict(await request.form())
    ok, msg, order_id, trx = payme_handle_callback(form, dict(request.headers))
    if ok and order_id:
        try:
            activate_after_payment(order_id)
            o = get_order_safe(order_id)
            if o:
                await bot.send_message(o["user_id"], f"✔️ Оплата принята. Подписка активна до {fmt_sub_until(o['user_id'])}")
        except Exception as e:
            pass
    return JSONResponse({"status": "ok" if ok else "error", "message": msg})

