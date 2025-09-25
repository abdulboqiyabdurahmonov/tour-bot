# bot.py
import os
import re
import logging
import asyncio
import random
import time
import json, base64
from dotenv import load_dotenv
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from psycopg.rows import dict_row

load_dotenv()  # подхватит .env локально

# --- токен
TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TELEGRAM_TOKEN")   # поддержка всех вариантов
)

if not TOKEN:
    raise RuntimeError("BOT_TOKEN/TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN не задан")

# --- сторонние модули
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
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from payments import db as _pay_db  # реиспользуем подключение из слоя платежей

# --- aiogram
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

# --- psycopg
from psycopg import connect
from psycopg.rows import dict_row

# --- httpx и локальные утилиты
import httpx
from db_init import init_db, get_config, set_config  # конфиг из БД

# ================= ЛОГИ =================
logging.basicConfig(level=logging.INFO)

# ===== ПАМЯТЬ ДИАЛОГА =====
LAST_RESULTS: dict[int, list[dict]] = {}   # user_id -> последние показанные туры
LAST_QUERY_AT: dict[int, float] = {}       # user_id -> ts последнего показа
LAST_PREMIUM_HINT_AT: dict[int, float] = {}  # user_id -> ts последней плашки "премиум"
LAST_QUERY_TEXT: dict[int, str] = {}       # user_id -> последний смысловой запрос
ASK_STATE: Dict[int, Dict] = {}
# ключ -> {user_id, tour_id}
ANSWER_MAP: dict[str, dict] = {}

# Синонимы/алиасы гео (минимальный словарик)
ALIASES = {
    "фукуок": ["фукуок", "phu quoc", "phuquoc", "phú quốc"],
    "шарм": ["шарм", "sharm", "sharm el sheikh", "sharm-el-sheikh", "шарм-эль-шейх"],
    "дубай": ["дубай", "dubai", "dxб"],
    "нячанг": ["нячанг", "nha trang", "nhatrang"],
    "Китай": ["Хайнань", "Hainan", "Sanya", "三亚", "Haikou", "海口"],
    "Индонезия": ["Бали", "Bali", "Denpasar"],
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
    if not text:
        return None
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

# Расширения стран (используются в cb_country)
COUNTRY_EXPAND_ANY = {
    "Китай": ["Китай", "Хайнань", "Hainan", "Sanya", "三亚", "Haikou", "海口"],
    "Индонезия": ["Индонезия", "Бали", "Bali", "Denpasar"],
    "Таиланд": ["Таиланд", "Пхукет", "Phuket", "Самуи", "Koh Samui"],
    "Турция": ["Турция", "Анталья", "Antalya", "Аланья", "Alanya"],
    "ОАЭ": ["ОАЭ", "Дубай", "Dubai", "Абу-Даби", "Abu Dhabi"],
    "Вьетнам": ["Вьетнам", "Нячанг", "Nha Trang", "Фукуок", "Phu Quoc"],
    "Мальдивы": ["Мальдивы", "Мале", "Male"],
    "Грузия": ["Грузия", "Батуми", "Batumi", "Тбилиси", "Tbilisi"],
}

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
# --- Payme Merchant API (JSON-RPC) настройки ---
PAYME_MERCHANT_XAUTH = os.getenv("PAYME_MERCHANT_XAUTH", "").strip()
PAYME_MERCHANT_KEY = os.getenv("PAYME_MERCHANT_KEY", "")
def _payme_auth_ok(x_auth: str | None) -> bool:
    return bool(x_auth) and secrets.compare_digest(x_auth, PAYME_MERCHANT_KEY)

def _payme_sandbox_ok(req: Request) -> bool:
    ip = req.client.host if req.client else ""
    # IP-адреса песочницы, которые видим в логах
    return ip in {"185.234.113.15", "213.230.116.57"}

# ===== PAYME =====
PAYME_ACCOUNT_FIELD = os.getenv("PAYME_ACCOUNT_FIELD", "order_id").strip()
PAYME_MERCHANT_ID = (os.getenv("PAYME_MERCHANT_ID") or "").strip()
FISCAL_IKPU = os.getenv("FISCAL_IKPU", "00702001001000001")   # твой ИКПУ (можно тестовый)
FISCAL_VAT_PERCENT = int(os.getenv("FISCAL_VAT_PERCENT", "12"))

# Google Sheets ENV
SHEETS_CREDENTIALS_B64 = (os.getenv("SHEETS_CREDENTIALS_B64") or "").strip()
SHEETS_SPREADSHEET_ID = (os.getenv("SHEETS_SPREADSHEET_ID") or "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Заявки")
KB_SHEET_NAME = os.getenv("KB_SHEET_NAME", "KB")

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не найден в переменных окружения!")

def build_payme_checkout_url(merchant_id: str, amount_tiyin: int, order_id: int, lang: str = "ru") -> str:
    if not merchant_id:
        raise ValueError("PAYME_MERCHANT_ID пуст — не могу собрать ссылку")

    amt = int(round(float(amount_tiyin)))
    if amt <= 0:
        raise ValueError(f"Некорректная сумма для Payme (тийины): {amount_tiyin}")

    ac = {PAYME_ACCOUNT_FIELD: int(order_id)}

    payload = {"m": merchant_id, "a": amt, "ac": ac, "l": lang}
    token = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
    return f"https://checkout.paycom.uz/{token}"

# ================= ПАГИНАЦИЯ / ПОДБОРКИ =================
import time, secrets
from typing import Dict, Any, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

PAGER_STATE: Dict[str, Dict[str, Any]] = {}   # token -> state (у тебя уже объявлен)
PAGER_TTL_SEC = 3600                          # 1 час (у тебя уже объявлен)

def _new_token() -> str:
    return secrets.token_urlsafe(8)

def _touch_state(token: str) -> None:
    st = PAGER_STATE.get(token)
    if st is not None:
        st["ts"] = time.monotonic()

def _cleanup_pager_state() -> None:
    now = time.monotonic()
    dead = [t for t, st in PAGER_STATE.items() if (now - st.get("ts", now)) > PAGER_TTL_SEC]
    for t in dead:
        PAGER_STATE.pop(t, None)

# ================= СИНОНИМЫ СТРАН =================
COUNTRY_SYNONYMS = {
    "Турция":   ["Турция", "Turkey", "Türkiye"],
    "ОАЭ":      ["ОАЭ", "UAE", "United Arab Emirates", "Dubai", "Abu Dhabi"],
    "Таиланд":  ["Таиланд", "Thailand"],
    "Вьетнам":  ["Вьетнам", "Vietnam"],
    "Грузия":   ["Грузия", "Georgia", "Sakartvelo"],
    "Мальдивы": ["Мальдивы", "Maldives"],
    "Китай":    ["Китай", "China", "PRC", "People's Republic of China", "PR China", "КНР"],
}

def country_terms_for(user_pick: str) -> list[str]:
    base = normalize_country(user_pick)
    return COUNTRY_SYNONYMS.get(base, [base])

# ====== ЯЗЫКИ / ЛОКАЛИЗАЦИЯ ======
SUPPORTED_LANGS = ("ru", "uz", "kk")
DEFAULT_LANG = "ru"  # язык по умолчанию

TRANSLATIONS = {
    "ru": {
        "choose_lang": "Выберите язык обслуживания:",
        "lang_saved": "Готово! Язык сохранён.",
        "hello": "🌍 Привет! Я — <b>TripleA Travel Bot</b> ✈️",
        "menu_find": "🎒 Найти туры",
        "menu_gpt": "🤖 Спросить GPT",
        "menu_sub": "🔔 Подписка",
        "menu_settings": "🌐 Выбор языка",
        "desc_find": "— покажу карточки с кнопками.",
        "desc_gpt": "— умные ответы про сезоны, визы и бюджеты.",
        "back": "⬅️ Назад",
    },
    "uz": {
        "choose_lang": "Xizmat tilini tanlang:",
        "lang_saved": "Tayyor! Til saqlandi.",
        "hello": "🌍 Salom! Men — <b>TripleA Travel Bot</b> ✈️",
        "menu_find": "🎒 Turlarni topish",
        "menu_gpt": "🤖 GPTdan so'rash",
        "menu_sub": "🔔 Obuna",
        "menu_settings": "🌐 Tilni tanlash",
        "desc_find": "— tugmalar bilan kartochkalarni ko‘rsataman.",
        "desc_gpt": "— mavsumlar, vizalar va byudjetlar bo‘yicha aqlli javoblar.",
        "back": "⬅️ Orqaga",
    },
    "kk": {
        "choose_lang": "Қызмет көрсету тілін таңдаңыз:",
        "lang_saved": "Дайын! Тіл сақталды.",
        "hello": "🌍 Сәлем! Мен — <b>TripleA Travel Bot</b> ✈️",
        "menu_find": "🎒 Тур табу",
        "menu_gpt": "🤖 GPT-ке сұрақ",
        "menu_sub": "🔔 Жазылым",
        "menu_settings": "🌐 Тілді таңдау",
        "desc_find": "— батырмалармен карточкаларды көрсетемін.",
        "desc_gpt": "— маусымдар, визалар және бюджеттер туралы ақылды жауаптар.",
        "back": "⬅️ Артқа",
    },
}

# Дополнительные ключи для фильтров/«показать ещё»
TRANSLATIONS["ru"].update({
    "filters.title": "Выбери подборку:",
    "filters.recent": "🔥 Актуальные 72ч",
    "filters.country.turkiye": "🌴 Турция",
    "filters.country.uae": "🇦🇪 ОАЭ",
    "filters.country.th": "🇹🇭 Таиланд",
    "filters.country.vn": "🇻🇳 Вьетнам",
    "filters.budget.500": "💸 ≤ $500",
    "filters.budget.800": "💸 ≤ $800",
    "filters.budget.1000": "💸 ≤ $1000",
    "filters.sort.price": "↕️ Сортировка по цене",
    "filters.more": "➕ Ещё фильтры скоро",
    "more.title": "Продолжить подборку?",
    "more.next": "➡️ Показать ещё",
})

TRANSLATIONS["uz"].update({
    "filters.title": "Tanlovni belgilang:",
    "filters.recent": "🔥 So‘nggi 72 soat",
    "filters.country.turkiye": "🌴 Turkiya",
    "filters.country.uae": "🇦🇪 BAA",
    "filters.country.th": "🇹🇭 Tailand",
    "filters.country.vn": "🇻🇳 Vetnam",
    "filters.budget.500": "💸 ≤ $500",
    "filters.budget.800": "💸 ≤ $800",
    "filters.budget.1000": "💸 ≤ $1000",
    "filters.sort.price": "↕️ Narx bo‘yicha",
    "filters.more": "➕ Yaqinda qo‘shamiz",
    "more.title": "Tanlovni davom ettiraymi?",
    "more.next": "➡️ Yana ko‘rsat",
})

TRANSLATIONS["kk"].update({
    "filters.title": "Таңдаңыз:",
    "filters.recent": "🔥 Соңғы 72 сағ",
    "filters.country.turkiye": "🌴 Түркия",
    "filters.country.uae": "🇦🇪 БАӘ",
    "filters.country.th": "🇹🇭 Тайланд",
    "filters.country.vn": "🇻🇳 Вьетнам",
    "filters.budget.500": "💸 ≤ $500",
    "filters.budget.800": "💸 ≤ $800",
    "filters.budget.1000": "💸 ≤ $1000",
    "filters.sort.price": "↕️ Баға бойыншa",
    "filters.more": "➕ Жақында",
    "more.title": "Жалғастырайық па?",
    "more.next": "➡️ Тағы көрсету",
})

# --- Тексты кнопок карточки тура (i18n) ---
TRANSLATIONS["ru"].update({
    "btn.ask": "✍️ Вопрос по туру",
    "btn.fav.add": "🤍 В избранное",
    "btn.fav.rm":  "❤️ В избранном",
    "btn.want": "📝 Хочу этот тур",
    "btn.admin_open": "🔗 Открыть (админ)",
})

TRANSLATIONS["uz"].update({
    "btn.ask": "✍️ Tur bo‘yicha savol",
    "btn.fav.add": "🤍 Sevimlilarga",
    "btn.fav.rm":  "❤️ Sevimlilarda",
    "btn.want": "📝 Ushbu turni xohlayman",
    "btn.admin_open": "🔗 Ochish (admin)",
})

TRANSLATIONS["kk"].update({
    "btn.ask": "✍️ Тур туралы сұрақ",
    "btn.fav.add": "🤍 Таңдаулыға",
    "btn.fav.rm":  "❤️ Таңдаулыларда",
    "btn.want": "📝 Осы турды қалаймын",
    "btn.admin_open": "🔗 Ашуу (админ)",
})

TRANSLATIONS["ru"].update({"weather.loading": "Секунду, уточняю погоду…"})
TRANSLATIONS["uz"].update({"weather.loading": "Bir soniya, ob-havoni aniqlayapman…"})
TRANSLATIONS["kk"].update({"weather.loading": "Бір сәт, ауа райын нақтылап жатырмын…"})

TRANSLATIONS["ru"].update({"btn.weather": "🌤 Погода"})
TRANSLATIONS["uz"].update({"btn.weather": "🌤 Ob-havo"})
TRANSLATIONS["kk"].update({"btn.weather": "🌤 Ауа райы"})

TRANSLATIONS["ru"].update({
    "hello_again": "МЕНЮ обновлено под выбранный язык ✅",
})
TRANSLATIONS["uz"].update({
    "hello_again": "Menyu tanlangan tilga yangilandi ✅",
})
TRANSLATIONS["kk"].update({
    "hello_again": "Мәзір таңдалған тілге жаңартылды ✅",
})

TRANSLATIONS["ru"].update({
    "filters.country.ge": "🇬🇪 Грузия",
    "filters.country.mv": "🏝 Мальдивы",
    "filters.country.cn": "🇨🇳 Китай",
})
TRANSLATIONS["uz"].update({
    "filters.country.ge": "🇬🇪 Gruziya",
    "filters.country.mv": "🏝 Maldiv orollari",
    "filters.country.cn": "🇨🇳 Xitoy",
})
TRANSLATIONS["kk"].update({
    "filters.country.ge": "🇬🇪 Грузия",
    "filters.country.mv": "🏝 Мальдив аралдары",
    "filters.country.cn": "🇨🇳 Қытай",
})

REQUIRED_KEYS = {"menu_find","menu_gpt","menu_sub","menu_settings","lang_saved","hello_again","desc_find","desc_gpt"}
def _validate_i18n():
    import logging
    for lang, d in TRANSLATIONS.items():
        miss = REQUIRED_KEYS - set(d.keys())
        if miss:
            logging.warning("i18n: %s missing keys: %s", lang, ", ".join(sorted(miss)))
_validate_i18n()

# --- i18n helpers для схемы TRANSLATIONS ---
DEFAULT_LANG = DEFAULT_LANG  # уже объявлен выше

def t(user_id: int | None, key: str) -> str:
    """
    Вернёт перевод по ключу из TRANSLATIONS с учётом языка пользователя.
    Если в выбранном языке ключа нет — берём из языка по умолчанию,
    иначе возвращаем сам ключ (чтобы не падать).
    """
    lang = _lang(user_id) if user_id else DEFAULT_LANG
    # сначала пробуем язык пользователя
    if lang in TRANSLATIONS and key in TRANSLATIONS[lang]:
        return TRANSLATIONS[lang][key]
    # потом дефолтный язык
    if key in TRANSLATIONS.get(DEFAULT_LANG, {}):
        return TRANSLATIONS[DEFAULT_LANG][key]
    # фоллбек: сам ключ
    return key

# (опционально) шимы для совместимости со старыми названиями функций
def main_kb_for(user_id: int):
    return main_menu_kb(user_id)

def filters_inline_kb(user_id: int | None = None):
    return filters_inline_kb_for(user_id or 0)

# ================= БОТ / APP =================
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
app = FastAPI()

# --- Динамическая проверка колонок схемы
SCHEMA_COLS: set[str] = set()

def _has_cols(*names: str) -> bool:
    return all(n in SCHEMA_COLS for n in names)

def _select_tours_clause() -> str:
    base = "id, country, city, hotel, price, currency, dates, source_url, posted_at, photo_url, description"
    extras = []
    extras.append("board" if _has_cols("board") else "NULL AS board")
    extras.append("includes" if _has_cols("includes") else "NULL AS includes")
    return f"{base}, {', '.join(extras)}"

# ================= БД =================

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)


def ensure_pending_wants_table():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_wants (
                user_id BIGINT PRIMARY KEY,
                tour_id INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


def ensure_leads_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id BIGSERIAL PRIMARY KEY,
                full_name TEXT NOT NULL DEFAULT '',
                phone TEXT,
                tour_id INTEGER,
                note TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                user_id BIGINT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS leads_created_at_idx ON leads(created_at);")


def ensure_favorites_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT NOT NULL,
                tour_id INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY(user_id, tour_id)
            );
            """
        )


def ensure_questions_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS questions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tour_id INTEGER,
                question TEXT NOT NULL,
                admin_chat_id BIGINT,
                admin_message_id BIGINT,
                status TEXT NOT NULL DEFAULT 'open',
                answer TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                answered_at TIMESTAMPTZ
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS questions_user_id_idx ON questions(user_id);")

def ensure_orders_columns():
    try:
        with _pay_db() as conn, conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE IF EXISTS orders
                  ADD COLUMN IF NOT EXISTS provider_trx_id TEXT,
                  ADD COLUMN IF NOT EXISTS perform_time     TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS cancel_time      TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS reason           INTEGER
            """)
    except Exception:
        logging.exception("Ensure orders columns failed")

# ================== ПРОВЕРКА ЛИДОВ / ПОДПИСКИ ==================

def user_has_leads(user_id: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM leads WHERE user_id=%s LIMIT 1;", (user_id,))
        return cur.fetchone() is not None


def user_has_subscription(user_id: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT val FROM app_config WHERE key=%s;", (f"sub_{user_id}",))
        row = cur.fetchone()
        return bool(row and row["val"] == "active")


def set_subscription(user_id: int, status: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO app_config(key, val) VALUES (%s, %s)
            ON CONFLICT(key) DO UPDATE SET val=EXCLUDED.val;
            """,
            (f"sub_{user_id}", status),
        )

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_payme_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="💳 Оплатить через Payme",
                url="https://checkout.paycom.uz/<ВАШ_ЛИНК_ИЛИ_INVOICE_ID>"
            )]
        ]
    )

# ============== GOOGLE SHEETS ==============
_gs_client = None


def _get_gs_client():
    global _gs_client
    if _gs_client is not None:
        return _gs_client
    if not (SHEETS_CREDENTIALS_B64 and SHEETS_SPREADSHEET_ID):
        logging.info("GS: credentials or spreadsheet id not set")
        _gs_client = None
        return None
    try:
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
    try:
        ws = spreadsheet.worksheet(title)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        pass
    ws = spreadsheet.add_worksheet(title=title, rows=500, cols=max(12, len(header) + 2))
    if header:
        ws.append_row(header, value_input_option="USER_ENTERED")
    logging.info(f"GS: created worksheet '{title}'")
    return ws


def _ensure_header(ws, header: list[str]) -> None:
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
    try:
        gc = _get_gs_client()
        if not gc:
            return ""
        sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
        try:
            ws = sh.worksheet(KB_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return ""
        rows = ws.get_all_records()
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
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT country, city, hotel, COALESCE(board, '') AS board, COALESCE(includes, '') AS includes,
                       price, currency, dates, posted_at
                FROM tours
                WHERE posted_at >= %s
                ORDER BY posted_at DESC
                LIMIT %s
            """,
                (cutoff, max_rows),
            )
            rows = cur.fetchall()
        lines = []
        for r in rows:
            when = localize_dt(r.get("posted_at"))
            price = fmt_price(r.get("price"), r.get("currency"))
            hotel = clean_text_basic(strip_trailing_price_from_hotel(r.get("hotel") or "Пакетный тур"))
            board = (r.get("board") or "").strip()
            inc = (r.get("includes") or "").strip()
            extra = []
            if board:
                extra.append(f"питание: {board}")
            if inc:
                extra.append(f"включено: {inc}")
            extra_txt = f" ({'; '.join(extra)})" if extra else ""
            lines.append(
                f"- {r.get('country')} — {r.get('city')}, {hotel}, {price}, даты: {r.get('dates') or '—'}{extra_txt}. {when}"
            )
        return "\n".join(lines)
    except Exception as e:
        logging.warning(f"Recent context load failed: {e}")
        return ""


def append_lead_to_sheet(lead_id: int, user, phone: str, t: dict):
    try:
        gc = _get_gs_client()
        if not gc:
            return
        sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
        header = [
            "created_utc",
            "lead_id",
            "username",
            "full_name",
            "phone",
            "country",
            "city",
            "hotel",
            "price",
            "currency",
            "dates",
            "source_url",
            "posted_local",
            "board",
            "includes",
        ]
        ws = _ensure_ws(sh, WORKSHEET_NAME, header)
        _ensure_header(ws, header)

        full_name = f"{(getattr(user, 'first_name', '') or '').strip()} {(getattr(user, 'last_name', '') or '').strip()}".strip()
        username = f"@{user.username}" if getattr(user, "username", None) else ""
        posted_local = localize_dt(t.get("posted_at"))
        hotel_text = t.get("hotel") or derive_hotel_from_description(t.get("description")) or "Пакетный тур"
        hotel_clean = clean_text_basic(strip_trailing_price_from_hotel(hotel_text))

        ws.append_row(
            [
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
            ],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        logging.error(f"append_lead_to_sheet failed: {e}")


# ================= УТИЛИТЫ КОНФИГА =================

def resolve_leads_chat_id() -> int:
    val = get_config("LEADS_CHAT_ID", LEADS_CHAT_ID_ENV)
    try:
        return int(val) if val else 0
    except Exception:
        return 0

def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(user_id, "menu_find")),
             KeyboardButton(text=t(user_id, "menu_gpt"))],
            [KeyboardButton(text=t(user_id, "menu_sub")),
             KeyboardButton(text=t(user_id, "menu_settings"))],
        ],
        resize_keyboard=True,
    )

def filters_inline_kb_for(user_id: int) -> InlineKeyboardMarkup:
    # Удобнее держать структуру списком пар (label_key, callback_country)
    countries = [
        ("filters.country.turkiye", "Турция"),
        ("filters.country.uae",      "ОАЭ"),
        ("filters.country.th",       "Таиланд"),
        ("filters.country.vn",       "Вьетнам"),
        # новые:
        ("filters.country.ge",       "Грузия"),
        ("filters.country.mv",       "Мальдивы"),
        ("filters.country.cn",       "Китай"),
    ]

    rows = [
        [InlineKeyboardButton(text=t(user_id, "filters.recent"), callback_data="tours_recent")],
    ]

    # Размещаем по два в ряд
    row = []
    for key, country in countries:
        row.append(InlineKeyboardButton(text=t(user_id, key), callback_data=f"country:{country}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)

    # Бюджеты + сортировка + more
    rows.append([
        InlineKeyboardButton(text=t(user_id, "filters.budget.500"),  callback_data="budget:USD:500"),
        InlineKeyboardButton(text=t(user_id, "filters.budget.800"),  callback_data="budget:USD:800"),
        InlineKeyboardButton(text=t(user_id, "filters.budget.1000"), callback_data="budget:USD:1000"),
    ])
    rows.append([InlineKeyboardButton(text=t(user_id, "filters.sort.price"), callback_data="sort:price_asc")])
    rows.append([InlineKeyboardButton(text=t(user_id, "filters.more"),       callback_data="noop")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def more_kb(token: str, next_offset: int, uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(uid, "more.next"), callback_data=f"more:{token}:{next_offset}")],
            [InlineKeyboardButton(text=t(uid, "back"),      callback_data="back_filters")],
        ]
    )

def want_contact_kb_for(user_id: int) -> ReplyKeyboardMarkup:
    share_txt = t(user_id, "share_phone")  # если нет — t() вернёт русский дефолт или сам ключ
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=share_txt, request_contact=True)]],
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

# ================= ПОГОДА =================
WEATHER_CACHE: Dict[str, Tuple[float, Dict]] = {}
WEATHER_TTL = 900

WMO = {
    "ru": {
        0: "Ясно ☀️", 1: "Преимущественно ясно 🌤", 2: "Переменная облачность ⛅️", 3: "Облачно ☁️",
        45: "Туман 🌫", 48: "Гололёдный туман 🌫❄️",
        51: "Морось слабая 🌦", 53: "Морось умеренная 🌦", 55: "Морось сильная 🌧",
        61: "Дождь слабый 🌦", 63: "Дождь умеренный 🌧", 65: "Дождь сильный 🌧",
        66: "Ледяной дождь слабый 🌧❄️", 67: "Ледяной дождь сильный 🌧❄️",
        71: "Снег слабый ❄️", 73: "Снег умеренный ❄️", 75: "Снег сильный ❄️",
        77: "Снежная крупа 🌨", 80: "Ливни слабые 🌦", 81: "Ливни умеренные 🌧", 82: "Ливни сильные 🌧",
        85: "Снегопад слабый 🌨", 86: "Снегопад сильный 🌨",
        95: "Гроза ⛈", 96: "Гроза с градом ⛈🧊", 99: "Сильная гроза с градом ⛈🧊",
    },
    "uz": {
        0: "Ochiq ☀️", 1: "Asosan ochiq 🌤", 2: "Qisman bulutli ⛅️", 3: "Bulutli ☁️",
        45: "Tuman 🌫", 48: "Muzli tuman 🌫❄️",
        51: "Yengil mayda yomg‘ir 🌦", 53: "O‘rtacha mayda yomg‘ir 🌦", 55: "Kuchli mayda yomg‘ir 🌧",
        61: "Yengil yomg‘ir 🌦", 63: "O‘rtacha yomg‘ir 🌧", 65: "Kuchli yomg‘ir 🌧",
        66: "Muzli yomg‘ir (yengil) 🌧❄️", 67: "Muzli yomg‘ir (kuchli) 🌧❄️",
        71: "Yengil qor ❄️", 73: "O‘rtacha qor ❄️", 75: "Kuchli qor ❄️",
        77: "Qor donachalari 🌨", 80: "Yomg‘ir quyishi (yengil) 🌦", 81: "Yomg‘ir quyishi (o‘rtacha) 🌧", 82: "Yomg‘ir quyishi (kuchli) 🌧",
        85: "Qor yog‘ishi (yengil) 🌨", 86: "Qor yog‘ishi (kuchli) 🌨",
        95: "Momaqaldiroq ⛈", 96: "Momaqaldiroq va do‘l ⛈🧊", 99: "Kuchli momaqaldiroq va do‘l ⛈🧊",
    },
    "kk": {
        0: "Аспан ашық ☀️", 1: "Көбіне ашық 🌤", 2: "Аралас бұлтты ⛅️", 3: "Бұлтты ☁️",
        45: "Тұман 🌫", 48: "Мұзды тұман 🌫❄️",
        51: "Ұсақ жаңбыр (әлсіз) 🌦", 53: "Ұсақ жаңбыр (орташа) 🌦", 55: "Ұсақ жаңбыр (күшті) 🌧",
        61: "Жаңбыр (әлсіз) 🌦", 63: "Жаңбыр (орташа) 🌧", 65: "Жаңбыр (күшті) 🌧",
        66: "Мұзды жаңбыр (әлсіз) 🌧❄️", 67: "Мұзды жаңбыр (күшті) 🌧❄️",
        71: "Қар (әлсіз) ❄️", 73: "Қар (орташа) ❄️", 75: "Қар (күшті) ❄️",
        77: "Қар түйіршіктері 🌨", 80: "Құйынды жаңбыр (әлсіз) 🌦", 81: "Құйынды жаңбыр (орташа) 🌧", 82: "Құйынды жаңбыр (күшті) 🌧",
        85: "Қар жауу (әлсіз) 🌨", 86: "Қар жауу (күшті) 🌨",
        95: "Найзағай ⛈", 96: "Найзағай, бұршақ ⛈🧊", 99: "Күшті найзағай, бұршақ ⛈🧊",
    },
}

def wmo_text(code: int, lang: str) -> str:
    return WMO.get(lang, WMO["ru"]).get(code, {
        "ru": "Погода", "uz": "Ob-havo", "kk": "Ауа райы"
    }[lang if lang in ("ru","uz","kk") else "ru"])


def _cleanup_weather_cache():
    now = time.time()
    for k, (ts, _) in list(WEATHER_CACHE.items()):
        if now - ts > WEATHER_TTL:
            WEATHER_CACHE.pop(k, None)


def _extract_place_from_weather_query(q: str) -> Optional[str]:
    txt = q.strip()

    # убрать частые служебные слова на трёх языках
    txt = re.sub(r"(сегодня|сейчас|завтра|пожалуйста|пж|pls|please|bugun|hozir|ertaga|iltimos|bügіn|qazіr|ертең|өтінемін)",
                 "", txt, flags=re.I)

    # «погода в/на ...», «ob-havo ...», «ауа райы ...»
    patterns = [
        r"(?:на|в|во|по)\s+([A-Za-zА-Яа-яЁёĞğİıŞşÇçÖöÜüҚқҒғҢңӘәӨөҰұҚқҺһʼ'\-\s]+)",
        r"(?:погод[ауые]\s+)([A-Za-zА-Яа-яЁёĞğİıŞşÇçÖöÜüҚқҒғҢңӘәӨөҰұҚқҺһʼ'\-\s]+)",
        r"(?:ob[-\s]?havo|obhavo)\s+([A-Za-zА-Яа-яЁёĞğİıŞşÇçÖöÜüʼ'\-\s]+)",
        r"(?:ауа\s*райы)\s+([A-Za-zА-Яа-яЁёҚқҒғҢңӘәӨөҰұҚқҺһʼ'\-\s]+)",
    ]
    m = None
    for p in patterns:
        m = re.search(p, txt, flags=re.I)
        if m:
            break
    if not m:
        return None

    place = m.group(1)
    place = re.sub(r"[?!.,:;]+$", "", place).strip()
    place = re.sub(r"\b(сегодня|завтра|сейчас|bugun|ertaga|hozir|бүгін|ертең|қазір)\b", "", place, flags=re.I).strip()
    place = re.sub(r"^(остров|oroli|аралы)\s+", "", place, flags=re.I)
    return place or None

async def get_weather_text(place: str, lang: str = "ru") -> str:
    lang = lang if lang in ("ru", "uz", "kk") else "ru"

    texts = {
        "ask_place": {
            "ru": "Напиши город/место: например, «погода в Стамбуле» или «погода на Бали».",
            "uz": "Shahar/joyni yozing: masalan, «Istanbulda ob-havo» yoki «Balida ob-havo».",
            "kk": "Қаланы/орынды жазыңыз: мысалы, «Стамбұлдағы ауа райы» немесе «Балиде ауа райы».",
        },
        "not_found": {
            "ru": "Не нашёл локацию «{q}». Попробуй иначе (город/остров/страна).",
            "uz": "«{q}» joyi topilmadi. Boshqacha yozib ko‘ring (shahar/orol/mamlakat).",
            "kk": "«{q}» орны табылмады. Басқа түрде жазыңыз (қала/арал/ел).",
        },
        "fetch_fail": {
            "ru": "Не удалось получить погоду для «{q}». Попробуй позже.",
            "uz": "«{q}» uchun ob-havo olinmadi. Keyinroq urinib ko‘ring.",
            "kk": "«{q}» үшін ауа райын алу мүмкін болмады. Кейінірек қайталап көріңіз.",
        },
        "label": {"ru": "Погода", "uz": "Ob-havo", "kk": "Ауа райы"},
        "now": {
            "ru": "Сейчас", "uz": "Hozir", "kk": "Қазір",
        },
        "feels": {
            "ru": "ощущается как", "uz": "his qilinadi", "kk": "сезіледі",
        },
        "humidity": {
            "ru": "Влажность", "uz": "Namlik", "kk": "Ылғалдылық",
        },
        "wind": {
            "ru": "Жел", "uz": "Shamol", "kk": "Жел",
        },
        "precip_prob": {
            "ru": "Вероятность осадков сегодня",
            "uz": "Bugun yog‘ingarchilik ehtimoli",
            "kk": "Бүгінгі жауын-шашын ықтималдығы",
        },
        "retry": {
            "ru": "Не удалось получить данные о погоде. Попробуй ещё раз чуть позже.",
            "uz": "Ob-havo ma’lumotlarini olish muvaffaqiyatsiz. Birozdan so‘ng qayta urinib ko‘ring.",
            "kk": "Ауа райы деректерін алу мүмкін болмады. Біраздан соң қайталап көріңіз.",
        },
    }

    if not place:
        return texts["ask_place"][lang]

    key = f"{lang}:{place.lower().strip()}"
    _cleanup_weather_cache()
    if key in WEATHER_CACHE:
        _, cached = WEATHER_CACHE[key]
        return cached["text"]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            geo_r = await client.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": place, "count": 1, "language": lang},
            )
            if geo_r.status_code != 200 or not geo_r.json().get("results"):
                return texts["not_found"][lang].format(q=escape(place))

            g = geo_r.json()["results"][0]
            lat, lon = g["latitude"], g["longitude"]
            label_parts = [g.get("name")]
            if g.get("admin1"):
                label_parts.append(g["admin1"])
            if g.get("country"):
                label_parts.append(g["country"])
            label = ", ".join([p for p in label_parts if p])

            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
                "hourly": "precipitation_probability",
                "timezone": "auto",
            }
            w_r = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            if w_r.status_code != 200:
                return texts["fetch_fail"][lang].format(q=escape(label))

            data = w_r.json()
            cur = data.get("current", {})
            code = int(cur.get("weather_code", 0))
            desc = wmo_text(code, lang)
            t = cur.get("temperature_2m")
            feels = cur.get("apparent_temperature")
            rh = cur.get("relative_humidity_2m")
            wind = cur.get("wind_speed_10m")

            prob = None
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            probs = hourly.get("precipitation_probability", [])
            if times and probs:
                today = (datetime.now(timezone.utc).astimezone()).strftime("%Y-%m-%d")
                prob = max((p for tt, p in zip(times, probs) if tt.startswith(today)), default=None)

            parts = [f"{texts['label'][lang]}: <b>{escape(label)}</b>", desc]
            if t is not None:
                tmp = f"{t:.0f}°C"
                if feels is not None and abs(feels - t) >= 1:
                    tmp += f" ({texts['feels'][lang]} {feels:.0f}°C)"
                parts.append(f"{texts['now'][lang]}: {tmp}")
            if rh is not None:
                parts.append(f"{texts['humidity'][lang]}: {int(rh)}%")
            if wind is not None:
                parts.append(f"{texts['wind'][lang]}: {wind:.1f} м/с")
            if prob is not None:
                parts.append(f"{texts['precip_prob'][lang]}: {int(prob)}%")

            txt = " | ".join(parts)
            WEATHER_CACHE[key] = (time.time(), {"text": txt})
            return txt
    except Exception as e:
        logging.warning(f"get_weather_text failed: {e}")
        return texts["retry"][lang]

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
    # убираем только кусок с ценой и валютой в конце строки
    return re.sub(
        r'\s*[–—-]?\s*\d[\d\s.,]*(?:USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€)\s*$',
        '',
        s,
        flags=re.I,
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
        d = int(d)
        mo = int(mo)
        y = int(y)
        if y < 100:
            y += 2000 if y < 70 else 1900
        if mo > 12 and d <= 12:
            d, mo = mo, d
        return f"{d:02d}.{mo:02d}.{y:04d}"

    return f"{_norm(d1, m1, y1)}–{_norm(d2, m2, y2)}"

def normalize_currency(cur: str) -> str:
    cur = cur.strip().upper().replace("＄", "$").replace("€", "EUR")
    if cur in {"$", "USD"}:
        return "USD"
    if cur in {"EUR"}:
        return "EUR"
    if cur in {"SUM", "UZ", "UZS"}:
        return "UZS"
    return cur

def localize_dt(dt: Optional[datetime]) -> str:
    if not isinstance(dt, datetime):
        return ""
    try:
        dt_local = dt if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC"))
        dt_local = dt_local.astimezone(TZ)
        return f"🕒 {dt_local.strftime('%d.%m.%Y %H:%M')} (TST)"
    except Exception:
        return f"🕒 {dt.strftime('%d.%m.%Y %H:%M')}"


CONTACT_STOP_WORDS = (
    "заброниров",
    "брониров",
    "звоните",
    "тел:",
    "телефон",
    "whatsapp",
    "вацап",
    "менеджер",
    "директ",
    "адрес",
    "@",
    "+998",
    "+7",
    "+380",
    "call-центр",
    "колл-центр",
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
    if re.search(r"\buai\b|ultra\s*all", joined):
        return "UAI (ultra)"
    if re.search(r"\bai\b|all\s*inclusive|всё включено|все включено", joined):
        return "AI (всё включено)"
    if re.search(r"\bhb\b|полупанси", joined):
        return "HB (полупансион)"
    if re.search(r"\bbb\b|завтра(к|ки)", joined):
        return "BB (завтраки)"
    if re.search(r"\bfb\b|полный\s*панс", joined):
        return "FB (полный)"
    return None


# ================= ДБ-ХЕЛПЕРЫ =================

import os
from urllib.parse import urlparse
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
def is_valid_url(u: str | None) -> bool:
    if not u:
        return False
    u = u.strip()
    if not u or len(u) > 512:
        return False
    p = urlparse(u)
    return p.scheme in ("http", "https") and bool(p.netloc)

async def safe_answer(msg: Message, *args, **kwargs):
    """Отправка сообщения с graceful-деградацией, если сломана инлайн-кнопка."""
    try:
        return await msg.answer(*args, **kwargs)
    except TelegramBadRequest as e:
        if "BUTTON_URL_INVALID" in str(e):
            # убираем клавиатуру и говорим пользователю, что ссылка ещё не настроена
            kwargs.pop("reply_markup", None)
            text = (kwargs.get("text") or args[0] if args else "") + "\n\n(Ссылка пока не настроена)"
            return await msg.answer(text)
        raise

# === замените вашу get_payme_kb на безопасную ===
def get_payme_kb() -> InlineKeyboardMarkup:
    PAYME_URL = os.getenv("PAYME_URL", "").strip()
    TG_SUPPORT = os.getenv("SUPPORT_USERNAME", "").lstrip("@").strip()

    rows: list[list[InlineKeyboardButton]] = []

    if is_valid_url(PAYME_URL):
        rows.append([InlineKeyboardButton(text="💳 Оплатить в Payme", url=PAYME_URL)])

    # запасной «живой» канал — написать менеджеру в тг
    if TG_SUPPORT:
        rows.append([InlineKeyboardButton(text="👤 Менеджер", url=f"https://t.me/{TG_SUPPORT}")])
    else:
        # совсем офлайн — хотя бы заглушка, чтобы не падало
        rows.append([InlineKeyboardButton(text="👤 Менеджер (скоро)", callback_data="noop:support")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def is_favorite(user_id: int, tour_id: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM favorites WHERE user_id=%s AND tour_id=%s LIMIT 1;", (user_id, tour_id))
        return cur.fetchone() is not None

def set_favorite(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO favorites(user_id, tour_id) VALUES (%s, %s)
            ON CONFLICT (user_id, tour_id) DO NOTHING;
            """,
            (user_id, tour_id),
        )


def unset_favorite(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM favorites WHERE user_id=%s AND tour_id=%s;", (user_id, tour_id))


def create_lead(tour_id: int, phone: Optional[str], full_name: str, note: Optional[str] = None):
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leads (full_name, phone, tour_id, note)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
                """,
                (full_name, phone, tour_id, note),
            )
            row = cur.fetchone()
            return row["id"] if row else None
    except Exception as e:
        logging.error(f"create_lead failed: {e}")
        return None


def _tours_has_cols(*cols: str) -> Dict[str, bool]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'tours'
            """
        )
        have = {r["column_name"] for r in cur.fetchall()}
    return {c: (c in have) for c in cols}


async def load_recent_context(limit: int = 6) -> str:
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
            hotel = (
                clean_text_basic(strip_trailing_price_from_hotel(r.get("hotel"))) if r.get("hotel") else "пакетный тур"
            )
            parts = [
                f"{r.get('country') or '—'} — {r.get('city') or '—'}",
                f"{hotel}",
                f"{price}",
            ]
            if r.get("dates"):
                parts.append(f"даты: {normalize_dates_for_display(r.get('dates'))}")
            if r.get("board"):
                parts.append(f"питание: {r.get('board')}")
            if r.get("includes"):
                parts.append(f"включено: {r.get('includes')}")
            lines.append(" • ".join(parts))
        return "\n".join(lines)
    except Exception as e:
        logging.warning(f"Recent context load failed: {e}")
        return ""


def set_pending_want(user_id: int, tour_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pending_wants(user_id, tour_id) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET tour_id = EXCLUDED.tour_id, created_at = now();
            """,
            (user_id, tour_id),
        )


def get_pending_want(user_id: int) -> Optional[int]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT tour_id FROM pending_wants WHERE user_id=%s;", (user_id,))
        row = cur.fetchone()
        return row["tour_id"] if row else None


def del_pending_want(user_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM pending_wants WHERE user_id=%s;", (user_id,))

def _valid_xauth(val: str) -> bool:
    cand = set()
    mid = os.getenv("PAYME_MERCHANT_ID", "").strip()
    k_test = os.getenv("PAYME_MERCHANT_TEST_KEY", "").strip()
    k_prod = os.getenv("PAYME_MERCHANT_KEY", "").strip()
    k_raw = os.getenv("PAYME_MERCHANT_XAUTH", "").strip()
    if mid and k_test:
        cand.add("Basic " + base64.b64encode(f"{mid}:{k_test}".encode()).decode())
    if mid and k_prod:
        cand.add("Basic " + base64.b64encode(f"{mid}:{k_prod}".encode()).decode())
    if k_raw:
        cand.add(k_raw)
    return val in cand

def _payme_sandbox_ok(request) -> bool:
    """Пускаем запросы из песочницы Payme даже если Basic не доехал."""
    try:
        ip = request.client.host if getattr(request, "client", None) else ""
    except Exception:
        ip = ""
    referer = request.headers.get("Referer", "")
    testop  = request.headers.get("Test-Operation", "")
    return (
        ip.startswith("185.234.113.")     # IP песочницы Payme
        or referer.startswith("http://test.paycom.uz")
        or testop == "Paycom"
    )

# ================= ПОИСК ТУРОВ =================

# --- Импорты должны быть выше в файле ---
# from typing import Optional, List, Tuple
# from datetime import datetime, timedelta, timezone
# import logging

# Мини-сторожок по «явно неверным» ценам (чтобы не ловить 5 USD за "друга")
MIN_PRICE_BY_CURRENCY = {
    "USD": 30,   # не показываем цены ниже 30 USD
    "EUR": 30,
    "RUB": 3000,
}

# Канонические названия стран (и как они лежат в БД)
CANON_COUNTRY = {
    "Турция": "Турция",
    "ОАЭ": "ОАЭ",
    "Таиланд": "Таиланд",
    "Вьетнам": "Вьетнам",
    "Грузия": "Грузия",
    "Мальдивы": "Мальдивы",
    "Китай": "Китай",
    # при желании: "Turkiye": "Турция", "UAE": "ОАЭ", ...
}

def normalize_country(name: str) -> str:
    name = (name or "").strip()
    return CANON_COUNTRY.get(name, name)

# === FETCH (совместимо со старыми вызовами) ===
# Требуются: get_conn, _select_tours_clause, normalize_country, RECENT_EXPR, cutoff_utc
from typing import Optional, Tuple, List
import logging

async def fetch_tours(
    query: Optional[str] = None,
    *,
    country: Optional[str] = None,
    currency_eq: Optional[str] = None,
    max_price: Optional[float] = None,
    hours: int = 24,
    limit: int = 10,
    strict_recent: bool = True,
    # 👇 совместимость со старыми хэндлерами:
    limit_recent: Optional[int] = None,
    limit_fallback: Optional[int] = None,
) -> Tuple[List[dict], bool]:
    """
    Возвращает (rows, is_recent_window_used).
    Свежесть считаем по RECENT_EXPR (posted_at и т.п.).
    Если strict_recent=False: сначала H часов → 72ч → без окна.
    Параметры limit_recent/limit_fallback (если переданы) перекрывают общий limit.
    """
    try:
        where: List[str] = []
        params: List = []

        if query:
            where.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s OR description ILIKE %s)")
            q = f"%{query}%"
            params += [q, q, q, q]

        if country:
            # допускаем вариации (Таиланд/Thailand/🇹🇭)
            where.append("country ILIKE %s")
            params.append(f"%{normalize_country(country)}%")

        if currency_eq:
            where.append("currency = %s")
            params.append(currency_eq)

        if max_price is not None:
            where.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)

        # лимиты с учётом обратной совместимости
        lim_recent = limit_recent if limit_recent is not None else limit
        lim_fb     = limit_fallback if limit_fallback is not None else limit

        # ORDER BY
        order_clause = (
            "ORDER BY price ASC NULLS LAST, posted_at DESC NULLS LAST"
            if max_price is not None
            else "ORDER BY posted_at DESC NULLS LAST"
        )

        select_list = _select_tours_clause()

        # -------- 1) окно H часов (recent) ----------
        recent_cond = f"{RECENT_EXPR} >= %s"
        recent_where = where + [recent_cond]
        recent_params = params + [cutoff_utc(hours)]

        sql_recent = (
            f"SELECT {select_list} FROM tours "
            + ("WHERE " + " AND ".join(recent_where) if recent_where else "")
            + f" {order_clause} LIMIT %s"
        )

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_recent, recent_params + [lim_recent])
            rows = cur.fetchall()
            if rows or strict_recent:
                return rows, True

            # -------- 2) окно 72 часа ----------
            cond72 = f"{RECENT_EXPR} >= %s"
            where72 = where + [cond72]
            params72 = params + [cutoff_utc(72)]

            sql72 = (
                f"SELECT {select_list} FROM tours "
                + ("WHERE " + " AND ".join(where72) if where72 else "")
                + f" {order_clause} LIMIT %s"
            )
            cur.execute(sql72, params72 + [lim_recent])
            rows72 = cur.fetchall()
            if rows72:
                return rows72, False

            # -------- 3) без окна (fallback) ----------
            sql_fb = (
                f"SELECT {select_list} FROM tours "
                + ("WHERE " + " AND ".join(where) if where else "")
                + f" {order_clause} LIMIT %s"
            )
            cur.execute(sql_fb, params + [lim_fb])
            return cur.fetchall(), False

    except Exception:
        logging.exception("Ошибка при fetch_tours")
        return [], True

# === ПАГИНАЦИЯ ===
async def fetch_tours_page(
    query: Optional[str] = None,
    *,
    country: Optional[str] = None,
    country_terms: Optional[list[str]] = None,
    any_terms: Optional[list[str]] = None,
    currency_eq: Optional[str] = None,
    max_price: Optional[float] = None,
    hours: Optional[int] = None,
    order_by_price: bool = False,
    limit: int = 10,
    offset: int = 0,
) -> List[dict]:
    """
    Пагинация; свежесть — по posted_at (RECENT_EXPR).
    """
    try:
        where, params = [], []

        if query:
            where.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s OR description ILIKE %s)")
            q = f"%{query}%"
            params += [q, q, q, q]

        if country_terms:
            ors = []
            for term in country_terms:
                ors.append("country ILIKE %s")
                params.append(f"%{term}%")
            where.append("(" + " OR ".join(ors) + ")")
        elif country:
            where.append("country ILIKE %s")
            params.append(f"%{normalize_country(country)}%")

        if any_terms:
            blocks = []
            for term in any_terms:
                blocks.append("(country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s OR description ILIKE %s)")
                params += [f"%{term}%", f"%{term}%", f"%{term}%", f"%{term}%"]
            where.append("(" + " OR ".join(blocks) + ")")

        if currency_eq:
            where.append("currency = %s")
            params.append(currency_eq)

        if max_price is not None:
            where.append("price IS NOT NULL AND price <= %s")
            params.append(max_price)

        if hours is not None:
            where.append(f"{RECENT_EXPR} >= %s")
            params.append(cutoff_utc(hours))

        order_clause = (
            "ORDER BY price ASC NULLS LAST, posted_at DESC NULLS LAST"
            if order_by_price else
            "ORDER BY posted_at DESC NULLS LAST"
        )
        select_list = _select_tours_clause()
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"SELECT {select_list} FROM tours {where_sql} {order_clause} LIMIT %s OFFSET %s"

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params + [limit, offset])
            return cur.fetchall()

    except Exception:
        logging.exception("Ошибка fetch_tours_page")
        return []

# ================= GPT =================
last_gpt_call = defaultdict(float)

def get_order_safe(order_id: int) -> dict | None:
    with _pay_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s;", (order_id,))
        return cur.fetchone()

def fmt_sub_until(user_id: int) -> str:
    with _pay_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_period_end FROM subscriptions WHERE user_id=%s;", (user_id,))
        row = cur.fetchone()
        if not row or not row["current_period_end"]:
            return "—"
        return row["current_period_end"].astimezone(TZ).strftime("%d.%m.%Y")

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
                    return [answer[i : i + MAX_LEN] for i in range(0, len(answer), MAX_LEN)]

                if r.status_code in (429, 500, 502, 503, 504):
                    delay = min(20.0, (2 ** attempt) + random.random())
                    await asyncio.sleep(delay)
                    continue
                logging.error(f"OpenAI error {r.status_code}: {r.text[:400]}")
                break
    except Exception as e:
        logging.exception(f"GPT call failed: {e}")

    return [
        "⚠️ ИИ сейчас перегружен. Попробуй ещё раз — а пока загляни в «🎒 Найти туры»: там только свежие предложения за последние 72 часа.",
    ]


# ================= КАРТОЧКИ/УВЕДОМЛЕНИЯ =================

def tour_inline_kb(tour: dict, is_fav: bool, user_id: Optional[int] = None) -> InlineKeyboardMarkup:
    lang = _lang(user_id) if user_id else DEFAULT_LANG
    tr = TRANSLATIONS[lang]
    rows = []

    # 🔒 ссылку видит только администратор
    url = (tour.get("source_url") or "").strip()
    if url and user_id == ADMIN_USER_ID:
        rows.append([InlineKeyboardButton(text=tr["btn.admin_open"], url=url)])

    # кнопки
    ask_btn = InlineKeyboardButton(text=tr["btn.ask"], callback_data=f"ask:{tour['id']}")

    fav_btn = InlineKeyboardButton(
        text=(tr["btn.fav.rm"] if is_fav else tr["btn.fav.add"]),
        callback_data=f"fav:{'rm' if is_fav else 'add'}:{tour['id']}",
    )

    want_btn = InlineKeyboardButton(text=tr["btn.want"], callback_data=f"want:{tour['id']}")

    # новая кнопка "погода"
    place = tour.get("city") or tour.get("country") or ""
    wx_btn = InlineKeyboardButton(text=tr["btn.weather"], callback_data=f"wx:{place}")

    back_btn = InlineKeyboardButton(text=tr["back"], callback_data="back_filters")

    # собираем ряды
    rows.append([ask_btn])
    rows.append([fav_btn, want_btn])
    rows.append([wx_btn])
    rows.append([back_btn])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_card_text(t: dict, lang: str = "ru") -> str:
    hotel   = safe_title(t)  # ← вся логика заголовка внутри safe_title
    country = (t.get("country") or "—").strip()
    city    = (t.get("city") or "—").strip()
    price   = fmt_price(t.get("price"), t.get("currency"))
    dates   = normalize_dates_for_display(t.get("dates")) if t.get("dates") else "—"
    board   = (t.get("board") or "").strip()
    inc     = (t.get("includes") or "").strip()
    when_dt = t.get("posted_at")
    when    = f"🕒 {localize_dt(when_dt)}" if when_dt else ""

    lines = [
        f"🏨 <b>{hotel}</b>",
        f"📍 {country} — {city}",
        f"💵 {price}",
        f"🗓 {dates}",
    ]
    if board:
        lines.append(f"🍽 Питание: {board}")
    if inc:
        lines.append(f"✅ Включено: {inc}")
    if when:
        lines.append(when)

    return "\n".join(lines)


def _letters_digits_ratio(s: str) -> float:
    import re
    if not s:
        return 0.0
    alnum = len(re.findall(r"[A-Za-zА-Яа-я0-9]", s))
    return alnum / max(1, len(s))


def safe_title(t: dict) -> str:
    h = clean_text_basic(strip_trailing_price_from_hotel(t.get("hotel") or ""))
    if _letters_digits_ratio(h) < 0.25 or len(h.strip()) < 3:
        alt = derive_hotel_from_description(t.get("description"))
        if alt:
            h = clean_text_basic(strip_trailing_price_from_hotel(alt))
    if _letters_digits_ratio(h) < 0.25 or len(h.strip()) < 3:
        ctry = (t.get("country") or "").strip()
        city = (t.get("city") or "").strip()
        h = (f"{ctry} — {city}".strip(" —") or "Тур")
    return h

async def send_tour_card(chat_id: int, user_id: int, tour: dict):
    fav = is_favorite(user_id, tour["id"]) 
    kb = tour_inline_kb(tour, fav, user_id)
    caption = build_card_text(tour, lang=_lang(user_id))
    await bot.send_message(chat_id, caption, reply_markup=kb, disable_web_page_preview=True)

import asyncio
from typing import List

async def send_batch_cards(chat_id: int, user_id: int, rows: list[dict], token: str, next_offset: int):
    if not rows:
        return False
    for t in rows:
        await send_tour_card(chat_id, user_id, t)
        await asyncio.sleep(0)

    LAST_RESULTS[user_id] = rows
    LAST_QUERY_AT[user_id] = time.monotonic()

    await bot.send_message(
        chat_id,
        "Продолжить подборку?",
        reply_markup=more_kb(token, next_offset, user_id),
    )
    return True

# ===== Общие хелперы для админ-уведомлений =====

def _admin_user_label(user) -> str:
    if getattr(user, "username", None):
        return f"@{user.username}"
    return (f"{(user.first_name or '')} {(user.last_name or '')}".strip() or "Гость")


def _compose_tour_block(t: dict) -> tuple[str, str | None]:
    price_str = fmt_price(t.get("price"), t.get("currency"))
    hotel_text = t.get("hotel") or derive_hotel_from_description(t.get("description"))
    hotel_clean = (
        clean_text_basic(strip_trailing_price_from_hotel(hotel_text)) if hotel_text else "Пакетный тур"
    )
    dates_norm = normalize_dates_for_display(t.get("dates"))
    time_str = localize_dt(t.get("posted_at"))
    board = (t.get("board") or "").strip()
    includes = (t.get("includes") or "").strip()
    src = (t.get("source_url") or "").strip()

    lines = [
        f"🌍 {safe(t.get('country'))} — {safe(t.get('city'))}",
        f"🏨 {safe(hotel_clean)}",
        f"💵 {price_str}",
        f"📅 {dates_norm}",
        time_str or "",
    ]
    if board:
        lines.append(f"🍽 Питание: {escape(board)}")
    if includes:
        lines.append(f"✅ Включено: {escape(includes)}")
    if src:
        lines.append(f'🔗 <a href="{escape(src)}">Источник</a>')
    text = "\n".join([l for l in lines if l]).strip()
    photo = (t.get("photo_url") or "").strip() or None
    return text, photo


async def _send_to_admin_group(text: str, photo: str | None, pin: bool = False):
    chat_id = resolve_leads_chat_id()
    if not chat_id:
        logging.warning("admin notify: LEADS_CHAT_ID не задан")
        return
    kwargs = {}
    if LEADS_TOPIC_ID:
        kwargs["message_thread_id"] = LEADS_TOPIC_ID
    if photo:
        # телега ограничивает длину подписи, страхуемся
        short = text if len(text) <= 1000 else (text[:990].rstrip() + "…")
        msg = await bot.send_photo(chat_id, photo=photo, caption=short, parse_mode="HTML", **kwargs)
    else:
        msg = await bot.send_message(
            chat_id, text, parse_mode="HTML", disable_web_page_preview=True, **kwargs
        )
    if pin:
        try:
            await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
        except Exception as e:
            logging.warning(f"pin failed: {e}")


# ===== Конкретные уведомления =====

async def notify_leads_group(t: dict, *, lead_id: int, user, phone: str, pin: bool = False):
    try:
        user_label = _admin_user_label(user)
        tour_block, photo = _compose_tour_block(t)
        head = f"🆕 <b>Заявка №{lead_id}</b>\n👤 {escape(user_label)}\n📞 {escape(phone)}"
        text = f"{head}\n{tour_block}"
        await _send_to_admin_group(text, photo, pin=pin)
    except Exception as e:
        logging.error(f"notify_leads_group failed: {e}")


async def notify_question_group(t: dict, *, user, question: str, answer_key: str):
    try:
        user_label = _admin_user_label(user)
        tour_block, photo = _compose_tour_block(t)
        head = (
            f"❓ <b>Вопрос по туру</b>\n"
            f"👤 от {escape(user_label)}\n"
            f"📝 {escape(question)}\n\n"
            f"🧩 Ответьте реплаем на это сообщение и начните с <code>#{answer_key}</code>"
        )
        text = f"{head}\n\n{tour_block}"
        await _send_to_admin_group(text, photo, pin=False)
    except Exception as e:
        logging.error(f"notify_question_group failed: {e}")


def _format_q_header(qid: int) -> str:
    return f"❓ <b>Вопрос по туру</b>  [Q#{qid}]"


_RECENT_GREETING = defaultdict(float)


def _should_greet_once(user_id: int, cooldown: float = 3.0) -> bool:
    now = time.monotonic()
    last = _RECENT_GREETING.get(user_id, 0.0)
    if now - last >= cooldown:
        _RECENT_GREETING[user_id] = now
        return True
    return False


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def is_menu_label(text: str, key: str) -> bool:
    variants = {_norm(TRANSLATIONS[lang][key]) for lang in SUPPORTED_LANGS}
    return _norm(text) in variants


MENU_KEYS = ("menu_find", "menu_gpt", "menu_sub", "menu_settings")


def _is_menu_text(txt: str) -> bool:
    return any(is_menu_label(txt or "", k) for k in MENU_KEYS)


# === helper: «пульс» индикатора набора ===
async def _typing_pulse(chat_id: int):
    try:
        while True:
            await bot.send_chat_action(chat_id, "typing")
            await asyncio.sleep(4.0)
    except asyncio.CancelledError:
        pass

# ==== ЯЗЫК/LOCALE ХЕЛПЕРЫ ====

# безопасный геттер языка пользователя
def _lang(user_id: int | None) -> str:
    try:
        # читаем из key-value стора, который у тебя уже есть
        code = get_config(f"lang_{int(user_id)}", None) if user_id else None
    except Exception:
        code = None
    # гарантируем валидность и откат к дефолту
    return code if code in SUPPORTED_LANGS else DEFAULT_LANG

# сохранить выбранный язык
def set_user_lang(user_id: int, lang: str) -> None:
    save = lang if lang in SUPPORTED_LANGS else DEFAULT_LANG
    set_config(f"lang_{user_id}", save)

# универсальный переводчик (если твоё t() уже есть — оставь его; если нет, используй этот)
def t(user_id: int | None, key: str) -> str:
    lang = _lang(user_id)
    return TRANSLATIONS.get(lang, {}).get(key, TRANSLATIONS[DEFAULT_LANG].get(key, key))

# инлайн-клавиатура выбора языка
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

def lang_inline_kb() -> InlineKeyboardMarkup:
    # подписи человекочитаемые — можешь изменить под себя
    names = {
        "ru": "Русский 🇷🇺",
        "uz": "Oʻzbekcha 🇺🇿",
        "kk": "Қазақша 🇰🇿",
        # если в SUPPORTED_LANGS есть ещё — добавь сюда
    }
    rows = []
    row = []
    for code in SUPPORTED_LANGS:
        text = names.get(code, code.upper())
        row.append(InlineKeyboardButton(text=text, callback_data=f"lang:{code}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

# алиас, чтобы старые вызовы не падали
def main_kb_for(user_id: int) -> ReplyKeyboardMarkup:
    return main_menu_kb(user_id)

# ещё один алиас: версия без user_id — берём дефолтный язык (русский)
def want_contact_kb() -> ReplyKeyboardMarkup:
    # используем уже существующую i18n-клавиатуру, но с user_id=0 => DEFAULT_LANG
    return want_contact_kb_for(0)

# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message):
    uid = message.from_user.id
    if get_config(f"lang_{uid}", None):            # язык уже выбран
        await message.answer(t(uid, "hello"), reply_markup=main_menu_kb(message.from_user.id))
        return
    await message.answer(t(uid, "choose_lang"), reply_markup=lang_inline_kb())

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
    except Exception:
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


# Быстрые команды
async def entry_find_tours(message: Message):
    uid = message.from_user.id
    await message.answer(
        t(uid, "filters.title"),
        reply_markup=filters_inline_kb_for(message.from_user.id))

async def entry_gpt(message: Message):
    await message.answer("Спроси что угодно про путешествия (отели, сезоны, визы, бюджеты).")


async def entry_sub(message: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💳 Payme (автопродление)", callback_data="sub:payme:recurring"),
            ],
            [
                InlineKeyboardButton(text="Разовая оплата через Payme", callback_data="sub:payme:oneoff"),
            ],
            [InlineKeyboardButton(text="ℹ️ Подробнее о тарифах", callback_data="sub:info")],
        ]
    )
    await message.answer(
        "Выбери способ оплаты и тариф (по умолчанию — <b>Basic 49 000 UZS / 30 дней</b>):",
        reply_markup=kb,
    )


async def entry_settings(message: Message):
    uid = message.from_user.id
    await message.answer(t(uid, "choose_lang"), reply_markup=lang_inline_kb())


@dp.message(Command("language"))
@dp.message(Command("settings"))
async def cmd_language(message: Message):
    await entry_settings(message)

SETTINGS_TRIGGERS = {TRANSLATIONS[lang]["menu_settings"] for lang in SUPPORTED_LANGS}

@dp.message(F.text.in_(SETTINGS_TRIGGERS))
async def on_settings_button(message: Message):
    await entry_settings(message)

@dp.callback_query(F.data.startswith("ask:"))
async def cb_ask(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Не удалось открыть форму вопроса.", show_alert=False)
        return

    uid = call.from_user.id
    ASK_STATE[uid] = {"tour_id": tour_id, "since": time.monotonic()}

    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена вопроса")]],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
    )

    await call.message.answer(
        "Напиши свой вопрос по этой карточке. Например:\n"
        "• Не вижу название отеля / страны\n"
        "• Уточните даты или питание\n"
        "• Сколько будет на 2 взрослых и ребёнка\n\n"
        "Чтобы отменить — нажми «❌ Отмена вопроса».",
        reply_markup=cancel_kb,
    )
    await call.answer()


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
    uid = call.from_user.id
    country_raw = call.data.split(":", 1)[1]
    country = normalize_country(country_raw)
    terms = country_terms_for(country)  # ← берём синонимы (RU/EN и т.д.)
    terms_any = COUNTRY_EXPAND_ANY.get(country, [])

    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": country,
        "currency_eq": None,
        "max_price": None,
        "hours": 24,
        "order_by_price": False,
        "ts": time.monotonic(),
    }

    # Фильтруем по 24ч + по любому синониму
    rows = await fetch_tours_page(country_terms=terms, hours=24, limit=6, offset=0)
    if not rows:
        await call.message.answer(
            f"За 24 часа по стране «{country}» нет новых туров.",
            reply_markup=filters_inline_kb_for(uid),
        )
        await call.answer()
        return

    await send_batch_cards(call.message.chat.id, uid, rows, token, len(rows))

    kb_more = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(uid, "more.next"),
                              callback_data=f"more:{token}:{len(rows)}")],
        [InlineKeyboardButton(text=t(uid, "back"), callback_data="back_filters")],
    ])
    await call.message.answer(t(uid, "more.title"), reply_markup=kb_more)
    await call.answer()

@dp.callback_query(F.data.startswith("sub:"))
async def cb_sub(call: CallbackQuery):
    _, provider, kind = call.data.split(":", 2)
    plan_code = "basic_m"

    # создаём заказ как и раньше
    order_id = create_order(call.from_user.id, provider=provider, plan_code=plan_code, kind=kind)

    order = get_order_safe(order_id) or {}
    # ожидаем, что в orders.amount хранится сумма В ТИЙИНАХ
    amount_tiyin = int(order.get("amount") or 4900000)  # fallback на 49 000 UZS

    if provider == "payme":
        mid = PAYME_MERCHANT_ID
        if not mid:
            await call.message.answer("⚠️ PAYME_MERCHANT_ID не задан в ENV.")
            await call.answer()
            return
        url = build_payme_checkout_url(mid, amount_tiyin, order_id, "ru")
    else:
        url = build_checkout_link(provider, order_id, plan_code)

    txt = (
        f"🔐 Заказ №{order_id}\n"
        f"Провайдер: <b>{'Click' if provider=='click' else 'Payme'}</b>\n"
        f"Тариф: <b>Basic</b> (30 дней)\n\n"
        "Нажми, чтобы оплатить. Окно откроется прямо в Telegram."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть оплату", url=url)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )
    await call.message.answer(txt, reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "sub:info")
async def cb_sub_info(call: CallbackQuery):
    await call.message.answer(
        "Тарифы:\n"
        "• Basic — 49 000 UZS/мес: доступ к свежим турам и умным ответам\n"
        "• Pro — 99 000 UZS/мес: приоритет и расширенные источники\n\n"
        "Оплата через Click/Payme. Автопродление можно отключить в любой момент.",
    )
    await call.answer()

@dp.callback_query(F.data.startswith("budget:"))
async def cb_budget(call: CallbackQuery):
    uid = call.from_user.id
    _, cur, limit_s = call.data.split(":")
    cur = normalize_currency(cur)
    limit_val = int(limit_s)

    # Заголовок
    await call.message.answer(
        f"<b>💸 Бюджет: ≤ {limit_val} {cur}</b>\n"
        f"В этом диапазоне ищу свежие предложения за последние 24 часа…"
    )

    # 1) Пробуем за 24 часа, строго по валюте
    token = _new_token()
    PAGER_STATE[token] = {
        "chat_id": call.message.chat.id,
        "query": None,
        "country": None,
        "currency_eq": cur,
        "max_price": limit_val,
        "hours": 24,
        "order_by_price": True,
        "ts": time.monotonic(),
    }
    rows = await fetch_tours_page(
        country=None, currency_eq=cur, max_price=limit_val,
        hours=24, limit=6, offset=0, order_by_price=True,
    )

    # 2) Если пусто — расширяем окно до 5 суток (120 ч), всё ещё в нужной валюте
    if not rows:
        token = _new_token()
        PAGER_STATE[token] = {
            "chat_id": call.message.chat.id,
            "query": None,
            "country": None,
            "currency_eq": cur,
            "max_price": limit_val,
            "hours": 120,
            "order_by_price": True,
            "ts": time.monotonic(),
        }
        rows = await fetch_tours_page(
            country=None, currency_eq=cur, max_price=limit_val,
            hours=120, limit=6, offset=0, order_by_price=True,
        )

    # 3) Если всё ещё пусто — 5 суток, любая валюта (но с фильтром по цене)
    if not rows:
        token = _new_token()
        PAGER_STATE[token] = {
            "chat_id": call.message.chat.id,
            "query": None,
            "country": None,
            "currency_eq": None,
            "max_price": limit_val,
            "hours": 120,
            "order_by_price": True,
            "ts": time.monotonic(),
        }
        rows = await fetch_tours_page(
            country=None, currency_eq=None, max_price=limit_val,
            hours=120, limit=6, offset=0, order_by_price=True,
        )
        if rows:
            await call.message.answer(
                "За последние 24 часа ничего не нашлось — показываю подходящие из базы."
            )

    if not rows:
        await call.message.answer(
            f"В пределах бюджета ≤ {limit_val} {cur} за последние 5 суток ничего не нашли.",
            reply_markup=filters_inline_kb_for(uid),
        )
        await call.answer()
        return

    await send_batch_cards(call.message.chat.id, uid, rows, token, len(rows))
    await call.message.answer("Продолжить подборку?",
                              reply_markup=more_kb(token, len(rows), uid))
    await call.answer()

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

    hours = state.get("hours") or (24 if state.get("country") else 72)
    country = normalize_country(state["country"]) if state.get("country") else None

    rows = await fetch_tours_page(
        query=state.get("query"),
        country=country,
        currency_eq=state.get("currency_eq"),
        max_price=state.get("max_price"),
        hours=hours,  # ← всегда число
        order_by_price=state.get("order_by_price", False),
        limit=6,
        offset=offset,
    )
    if not rows:
        await call.answer("Это всё на сегодня ✨", show_alert=False)
        return

    _touch_state(token)
    await send_batch_cards(call.message.chat.id, call.from_user.id, rows, token, offset + len(rows))



@dp.callback_query(F.data.startswith("wx:"))
async def cb_weather(call: CallbackQuery):
    uid = call.from_user.id
    place = (call.data.split(":", 1)[1] or "").strip() or "Ташкент"
    await call.answer("⏳")
    txt = await get_weather_text(place)
    await call.message.answer(txt, disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("fav:add:"))
async def cb_fav_add(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[2])
    except Exception:
        await call.answer("Ошибка избранного.", show_alert=False)
        return
    set_favorite(call.from_user.id, tour_id)
    await call.answer("Добавлено в избранное ❤️", show_alert=False)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours WHERE id=%s;", (tour_id,))
        t = cur.fetchone()
    if t:
        await call.message.edit_reply_markup(reply_markup=tour_inline_kb(t, True, call.from_user.id))


@dp.callback_query(F.data.startswith("fav:rm:"))
async def cb_fav_rm(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[2])
    except Exception:
        await call.answer("Ошибка избранного.", show_alert=False)
        return
    unset_favorite(call.from_user.id, tour_id)
    await call.answer("Убрано из избранного 🤍", show_alert=False)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours WHERE id=%s;", (tour_id,))
        t = cur.fetchone()
    if t:
        # после удаления показываем кнопку «добавить», т.е. is_fav=False
        await call.message.edit_reply_markup(reply_markup=tour_inline_kb(t, False, call.from_user.id))

from aiogram.types import CallbackQuery, ReplyKeyboardRemove

@dp.callback_query(F.data.startswith("lang:"))
async def cb_lang(call: CallbackQuery):
    uid = call.from_user.id
    lang = call.data.split(":", 1)[1]

    # 1) Сохраняем язык
    set_user_lang(uid, lang)

    # 2) Пытаемся обновить ИНЛАЙН-клавиатуру фильтров (если сейчас открыт «подбор»)
    edited_inline = False
    try:
        await call.message.edit_reply_markup(reply_markup=filters_inline_kb_for(uid))
        edited_inline = True
    except Exception:
        pass  # не тот экран — ок

    # 3) Если есть последняя карточка тура — обновим её текст/кнопки под новый язык
    try:
        last_tours = LAST_RESULTS.get(uid, [])
    except Exception:
        last_tours = []
    if last_tours:
        tour = last_tours[0]
        caption = build_card_text(tour, lang=lang)
        fav = is_favorite(uid, tour["id"])
        kb = tour_inline_kb(tour, fav, uid)
        try:
            await call.message.edit_text(caption, reply_markup=kb)
        except Exception:
            try:
                await call.message.edit_caption(caption, reply_markup=kb)
            except Exception:
                pass  # не критично — всё равно перешлём новое меню ниже

    # 4) ВСЕГДА переотправляем reply-клавиатуру (иначе меню не сменит язык)
    try:
        await bot.send_message(uid, "…", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass

    await bot.send_message(
        uid,
        t(uid, "hello_again"),
        reply_markup=main_menu_kb(uid)  # <-- принципиально: свежая клавиатура
    )

    # 5) Однократно отвечаем на callback
    await call.answer(t(uid, "lang_saved") if (edited_inline or last_tours) else t(uid, "lang_saved"))

@dp.callback_query(F.data.startswith("want:"))
async def cb_want(call: CallbackQuery):
    try:
        tour_id = int(call.data.split(":")[1])
    except Exception:
        await call.answer("Ошибка заявки.", show_alert=False)
        return

    uid = call.from_user.id
    if user_has_leads(uid) and not user_has_subscription(uid):
        await call.message.answer(
            "⚠️ У тебя уже была бесплатная заявка.\n"
            "Для следующих нужно подключить подписку 🔔",
             reply_markup=get_payme_kb(),
        )
        await call.answer()
        return

    WANT_STATE[uid] = {"tour_id": tour_id}
    try:
        set_pending_want(uid, tour_id)
    except Exception as e:
        logging.warning(f"set_pending_want failed: {e}")

    await call.message.answer("Окей! Отправь контакт, чтобы менеджер связался 👇", reply_markup=want_contact_kb())
    await call.answer()


# === ПОГОДА: команды/триггеры ===
@dp.message(Command("weather"))
async def cmd_weather(message: Message):
    uid = message.from_user.id
    lang = _lang(uid)

    ask = (message.text or "").partition(" ")[2].strip()
    place = ask or None  # не подставляем жёстко «Ташкент», пусть парсится из текста или спросим явнее

    await message.answer(TRANSLATIONS[lang].get("weather.loading", "Секунду, уточняю погоду…"))

    txt = await get_weather_text(place, lang=lang)  # <- обязательно передаём lang
    await message.answer(txt, disable_web_page_preview=True)


# Триггер по словам «погода / ob-havo / ауа райы» на разных языках
@dp.message(F.text.regexp(r"(?iu)\b(погод|ob[-\s]?havo|ауа\s*райы)\b"))
async def handle_weather(message: Message):
    uid = message.from_user.id
    lang = _lang(uid)

    place = _extract_place_from_weather_query(message.text or "")
    txt = await get_weather_text(place, lang=lang)
    await message.answer(txt, disable_web_page_preview=True)

@dp.message(F.chat.type == "private", F.contact)
async def on_contact(message: Message):
    st = WANT_STATE.pop(message.from_user.id, None)
    if not st:
        logging.info(f"Contact came without pending want (user_id={message.from_user.id})")
        await message.answer(
            "Контакт получен. Если нужен подбор, нажми «🎒 Найти туры».",
            reply_markup=main_kb_for(message.from_user.id)
        )
        return

    phone = message.contact.phone_number
    tour_id = st["tour_id"]

    full_name = (getattr(message.from_user, "full_name", "") or "").strip()
    if not full_name:
        parts = [(message.from_user.first_name or ""), (message.from_user.last_name or "")]
        full_name = (
            " ".join(p for p in parts if p).strip()
            or (f"@{message.from_user.username}" if message.from_user.username else "Telegram user")
        )

    lead_id = create_lead(tour_id, phone, full_name, note="from contact share")

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_select_tours_clause()}
            FROM tours WHERE id=%s;
        """,
            (tour_id,),
        )
        t = cur.fetchone()

    if t and lead_id:
        await notify_leads_group(t, lead_id=lead_id, user=message.from_user, phone=phone, pin=False)
        append_lead_to_sheet(lead_id, message.from_user, phone, t)
        await message.answer(
            f"Принято! Заявка №{lead_id}. Менеджер скоро свяжется 📞",
            reply_markup=main_kb_for(message.from_user.id)
        )
    else:
        await message.answer(
            "Контакт получен, но не удалось создать заявку. Попробуй ещё раз или напиши менеджеру.",
            reply_markup=main_kb_for(message.from_user.id)
        )


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer("Скоро добавим детальные фильтры 🤝", show_alert=False)


@dp.callback_query(F.data == "back_filters")
async def back_filters(call: CallbackQuery):
    lang = _lang(call.from_user.id)
    await call.message.edit_text(
        TRANSLATIONS[lang]["filters.title"],
        reply_markup=filters_inline_kb_for(call.from_user.id)
    )
    await call.answer()

@dp.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    await call.message.answer(t(call.from_user.id, "hello"), reply_markup=main_kb_for(call.from_user.id))
    

# срабатывает ТОЛЬКО если юзер находится в ASK_STATE
@dp.message(F.chat.type == "private", F.text, lambda m: m.from_user.id in ASK_STATE)
async def on_question_text(message: Message):
    st = ASK_STATE.get(message.from_user.id)
    txt = (message.text or "").strip()

    if txt.lower() in {"отмена", "❌ отмена вопроса"} or txt.startswith("❌"):
        ASK_STATE.pop(message.from_user.id, None)
        await message.answer("Ок, вопрос отменён.", reply_markup=main_kb_for(message.from_user.id))
        return

    tour_id = st.get("tour_id")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_select_tours_clause()} FROM tours WHERE id=%s;", (tour_id,))
        t = cur.fetchone()

    if not t:
        ASK_STATE.pop(message.from_user.id, None)
        await message.answer(
            "Не нашёл карточку тура. Попробуй ещё раз из карточки.",
            reply_markup=main_kb_for(message.from_user.id),
        )
        return

    # генерируем ключ и запоминаем, кому слать ответ
    answer_key = secrets.token_urlsafe(4)  # короткий ключ
    ANSWER_MAP[answer_key] = {"user_id": message.from_user.id, "tour_id": tour_id}

    # отправляем в админ-группу (ОДИН вызов, с ключом!)
    await notify_question_group(t, user=message.from_user, question=txt, answer_key=answer_key)

    ASK_STATE.pop(message.from_user.id, None)
    await message.answer(
        "Спасибо! Передал вопрос менеджеру — вернёмся с уточнениями 📬",
        reply_markup=main_kb_for(message.from_user.id),
    )

@dp.message(F.text.func(_is_menu_text))
async def on_menu_buttons(message: Message):
    uid = message.from_user.id
    txt = (message.text or "").strip()

    if is_menu_label(txt, "menu_find"):
        await entry_find_tours(message)
        return

    if is_menu_label(txt, "menu_gpt"):
        if not user_has_subscription(uid):
            await safe_answer(
                message,
                "🤖 GPT доступен только по подписке.\nПодключи её здесь:",
                reply_markup=get_payme_kb(),
            )
            return
        await entry_gpt(message)
        return

    if is_menu_label(txt, "menu_sub"):
        await entry_sub(message)
        return

    if is_menu_label(txt, "menu_settings"):
        await entry_settings(message)
        return
        
from aiogram import F
from aiogram.types import CallbackQuery

@dp.callback_query(F.data.startswith("noop:"))
async def noop(cb: CallbackQuery):
    await cb.answer("Ссылка ещё не настроена.", show_alert=True)

# --- Смарт-роутер текста
@dp.message(F.chat.type == "private", F.text)
async def smart_router(message: Message):
    user_text = (message.text or "").strip()

    # если нажали кнопку меню — не обрабатываем тут
    if any(is_menu_label(user_text, k) for k in ("menu_find", "menu_gpt", "menu_sub", "menu_settings")):
        return

    # пульс «печатает…» на время обработки
    pulse = asyncio.create_task(_typing_pulse(message.chat.id))
    try:
        # быстрые источники по фразам «ссылка/источник»
        if re.search(r"\b((дай\s+)?ссылк\w*|источник\w*|link)\b", user_text, flags=re.I):
            last = LAST_RESULTS.get(message.from_user.id) or []
            premium_users = {123456789}
            is_premium = message.from_user.id in premium_users

            if not last:
                guess = _guess_query_from_link_phrase(user_text) or LAST_QUERY_TEXT.get(message.from_user.id)
                if guess:
                    rows, _is_recent = await fetch_tours(guess, hours=168, limit_recent=6, limit_fallback=6)
                    if rows:
                        LAST_RESULTS[message.from_user.id] = rows
                        last = rows

            if not last:
                q_hint = LAST_QUERY_TEXT.get(message.from_user.id)
                hint_txt = (f"По последнему запросу «{escape(q_hint)}» ничего свежего не нашёл."
                            if q_hint else "Не вижу последних карточек.")
                await message.answer(
                    f"{hint_txt} Нажми «🎒 Найти туры» и выбери вариант — тогда пришлю источник.",
                    reply_markup=filters_inline_kb(),
                )
                return

            shown = 0
            for trow in last[:3]:
                src = (trow.get("source_url") or "").strip()
                if is_premium and src:
                    await message.answer(
                        f'🔗 Источник: <a href="{escape(src)}">перейти к посту</a>',
                        disable_web_page_preview=True,
                    )
                else:
                    ch = (trow.get("source_chat") or "").lstrip("@")
                    when = localize_dt(trow.get("posted_at"))
                    label = f"Источник: {escape(ch) or 'тур-канал'}, {when or 'дата неизвестна'}"
                    hint = " • В Premium покажу прямую ссылку."
                    await message.answer(f"{label}{hint}")
                shown += 1

            if shown == 0:
                await message.answer("Для этого набора источников прямых ссылок нет. Попробуй свежие туры через фильтры.")
            return

        # погода (быстрый ответ)
        if re.search(r"\bпогод", user_text, flags=re.I):
            place = _extract_place_from_weather_query(user_text)
            await message.answer("Секунду, уточняю погоду…")
            reply = await get_weather_text(place or "")
            await message.answer(reply, disable_web_page_preview=True)
            return

        # ===== «актуальные/свежие/горящие туры» =====
        m_recent = re.search(r"\b(актуальн\w*|свеж\w*|горящ\w*|последн\w*)\s+(туры|предложени\w*)\b", user_text, flags=re.I)
        m_72 = re.search(r"\b(72\s*ч|за\s*72\s*час\w*|за\s*3\s*дн\w*)\b", user_text, flags=re.I)
        m_sort_price = re.search(r"\b(дешевле|дешёвые|по\s*цене|сортировк\w+\s*по\s*цене)\b", user_text, flags=re.I)

        if m_recent or m_72:
            rows = await fetch_tours_page(hours=72, order_by_price=bool(m_sort_price), limit=6, offset=0)
            header = "🔥 Актуальные за 72 часа" + (" — дешевле → дороже" if m_sort_price else "")
            await message.answer(f"<b>{header}</b>")

            token = _new_token()
            PAGER_STATE[token] = {
                "chat_id": message.chat.id, "query": None, "country": None, "currency_eq": None,
                "max_price": None, "hours": 72, "order_by_price": bool(m_sort_price), "ts": time.monotonic(),
            }
            await send_batch_cards(message.chat.id, message.from_user.id, rows, token, len(rows))
            return

        # короткие смысловые запросы → подбор туров
        m_interest = re.search(r"^(?:мне\s+)?(.+?)\s+интересует(?:\s*!)?$", user_text, flags=re.I)
        if m_interest or (len(user_text) <= 30):
            q_raw = m_interest.group(1) if m_interest else user_text
            q = _guess_query_from_link_phrase(q_raw) or q_raw

            queries = _expand_query(q)
            rows_all: List[dict] = []
            for qx in queries:
                rows, _is_recent = await fetch_tours(qx, hours=72, limit_recent=6, limit_fallback=0)
                if rows:
                    rows_all.extend(rows)

            if not rows_all:
                rows_all, _ = await fetch_tours(q, hours=168, limit_recent=0, limit_fallback=6)

            # дедуп по id
            seen = set()
            rows_all = [r for r in rows_all if not (r.get("id") in seen or seen.add(r.get("id")))]

            if rows_all:
                _remember_query(message.from_user.id, q)
                await message.answer(f"<b>Нашёл варианты по запросу: {escape(q)}</b>")
                token = _new_token()
                PAGER_STATE[token] = {
                    "chat_id": message.chat.id, "query": q, "country": None, "currency_eq": None,
                    "max_price": None, "hours": 72, "order_by_price": False, "ts": time.monotonic(),
                }
                await send_batch_cards(message.chat.id, message.from_user.id, rows_all[:6], token, len(rows_all[:6]))
                return

        # чуть длиннее — пробуем «72ч» по фразе
        if len(user_text) <= 40:
            rows, is_recent = await fetch_tours(user_text, hours=72)
            if rows:
                _remember_query(message.from_user.id, user_text)
                header = "🔥 Нашёл актуальные за 72 часа:" if is_recent else "ℹ️ Свежих 72ч нет — вот последние варианты:"
                await message.answer(f"<b>{header}</b>")
                token = _new_token()
                PAGER_STATE[token] = {
                    "chat_id": message.chat.id, "query": user_text, "country": None, "currency_eq": None,
                    "max_price": None, "hours": 72 if is_recent else None, "order_by_price": False, "ts": time.monotonic(),
                }
                await send_batch_cards(message.chat.id, message.from_user.id, rows, token, len(rows))
                return

        # fallback → без GPT (предлагаем кнопки)
        await message.answer(
            "Пока не понял запрос. Нажми «🎒 Найти туры» или «🤖 Спросить GPT» (нужна подписка).",
            reply_markup=main_kb_for(message.from_user.id),
        )
        return

    finally:
        pulse.cancel()

# ---- helpers ----
def _extract_answer_key_from_message(msg: Message) -> Optional[str]:
    """Ищем #ключ в самом сообщении и/или в том, на которое ответили (text/caption)."""
    def _find(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        m = re.search(r"#([A-Za-z0-9_\-]{5,})", s)
        return m.group(1) if m else None

    # 1) пытаемся в самом ответе
    key = _find(getattr(msg, "text", None)) or _find(getattr(msg, "caption", None))
    if key:
        return key

    # 2) пробуем в исходном сообщении, на которое сделали reply
    r = getattr(msg, "reply_to_message", None)
    if r:
        return _find(getattr(r, "text", None)) or _find(getattr(r, "caption", None))
    return None

# ответ админа из группы: ДОЛЖЕН быть reply на сообщение бота (ключ можно не писать)
@dp.message(F.reply_to_message)
async def on_admin_group_answer(message: Message):
    # обрабатываем только нужную группу/топик
    if message.chat.id != resolve_leads_chat_id():
        return
    if LEADS_TOPIC_ID and getattr(message, "message_thread_id", None) != LEADS_TOPIC_ID:
        return

    key = _extract_answer_key_from_message(message)
    if not key:
        # тихо выходим, чтобы не спамить группу — менеджер ответил не на то сообщение
        return

    route = ANSWER_MAP.pop(key, None)
    if not route:
        await message.reply("Ключ ответа не найден или устарел. Попросите пользователя задать вопрос заново.")
        return

    user_id = route["user_id"]

    # сам текст ответа менеджера
    text_raw = (message.text or message.caption or "").strip()
    # если вдруг менеджер всё-таки дописал #ключ — уберём его из тела
    text_to_user = re.sub(r"#([A-Za-z0-9_\-]{5,})\s*", "", text_raw, count=1).strip()
    if not text_to_user:
        await message.reply("Пустой ответ не отправлен.")
        return

    try:
        await bot.send_message(
            user_id,
            f"📩 Ответ от менеджера:\n\n{escape(text_to_user)}",
            disable_web_page_preview=True,
        )
        await message.reply("Отправлено пользователю ✅")
    except Exception as e:
        logging.error("forward answer failed: %s", e)
        await message.reply("Не смог отправить пользователю.")

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
        ensure_orders_columns()
    except Exception as e:
        logging.error(f"orders ensure failed: {e}")

    try:
        ensure_pending_wants_table()
        ensure_leads_schema()
        ensure_favorites_schema()
        ensure_questions_schema()
    except Exception as e:
        logging.error(f"Schema ensure failed: {e}")

    try:
        gc = _get_gs_client()
        if not gc:
            logging.info("GS warmup skipped: client is None (нет кредов или ID)")
        else:
            sid = SHEETS_SPREADSHEET_ID or "(empty)"
            logging.info(f"GS warmup: trying open spreadsheet id='{sid}'")
            sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
            logging.info(f"GS warmup: opened spreadsheet title='{sh.title}'")
            try:
                titles = [ws.title for ws in sh.worksheets()]
                logging.info(f"GS warmup: worksheets={titles}")
            except Exception as e_list:
                logging.warning(f"GS: cannot list worksheets: {e_list}")
            header = [
                "created_utc",
                "lead_id",
                "username",
                "full_name",
                "phone",
                "country",
                "city",
                "hotel",
                "price",
                "currency",
                "dates",
                "source_url",
                "posted_local",
                "board",
                "includes",
            ]
            ws = _ensure_ws(sh, os.getenv("WORKSHEET_NAME", "Заявки"), header)
            _ensure_header(ws, header)
            logging.info(f"✅ GS warmup: лист '{ws.title}' готов (rows={ws.row_count}, cols={ws.col_count})")
    except gspread.SpreadsheetNotFound as e:
        logging.error(
            f"GS warmup failed: spreadsheet not found by id='{SHEETS_SPREADSHEET_ID}': {e}"
        )
    except gspread.exceptions.APIError as e:
        logging.error(f"GS warmup failed (APIError): {e}")
    except Exception as e:
        logging.error(f"GS warmup failed (generic): {e}")

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS board TEXT;")
            cur.execute("ALTER TABLE IF EXISTS tours ADD COLUMN IF NOT EXISTS includes TEXT;")
    except Exception as e:
        logging.warning(f"Ensure tours columns failed: {e}")

    try:
        with get_conn() as conn, conn.cursor() as cur:
            info = conn.info
            logging.info(
                f"🗄 DB DSN: host={info.host} db={info.dbname} user={info.user} port={info.port}"
            )
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'tours'
                ORDER BY ordinal_position
                """
            )
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

# ====== Payme JSON-RPC helpers ======
def _now_ms() -> int:
    return int(time.time() * 1000)

def _rpc_ok(rpc_id, payload: dict):
    return {"jsonrpc": "2.0", "id": rpc_id, "result": payload}

def _rpc_err(rpc_id, code: int, ru: str, uz: str | None = None, en: str | None = None, data: str | None = None):
    return {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {
            "code": code,
            "message": {"ru": ru, "uz": uz or ru, "en": en or ru},
            **({"data": data} if data else {}),
        },
    }

# --- Авторизация ---
def _payme_auth_ok_from_header(header_val: str | None) -> bool:
    """
    Принимаем Basic <base64(login:password)>, где login может быть:
      • 'Paycom' (официально для Merchant API)
      • PAYME_MERCHANT_ID (разрешим альтернативно)
    password должен совпадать с тестовым или боевым ключом.
    """
    if not header_val or not header_val.startswith("Basic "):
        return False

    # Позволяем точное совпадение с XAUTH из ENV
    xauth_raw = (os.getenv("PAYME_MERCHANT_XAUTH") or "").strip()
    if xauth_raw and header_val.strip() == xauth_raw:
        return True

    try:
        raw = base64.b64decode(header_val.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False

    login, _, pwd = raw.partition(":")
    if not pwd:
        return False

    allowed_logins = {"Paycom"}
    mid = (os.getenv("PAYME_MERCHANT_ID") or "").strip()
    if mid:
        allowed_logins.add(mid)

    keys = {
        (os.getenv("PAYME_MERCHANT_TEST_KEY") or "").strip(),
        (os.getenv("PAYME_MERCHANT_KEY") or "").strip(),
    }
    keys.discard("")

    return (login in allowed_logins) and (pwd in keys)

def _payme_auth_check(headers: dict) -> bool:
    # Песочница шлёт Authorization, иногда X-Auth
    auth = headers.get("Authorization") or headers.get("authorization")
    xauth = headers.get("X-Auth") or headers.get("x-auth")
    return _payme_auth_ok_from_header(auth) or _payme_auth_ok_from_header(xauth)

# --- Работа с заказом/суммой ---
def _get_order(order_id: int) -> dict | None:
    with _pay_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s;", (order_id,))
        return cur.fetchone()

def _order_amount_tiyin(o: dict) -> int | None:
    val = o.get("amount")
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return None

# --- In-memory реестр транзакций (для идемпотентности песочницы) ---
# Ключ: payme_transaction_id (str)
# Значение:
# { "order_id": int, "amount": int, "state": int,
#   "create_time": int, "perform_time": int, "cancel_time": int,
#   "reason": int }
TRX_STORE: dict[str, dict] = {}

def _trx_from_db(trx_id: str) -> dict | None:
    """
    Пытаемся восстановить состояние из БД (если было).
    """
    try:
        with _pay_db() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id AS order_id, amount, status,
                       EXTRACT(EPOCH FROM created_at)*1000 AS create_time,
                       EXTRACT(EPOCH FROM perform_time)*1000 AS perform_time,
                       EXTRACT(EPOCH FROM cancel_time)*1000 AS cancel_time,
                       COALESCE(reason, 0) AS reason
                FROM orders
                WHERE provider_trx_id=%s
                LIMIT 1;
            """, (trx_id,))
            r = cur.fetchone()
            if not r:
                return None
            st_map = {"new": 0, "created": 1, "paid": 2, "canceled": -1, "canceled_after_perform": -2}
            data = {
                "order_id": int(r["order_id"]),
                "amount": _order_amount_tiyin(r) or 0,
                "state": st_map.get((r["status"] or "").strip(), 0),
                "create_time": int(r["create_time"] or 0),
                "perform_time": int(r["perform_time"] or 0),
                "cancel_time": int(r["cancel_time"] or 0),
                "reason": int(r["reason"] or 0),
            }
            TRX_STORE[trx_id] = data
            return data
    except Exception:
        return None

# ---- мок для быстрого получения order_id/amount ----
@app.api_route("/payme/mock/new", methods=["GET"])
@app.api_route("/payme/mock/new/{amount}", methods=["GET"])
async def payme_mock_new(amount: int = 4900000):
    oid = create_order(ADMIN_USER_ID or 0, provider="payme", plan_code="basic_m", kind="merchant")
    with _pay_db() as conn, conn.cursor() as cur:
        try:
            cur.execute("UPDATE orders SET amount=%s WHERE id=%s", (amount, oid))
        except Exception:
            logging.exception("mock new: set amount failed")
    return {"order_id": oid, "amount": amount}

# ---- основной JSON-RPC обработчик ----
from fastapi import Header
from fastapi.responses import JSONResponse

@app.post("/payme/merchant")
async def payme_merchant(request: Request, x_auth: str | None = Header(default=None)):
    body    = await request.json()
    req_id  = body.get("id")
    method  = (body.get("method") or "").strip()
    params  = body.get("params") or {}
    account = params.get("account") or {}

    auth_ok = _payme_auth_check(request.headers)
    if not auth_ok:
        return JSONResponse(_rpc_err(req_id, -32504, "Недопустимая авторизация"))

    amount_in = params.get("amount")
    trx_id_in = params.get("id")
    order_id  = account.get("order_id")

    logging.info("[Payme] method=%s order_id=%s amount_in=%s auth_ok=%s",
                 method, order_id, amount_in, True)

    # --- ПРЕ-ЗАГРУЗКА ЗАКАЗА (ОТДЕЛЬНЫЙ БЛОК, НЕ ЧАСТЬ СВИЧА!) ---
    order = None
    if method in {"CheckPerformTransaction", "CreateTransaction"}:
        try:
            if order_id is not None:
                order = _get_order(int(order_id))
        except Exception:
            order = None

    # ================== METHOD SWITCH ==================
    if method == "CheckPerformTransaction":
        if not order:
            return JSONResponse(_rpc_err(req_id, -31050, "Заказ не найден"))
        expected = _order_amount_tiyin(order)
        if expected is None:
            return JSONResponse(_rpc_err(req_id, -31008, "Сумма в заказе не задана"))
        try:
            sent = int(amount_in)
        except Exception:
            return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
        if sent != expected:
            logging.warning("[Payme] amount mismatch: sent=%s expected=%s order_id=%s", sent, expected, order_id)
            return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
        return JSONResponse(_rpc_ok(req_id, {"allow": True}))
    
    elif method == "CreateTransaction":
        payme_trx = str(trx_id_in or "").strip()
        client_ms = int(params.get("time") or 0)   # ВАЖНО: время от Paycom
        if not payme_trx or client_ms <= 0:
            return JSONResponse(_rpc_err(req_id, -32602, "Invalid params"))
    
        # 1) идемпотентность
        snap = TRX_STORE.get(payme_trx) or _trx_from_db(payme_trx)
        if snap:
            try:
                sent = int(amount_in)
            except Exception:
                return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
            if snap.get("amount") not in (None, sent):
                return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
            return JSONResponse(_rpc_ok(req_id, {
                "create_time": int(snap.get("create_time") or 0),
                "transaction": payme_trx,
                "state": 2 if int(snap.get("state") or 1) == 2 else 1
            }))
    
        # 2) проверка заказа/суммы
        if not order:
            return JSONResponse(_rpc_err(req_id, -31050, "Заказ не найден"))
    
        order_status = (order.get("status") or "").strip().lower()
        if order_status in {"paid", "canceled", "canceled_after_perform"}:
            return JSONResponse(_rpc_err(req_id, -31099, "Невозможно создать транзакцию для данного заказа"))
    
        expected = _order_amount_tiyin(order)
        if expected is None:
            return JSONResponse(_rpc_err(req_id, -31008, "Сумма в заказе не задана"))
    
        try:
            sent = int(amount_in)
        except Exception:
            return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
    
        if sent != expected:
            logging.warning(f"[Payme] Create mismatch: sent={sent} expected={expected} order_id={order_id}")
            return JSONResponse(_rpc_err(req_id, -31001, "Неверная сумма"))
    
        # 3) запись trx — фиксируем created_at из client_ms (params.time)
        create_time = int(params.get("time") or 0)
        if create_time <= 0:
            return JSONResponse(_rpc_err(req_id, -32602, "Invalid params"))
        
        try:
            with _pay_db() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT provider_trx_id FROM orders WHERE id=%s;", (int(order_id),))
                row = cur.fetchone()
                if row and row.get("provider_trx_id") and row["provider_trx_id"] != payme_trx:
                    return JSONResponse(_rpc_err(req_id, -31099, "Транзакция уже существует для этого заказа"))
        
                cur.execute(
                    """
                    UPDATE orders
                       SET provider_trx_id=%s,
                           status=%s,
                           created_at = COALESCE(created_at, to_timestamp(%s/1000.0))
                     WHERE id=%s
                    """,
                    (payme_trx, "created", create_time, int(order_id)),
                )
                # ← читаем create_ms из БД с округлением, чтобы везде был ОДИНАКОВЫЙ int
                cur.execute(
                    """
                    SELECT ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS create_ms
                      FROM orders
                     WHERE provider_trx_id=%s
                     LIMIT 1
                    """,
                    (payme_trx,),
                )
                row2 = cur.fetchone()
                db_create_ms = int(row2["create_ms"] or create_time)
                conn.commit()
        except Exception:
            logging.exception("[Payme] DB error in CreateTransaction")
            return JSONResponse(_rpc_err(req_id, -32400, "Внутренняя ошибка (create)"))
        
        # синхронизируем кэш ровно этим же значением
        TRX_STORE[payme_trx] = {
            "order_id": int(order_id),
            "amount": sent,
            "state": 1,
            "create_time": db_create_ms,
            "perform_time": 0,
            "cancel_time": 0,
            "reason": None,
        }
        
        return JSONResponse(_rpc_ok(req_id, {
            "create_time": db_create_ms,
            "transaction": payme_trx,
            "state": 1
        }))

    
    # -------- PerformTransaction --------
    elif method == "PerformTransaction":
        payme_trx = str(trx_id_in or "").strip()
        trx = _trx_from_db(payme_trx) or TRX_STORE.get(payme_trx)
        if not trx:
            return JSONResponse(_rpc_err(req_id, -31003, "Транзакция не найдена"))
    
        if int(trx.get("state") or 1) == 2:
            return JSONResponse(_rpc_ok(req_id, {
                "perform_time": int(trx.get("perform_time") or 0),
                "transaction": payme_trx,
                "state": 2
            }))
    
        perform_ms = _now_ms()
        try:
            with _pay_db() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET status=%s, perform_time=to_timestamp(%s/1000.0) WHERE provider_trx_id=%s;",
                    ("paid", perform_ms, payme_trx)
                )
                conn.commit()
        except Exception:
            logging.exception("[Payme] DB error in PerformTransaction")
            return JSONResponse(_rpc_err(req_id, -32400, "Внутренняя ошибка (perform)"))
    
        trx.update({"state": 2, "perform_time": perform_ms})
        TRX_STORE[payme_trx] = trx
    
        logging.info(f"[Payme] PerformTransaction OK trx_id={payme_trx}")
        return JSONResponse(_rpc_ok(req_id, {
            "perform_time": perform_ms,
            "transaction": payme_trx,
            "state": 2
        }))
    
    # -------- CancelTransaction --------
    elif method == "CancelTransaction":
        payme_trx = str(trx_id_in or "").strip()
        if not payme_trx:
            return JSONResponse(_rpc_err(req_id, -31003, "Транзакция не найдена"))
    
        cancel_reason = params.get("reason")
        try:
            cancel_reason = int(cancel_reason) if cancel_reason is not None else None
        except Exception:
            cancel_reason = None
    
        try:
            trx = _trx_from_db(payme_trx) or TRX_STORE.get(payme_trx)
            if not trx:
                return JSONResponse(_rpc_err(req_id, -31003, "Транзакция не найдена"))
    
            cur_state     = int(trx.get("state", 1))
            create_time   = int(trx.get("create_time", 0)) or _now_ms()
            perform_time  = int(trx.get("perform_time", 0))
            cancel_time   = int(trx.get("cancel_time", 0))
            stored_reason = trx.get("reason")
    
            # идемпотентность
            if cur_state == -1:
                return JSONResponse(_rpc_ok(req_id, {
                    "cancel_time": cancel_time,
                    "transaction": payme_trx,
                    "state": -1,
                    "reason": stored_reason,
                }))
    
            new_state = -1
            new_status_db = "canceled_after_perform" if (cur_state == 2 or perform_time > 0) else "canceled"
    
            if not cancel_time:
                cancel_time = _now_ms()
    
            with _pay_db() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT id, status FROM orders WHERE provider_trx_id=%s LIMIT 1;", (payme_trx,))
                row = cur.fetchone()
                if row:
                    prev_status = (row["status"] or "").strip().lower()
                    if prev_status != new_status_db:
                        cur.execute(
                            """
                            UPDATE orders
                               SET status=%s,
                                   cancel_time=to_timestamp(%s/1000.0),
                                   reason=%s
                             WHERE id=%s
                            """,
                            (new_status_db, cancel_time, cancel_reason, row["id"])
                        )
                conn.commit()
    
            trx = {
                "create_time": create_time,
                "perform_time": perform_time,
                "cancel_time": cancel_time,
                "state": new_state,                 # только -1
                "reason": cancel_reason if cancel_reason is not None else stored_reason,
            }
            TRX_STORE[payme_trx] = trx
    
            return JSONResponse(_rpc_ok(req_id, {
                "cancel_time": cancel_time,
                "transaction": payme_trx,
                "state": -1,
                "reason": trx["reason"],
            }))
    
        except Exception:
            logging.exception("[Payme] CancelTransaction error")
            return JSONResponse(_rpc_err(req_id, -32400, "Внутренняя ошибка (cancel)"))
    
    elif method == "CheckTransaction":
        try:
            payme_trx = str(params.get("id") or "").strip()
            if not payme_trx:
                return JSONResponse(_rpc_err(req_id, -32602, "Invalid params"))
    
            # 1) ПЕРВЫМ делом — кэш (идентичен между вызовами)
            trx = TRX_STORE.get(payme_trx)
            if trx:
                payload = {
                    "create_time": int(trx.get("create_time") or 0),
                    "perform_time": int(trx.get("perform_time") or 0),
                    "cancel_time": int(trx.get("cancel_time") or 0),
                    "transaction": payme_trx,
                    "state": int(trx.get("state") or 1),
                }
                if payload["state"] < 0:
                    payload["reason"] = int(trx.get("reason") or 0)
                return JSONResponse(_rpc_ok(req_id, payload))
    
            # 2) Фолбэк — БД (кэш не найден, например после рестарта)
            with _pay_db() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT
                        ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS create_ms,
                        ROUND(EXTRACT(EPOCH FROM perform_time) * 1000)::BIGINT AS perform_ms,
                        ROUND(EXTRACT(EPOCH FROM cancel_time)  * 1000)::BIGINT AS cancel_ms,
                        status,
                        COALESCE(reason,0) AS reason
                      FROM orders
                     WHERE provider='payme' AND provider_trx_id=%s
                     LIMIT 1
                """, (payme_trx,))
                r = cur.fetchone()
    
            if not r:
                return JSONResponse(_rpc_err(req_id, -31003, "Transaction not found"))
    
            s = (r["status"] or "").strip().lower()
            state = 2 if s in ("paid", "performed", "done") else (-1 if s in ("canceled_after_perform","refunded","canceled") else 1)
    
            payload = {
                "create_time": int(r.get("create_ms") or 0),
                "perform_time": int(r.get("perform_ms") or 0),
                "cancel_time": int(r.get("cancel_ms") or 0),
                "transaction": payme_trx,
                "state": state,
            }
            if state < 0:
                payload["reason"] = int(r.get("reason") or 0)
    
            return JSONResponse(_rpc_ok(req_id, payload))
    
        except Exception:
            logging.exception("[Payme] CheckTransaction fatal")
            return JSONResponse(_rpc_err(req_id, -32400, "Внутренняя ошибка (check)"))
    
    # -------- GetStatement --------
    elif method == "GetStatement":
        if not (auth_ok or _payme_sandbox_ok(request)):
            return JSONResponse(_rpc_err(req_id, -32504, "Insufficient privileges"))
    
        try:
            frm = int(params.get("from"))
            to  = int(params.get("to"))
        except Exception:
            return JSONResponse(_rpc_err(req_id, -32602, "Неверные параметры (from/to)"))
    
        if to < frm:
            frm, to = to, frm
    
        def _state_from_status(status: str) -> int:
            s = (status or "").strip().lower()
            if s in ("paid", "performed", "done"): return 2
            if s in ("canceled_after_perform", "refunded", "canceled"): return -1
            return 1
    
        txs = []
        for trx_id, t in (TRX_STORE or {}).items():
            ctime = int(t.get("create_time") or 0)
            if frm <= ctime <= to:
                state = int(t.get("state") or 1)
                item = {
                    "id": trx_id,
                    "time": ctime,
                    "amount": int(t.get("amount") or 0),
                    "account": {"order_id": str(t.get("order_id", ""))},
                    "create_time": ctime,
                    "perform_time": int(t.get("perform_time") or 0),
                    "cancel_time": int(t.get("cancel_time") or 0),
                    "transaction": trx_id,
                    "state": state,
                }
                if state < 0:
                    item["reason"] = int(t.get("reason") or 0)
                txs.append(item)
    
        try:
            with _pay_db() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT provider_trx_id,
                           id AS order_id,
                           amount,
                           status,
                           EXTRACT(EPOCH FROM created_at)*1000 AS create_ms,
                           EXTRACT(EPOCH FROM perform_time)*1000 AS perform_ms,
                           EXTRACT(EPOCH FROM cancel_time)*1000  AS cancel_ms,
                           COALESCE(reason,0) AS reason
                      FROM orders
                     WHERE provider='payme'
                       AND provider_trx_id IS NOT NULL
                       AND EXTRACT(EPOCH FROM created_at)*1000 BETWEEN %s AND %s
                    """,
                    (frm, to),
                )
                seen = {x["id"] for x in txs}
                for r in cur.fetchall():
                    trx_id = r["provider_trx_id"]
                    if trx_id in seen:
                        continue
                    state = _state_from_status(r["status"])
                    item = {
                        "id": trx_id,
                        "time": int(r["create_ms"] or 0),
                        "amount": int(r["amount"] or 0),
                        "account": {"order_id": str(r["order_id"])},
                        "create_time": int(r["create_ms"] or 0),
                        "perform_time": int(r["perform_ms"] or 0),
                        "cancel_time": int(r["cancel_ms"] or 0),
                        "transaction": trx_id,
                        "state": state,
                    }
                    if state < 0:
                        item["reason"] = int(r["reason"] or 0)
                    txs.append(item)
            conn.commit()
        except Exception:
            logging.exception("[Payme] DB error in GetStatement")
            return JSONResponse(_rpc_err(req_id, -32400, "Внутренняя ошибка (getStatement)"))
    
        logging.info("[Payme] GetStatement OUT: %d tx(s)", len(txs))
        return JSONResponse(_rpc_ok(req_id, {"transactions": txs}))
    
    # неизвестный метод
    return JSONResponse(_rpc_err(req_id, -32601, "Метод не найден"))

# ---- callback (как было) ----
@app.post("/payme/callback")
async def payme_cb(request: Request):
    form = dict(await request.form())
    ok, msg, order_id, trx = payme_handle_callback(form, dict(request.headers))
    if ok and order_id:
        try:
            activate_after_payment(order_id)
            o = get_order_safe(order_id)
            if o:
                await bot.send_message(
                    o["user_id"], f"✔️ Оплата принята. Подписка активна до {fmt_sub_until(o['user_id'])}"
                )
        except Exception:
            pass
    return JSONResponse({"status": "ok" if ok else "error", "message": msg})

@app.get("/pay/success")
async def pay_success():
    return JSONResponse(
        {"status": "ok", "html": "<h3>Оплата принята. Можно закрыть окно и вернуться в бота ✨</h3>"}
    )


@app.get("/pay/cancel")
async def pay_cancel():
    return JSONResponse({"status": "canceled", "html": "<h3>Платёж отменён. Попробуйте снова из бота.</h3>"})
