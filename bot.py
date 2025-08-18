# bot.py
import os
import json
import base64
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
import asyncio

# ===== ENV =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
TOURS_SHEET = os.getenv("TOURS_SHEET", "Tours")

FRESH_DAYS = int(os.getenv("FRESH_DAYS", "30"))         # считаем свежими N дней
UZS_PER_USD = float(os.getenv("UZS_PER_USD", "12800"))  # грубая конвертация UZS->USD

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN обязателен")
if not SHEETS_SPREADSHEET_ID:
    raise RuntimeError("SHEETS_SPREADSHEET_ID обязателен")

# ===== Google Sheets =====
def _gspread_client():
    raw = os.getenv("GOOGLE_CREDENTIALS")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS обязателен")
    creds_text = base64.b64decode(raw).decode("utf-8") if not raw.lstrip().startswith("{") else raw
    info = json.loads(creds_text)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

gc = _gspread_client()
ws = gc.open_by_key(SHEETS_SPREADSHEET_ID).worksheet(TOURS_SHEET)

# ===== Поиск =====
def _parse_ts(ts_val: str) -> datetime:
    try:
        return datetime.fromisoformat(str(ts_val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(days=365)

def _price_to_usd(amount, currency: str) -> Optional[float]:
    if amount in (None, ""):
        return None
    try:
        p = float(amount)
    except Exception:
        return None
    cur = (currency or "USD").upper()
    if cur == "USD":
        return p
    if cur in ("UZS", "SUM", "СУМ"):
        return p / UZS_PER_USD if UZS_PER_USD > 0 else None
    return p  # fallback

def search_tours(query: str, max_price_usd: Optional[float]) -> List[dict]:
    rows = ws.get_all_records()
    fresh_cut = datetime.now(timezone.utc) - timedelta(days=FRESH_DAYS)
    out: List[Tuple[float, dict]] = []
    q = (query or "").lower()
    for r in rows:
        ts = _parse_ts(r.get("ts_utc", ""))
        if ts < fresh_cut:
            continue
        hay = f"{r.get('destination_raw','')} {r.get('raw_text','')}".lower()
        if q not in hay:
            continue
        usd = _price_to_usd(r.get("price_amount"), r.get("price_currency"))
        if usd is None:
            continue
        if max_price_usd is not None and usd > max_price_usd:
            continue
        out.append((usd, r))
    out.sort(key=lambda x: x[0])
    return [r for _, r in out[:3]]

# ===== Bot =====
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "Привет! Я нахожу самые дешёвые туры из подключённых каналов ✈️\n"
        "Пример: /find дубай 600 — покажу топ-3 до $600.\n"
        "Команда: /find <куда> [макс_цена_usd]"
    )

@dp.message(Command("find"))
async def cmd_find(m: Message):
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 2:
        return await m.answer("Использование: /find <куда> [макс_цена_usd]\nНапр.: /find дубай 600")
    query = parts[1]
    max_price = None
    if len(parts) == 3:
        try:
            max_price = float(parts[2].replace(",", "."))
        except Exception:
            max_price = None

    hits = search_tours(query, max_price)
    if not hits:
        return await m.answer("Ничего не нашёл. Попробуй другое слово или увеличь бюджет.")

    chunks = []
    for r in hits:
        chunks.append(
            f"🌍 {r.get('destination_raw','?')}\n"
            f"📅 {r.get('dates_raw','')}\n"
            f"🏨 {r.get('hotel_raw','')}\n"
            f"💵 {r.get('price_amount','')} {r.get('price_currency','')}\n"
            f"🔗 {r.get('post_url','')}\n"
            "— — —"
        )
    await m.answer("\n".join(chunks))

async def main():
    print("Bot is running…")
    await dp.start_polling(bot, allowed_updates=["message"])

if __name__ == "__main__":
    asyncio.run(main())

