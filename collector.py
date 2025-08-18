# collector.py
import os
import re
import json
import base64
import asyncio
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

import gspread
from google.oauth2.service_account import Credentials

# ===== ENV =====
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
SESSION_NAME = os.getenv("SESSION_NAME", "/data/tg_session")  # на Render ставь /data/tg_session

CHANNELS = [c.strip() for c in (os.getenv("CHANNELS") or "").split(",") if c.strip()]

SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
TOURS_SHEET = os.getenv("TOURS_SHEET", "Tours")

if not API_ID or not API_HASH or not PHONE_NUMBER:
    raise RuntimeError("API_ID / API_HASH / PHONE_NUMBER обязательны (.env).")
if not SHEETS_SPREADSHEET_ID:
    raise RuntimeError("SHEETS_SPREADSHEET_ID обязателен.")

# --- восстановление Telethon-сессии из base64 (для первого деплоя без интерактива) ---
sess_path = SESSION_NAME
sess_file = sess_path if sess_path.endswith(".session") else sess_path + ".session"
sess_b64 = os.getenv("SESSION_B64")
if sess_b64 and not os.path.exists(sess_file):
    os.makedirs(os.path.dirname(sess_path), exist_ok=True)
    with open(sess_file, "wb") as f:
        f.write(base64.b64decode(sess_b64))
    print("✔ Restored Telethon session from SESSION_B64")

# ===== Google Sheets =====
def _gspread_client():
    raw = os.getenv("GOOGLE_CREDENTIALS")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS обязателен (JSON одной строкой или base64).")
    try:
        creds_text = base64.b64decode(raw).decode("utf-8") if not raw.lstrip().startswith("{") else raw
        info = json.loads(creds_text)
        if "private_key" in info and "\\n" in info["private_key"]:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
    except Exception as e:
        raise RuntimeError(f"Неверный GOOGLE_CREDENTIALS: {e}")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

gc = _gspread_client()
sh = gc.open_by_key(SHEETS_SPREADSHEET_ID)
try:
    ws = sh.worksheet(TOURS_SHEET)
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TOURS_SHEET, rows=200000, cols=10)
    ws.append_row(
        ["ts_utc","channel","message_id","post_url","price_amount","price_currency","destination_raw","dates_raw","hotel_raw","raw_text"],
        value_input_option="RAW"
    )

# ===== Парсер полей из постов =====
PRICE_RE = re.compile(r'(?P<cur>\$|usd|доллар(?:ов)?|sum|сум|uzs)\s*?([:=]?\s*)?(?P<val>\d{2,7}(?:[.,]\d{3})*(?:[.,]\d{1,2})?)', re.I)
ALT_PRICE_RE = re.compile(r'(?P<val>\d{2,7}(?:[.,]\d{3})*)(\s*)(?P<cur>usd|uzs|сум|sum|\$)', re.I)
DATE_RE = re.compile(r'(\d{1,2})\s*[.-–—]\s*(\d{1,2})\s*(?:[./]|[ \t-]*(янв|фев|мар|апр|май|июн|июл|авг|сен|oct|ноя|дек|yan|fev|mart|apr|may|iyun|iyul|avg|sen|okt|noy|dek))?', re.I)
HOTEL_RE = re.compile(r'(hotel|отель)\s+([^\n,|]+)', re.I)

POPULAR = [
    "Дубай","Анталия","Бодрум","Стамбул","Алания","Абхазия","Самарканд","Бухара","Бали","Хайнань","Тбилиси","Батуми","Шарм","Хургада",
    "Dubai","Istanbul","Antalya","Bodrum","Alanya","Abkhazia","Hainan","Tbilisi","Batumi","Sharm","Hurghada"
]

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def _norm_currency(tok: str) -> str:
    t = (tok or "").lower()
    if "$" in t or "usd" in t or "доллар" in t: return "USD"
    if "sum" in t or "сум" in t or "uzs" in t: return "UZS"
    return "USD"

def extract_price(text: str):
    m = PRICE_RE.search(text) or ALT_PRICE_RE.search(text)
    if not m: return None, None
    cur = _norm_currency(m.group("cur"))
    val = (m.group("val") or "").replace(" ", "").replace(",", "").replace("’","").replace("‘","").replace("`","")
    try:
        return float(val), cur
    except Exception:
        try:
            return float(val.replace(".", "")), cur
        except Exception:
            return None, cur

def extract_dates(text: str) -> str:
    m = DATE_RE.search(text)
    return m.group(0) if m else ""

def extract_hotel(text: str) -> str:
    m = HOTEL_RE.search(text)
    return m.group(0) if m else ""

def extract_destination(text: str) -> str:
    for c in POPULAR:
        if re.search(rf'\b{re.escape(c)}\b', text, re.I):
            return c
    return text.strip().splitlines()[0][:60] if text.strip() else ""

def make_url(username: Optional[str], msg_id: int) -> str:
    return f"https://t.me/{username}/{msg_id}" if username else ""

def append_row(channel_title: str, username: Optional[str], msg_id: int, text: str):
    price, cur = extract_price(text)
    dest = extract_destination(text)
    dates = extract_dates(text)
    hotel = extract_hotel(text)
    url = make_url(username, msg_id)
    ws.append_row([
        _now_utc(),
        channel_title or (username or ""),
        str(msg_id),
        url,
        price or "",
        cur or "",
        dest or "",
        dates or "",
        hotel or "",
        text[:5000],
    ], value_input_option="RAW")

# ===== Telethon =====
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNELS if CHANNELS else None))
async def on_new_post(evt: events.NewMessage.Event):
    if not evt.is_channel:
        return
    text = (evt.message.message or "").strip()
    if not text:
        return
    chat = await evt.get_chat()
    try:
        append_row(
            channel_title=getattr(chat, "title", "") or "",
            username=getattr(chat, "username", None),
            msg_id=evt.message.id,
            text=text
        )
        print("✔ saved", getattr(chat, "username", ""), evt.message.id)
    except Exception as e:
        print("append error:", e)

async def backfill(limit_per_channel: int = 100):
    """Подкачать историю по указанным каналам при старте."""
    if not CHANNELS:
        print("⚠️ CHANNELS пуст — backfill пропущен, слушаем новые посты.")
        return
    for ch in CHANNELS:
        try:
            async for msg in client.iter_messages(ch, limit=limit_per_channel):
                text = (msg.text or msg.message or "").strip() if msg else ""
                if not text:
                    continue
                chat = await client.get_entity(ch)
                append_row(
                    channel_title=getattr(chat, "title", "") or "",
                    username=getattr(chat, "username", None),
                    msg_id=msg.id,
                    text=text
                )
            print(f"Backfilled {ch}")
        except FloodWaitError as e:
            print(f"FloodWait: sleeping {e.seconds}s for {ch}")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"Backfill error {ch}:", e)

async def main():
    print("Connecting…")
    await client.start(phone=PHONE_NUMBER)  # при первом запуске локально спросит код, на Render лучше через SESSION_B64
    print("Logged in.")
    await backfill(limit_per_channel=100)
    print("Listening new posts…")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())

