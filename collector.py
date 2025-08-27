import os
import re
import logging
import asyncio
from datetime import datetime
from io import BytesIO
from typing import Optional, Tuple

import httpx
from telethon.sessions import StringSession
from telethon import TelegramClient
from psycopg import connect

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ============ ENV ============
API_ID = int(os.getenv("TG_API_ID") or 0)
API_HASH = os.getenv("TG_API_HASH")
SESSION_B64 = os.getenv("TG_SESSION_B64")
CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "").split(",") if c.strip()]  # пример: @tour1,@tour2
DATABASE_URL = os.getenv("DATABASE_URL")

if not API_ID or not API_HASH or not SESSION_B64 or not CHANNELS or not DATABASE_URL:
    raise ValueError("❌ Проверь TG_API_ID, TG_API_HASH, TG_SESSION_B64, CHANNELS и DATABASE_URL в окружении")

# Максимальный размер файла для telegra.ph ~5MB
TELEGRAPH_UPLOAD = "https://telegra.ph/upload"
TELEGRAPH_MAX_BYTES = 5 * 1024 * 1024

# ============ БД ============
def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def save_tour(data: dict):
    """Сохраняем тур в PostgreSQL (апсертом дополняем пробелы, если была дубликат-запись)."""
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO tours
                  (country, city, hotel, price, currency, dates, description,
                   source_url, posted_at, message_id, source_chat, photo_url)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                  photo_url   = COALESCE(EXCLUDED.photo_url, tours.photo_url)
                RETURNING id;
                """,
                (
                    data.get("country"),
                    data.get("city"),
                    data.get("hotel"),
                    data.get("price"),
                    data.get("currency"),
                    data.get("dates"),
                    data.get("description"),
                    data.get("source_url"),
                    data.get("posted_at"),
                    data.get("message_id"),
                    data.get("source_chat"),
                    data.get("photo_url"),
                ),
            )
            inserted = cur.fetchone()
            if inserted:
                logging.info(
                    f"💾 Сохранил/обновил тур: {data.get('country')} | {data.get('city')} | "
                    f"{data.get('price')} {data.get('currency')} (id={inserted[0]})"
                )
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении тура: {e}")

# ============ ПАРСЕР ============

# --- цена и валюта ---
NBSP = "\u00A0"

def _normalize_number(s: str) -> Optional[float]:
    s = s.replace(" ", "").replace(NBSP, "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

PRICE_RE = re.compile(
    r"""(?ix)
    (?:от|всего|≈|~)?\s*
    (?:
        (?P<num1>\d[\d\s\u00A0\.,]{1,7})\s*(?P<cur1>USD|EUR|UZS|RUB|СУМ|сум|руб|\$|€)
      | (?P<cur2>USD|EUR|UZS|RUB|\$|€)\s*(?P<num2>\d[\d\s\u00A0\.,]{1,7})
    )
    """
)

def parse_price_currency(text: str) -> Tuple[Optional[float], Optional[str]]:
    m = PRICE_RE.search(text)
    if not m:
        return None, None
    if m.group("num1"):
        num = _normalize_number(m.group("num1"))
        cur = m.group("cur1")
    else:
        num = _normalize_number(m.group("num2"))
        cur = m.group("cur2")
    if not num:
        return None, None
    cu = (cur or "").strip().upper()
    if cu in {"$", "US$", "USD$"}: cu = "USD"
    elif cu in {"€", "EUR€"}:      cu = "EUR"
    elif cu in {"UZS", "СУМ", "СУМ.", "СУМЫ"}: cu = "UZS"
    elif cu in {"РУБ", "РУБ."}:    cu = "RUB"
    return num, cu

# --- даты (человекочитаемые и точные) ---
MONTH_WORDS = {
    r"январ[ьяею]": "01", r"феврал[ьяею]": "02", r"март[ае]?": "03",
    r"апрел[ьяею]": "04", r"ма[йяе]": "05", r"июн[ьяею]": "06",
    r"июл[ьяею]": "07", r"август[аеуы]?": "08", r"сентябр[ьяею]": "09",
    r"октябр[ьяею]": "10", r"ноябр[ьяею]": "11", r"декабр[ьяею]": "12",
}
DURATION_RE = re.compile(r"(?i)(?:(\d{1,2})\s*дн\w*)?(?:\s*[/,;•\.]?\s*)?(?:(\d{1,2})\s*ноч\w*)")

def parse_human_dates(text: str) -> Optional[str]:
    t = text.strip().lower()

    # точный диапазон: dd.mm(.yy|yyyy)?–dd.mm(.yy|yyyy)?
    m = re.search(
        r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\s?[–\-]\s?(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?",
        t,
    )
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        def _mk(d, m_, y):
            yv = int(y) if y else datetime.now().year
            if yv < 100:
                yv += 2000 if yv < 70 else 1900
            return f"{int(d):02d}.{int(m_):02d}.{yv:04d}"
        return f"{_mk(d1, m1, y1)}–{_mk(d2, m2, (y2 or y1))}"

    # месяц словами
    mon_txt = None
    for pat in MONTH_WORDS.keys():
        m = re.search(pat, t)
        if m:
            mon_txt = m.group(0).capitalize()
            break

    # длительность
    dmatch = DURATION_RE.search(t)
    days = dmatch.group(1) if dmatch else None
    nights = dmatch.group(2) if dmatch else None

    if mon_txt or days or nights:
        parts = []
        if mon_txt: parts.append(mon_txt)
        dur = []
        if days:   dur.append(f"{days}д")
        if nights: dur.append(f"{nights}н")
        if dur: parts.append(" / ".join(dur))
        return " · ".join(parts) if parts else None

    return None

# --- чистка текста/описания ---
def clean_text_basic(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    s = re.sub(r'[*_`]+', '', s)
    s = s.replace('|', ' ')
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

CONTACT_MARKERS = [
    "заброниров", "брониров", "звоните", "тел:", "телефон", "whatsapp", "вацап", "ватсап",
    "менеджер", "директ", "наш адрес", "колл-центр", "call center", "call-центр",
    "пишите", "свяжитесь", "контакт", "@", "+998", "+7", "+380", "+375", "+374", "+996", "+992", "+993", "+994"
]

def clean_description_block(text: str) -> str:
    """Берём верхнюю информативную часть, отсекаем блок с контактами и маркетингом."""
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]  # без пустых
    kept = []
    for ln in lines:
        low = ln.lower()
        if any(tok in low for tok in CONTACT_MARKERS):
            break
        kept.append(ln)
        if len(kept) >= 10:
            break
    out = "\n".join(kept)
    out = re.sub(r"[ \t]{2,}", " ", out)
    return out[:500]

# --- отель/бренд ---
HOTEL_LINE_RE = re.compile(
    r"(?im)^(?=.*?(отел[ья]|hotel|resort|palace|inn|spa|vinpearl|rixos|hilton|marriott|accor|barcelo|melia|iberostar))([^\n]{5,80})$"
)

def extract_hotel(text: str) -> Optional[str]:
    m = HOTEL_LINE_RE.search(text or "")
    if not m:
        return None
    line = m.group(2)
    line = re.sub(r"[•✅⭐️✨✳️❗️❕🔥💥👉🏨🏝️🌴🌊🌞🛫🛬🚖📍🧳]+", " ", line)
    line = re.sub(r"\s{2,}", " ", line).strip(" -—:·")
    line = re.sub(r"(?i)^любим(ая|ый)\s+сеть\s+отел[еий]+\s+", "", line)
    return line.strip()

# --- география ---
def guess_country(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    mapping = {
        "Нячанг": "Вьетнам",
        "Анталья": "Турция",
        "Пхукет": "Таиланд",
        "Паттайя": "Таиланд",
        "Самуи": "Таиланд",
        "Краби": "Таиланд",
        "Бангкок": "Таиланд",
        "Дубай": "ОАЭ",
        "Бали": "Индонезия",
        "Тбилиси": "Грузия",
    }
    return mapping.get(city, None)

# --- финальная сборка поста ---
def parse_post(text: str, link: str, msg_id: int, chat: str, posted_at: datetime, photo_url: Optional[str]):
    price, currency = parse_price_currency(text)
    dates = parse_human_dates(text)

    # город (сначала известные, потом fallback на первое "СловоСБольшойБуквы")
    city_match = re.search(r"(Бали|Дубай|Нячанг|Анталья|Пхукет|Тбилиси|Бангкок|Краби|Паттайя|Самуи)", text, re.I)
    city = city_match.group(1) if city_match else None
    if not city:
        m = re.search(r"\b([А-ЯЁ][а-яё]+)\b", text)
        city = m.group(1) if m else None

    hotel = extract_hotel(text)

    # описание аккуратно
    description = clean_description_block(text)

    return {
        "country": guess_country(city) if city else None,
        "city": city,
        "hotel": hotel,
        "price": price,
        "currency": currency,
        "dates": dates,
        "description": description,
        "source_url": link,
        "posted_at": posted_at.replace(tzinfo=None),
        "message_id": msg_id,
        "source_chat": chat,
        "photo_url": photo_url,
    }

# ============ ФОТО (загрузка в telegra.ph) ============

async def extract_photo_bytes(client: TelegramClient, msg) -> Optional[bytes]:
    try:
        # Есть ли фото/картинка
        is_photo = bool(getattr(msg, "photo", None))
        is_image_doc = bool(
            getattr(msg, "document", None) and
            getattr(msg.document, "mime_type", "") and
            msg.document.mime_type.startswith("image/")
        )
        if not (is_photo or is_image_doc):
            return None

        bio = BytesIO()
        await client.download_media(message=msg, file=bio)
        data = bio.getvalue()
        if not data:
            return None
        if len(data) > TELEGRAPH_MAX_BYTES:
            logging.info(f"📷 Фото {len(data)} bytes > 5MB — пропускаю для telegra.ph")
            return None
        return data
    except Exception as e:
        logging.warning(f"extract_photo_bytes error: {e}")
        return None

async def upload_to_telegraph(image_bytes: bytes) -> Optional[str]:
    try:
        files = {'file': ('image.jpg', image_bytes, 'image/jpeg')}
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(TELEGRAPH_UPLOAD, files=files)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data and "src" in data[0]:
                return "https://telegra.ph" + data[0]["src"]
            logging.warning(f"Unexpected telegraph response: {data}")
        else:
            logging.warning(f"Telegraph upload failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        logging.warning(f"upload_to_telegraph error: {e}")
    return None

async def photo_url_from_message(client: TelegramClient, msg) -> Optional[str]:
    img = await extract_photo_bytes(client, msg)
    if not img:
        return None
    return await upload_to_telegraph(img)

# ============ КОЛЛЕКТОР ============
async def collect_once(client: TelegramClient):
    for channel in CHANNELS:
        if not channel:
            continue
        logging.info(f"📥 Читаю канал: {channel}")
        async for msg in client.iter_messages(channel, limit=50):
            # Текст (подпись к фото тоже попадает в .text)
            if not msg.text:
                continue
            try:
                link = f"https://t.me/{channel.strip('@')}/{msg.id}"
                photo_url = await photo_url_from_message(client, msg)
                data = parse_post(
                    msg.text,
                    link,
                    msg.id,
                    channel,
                    msg.date,
                    photo_url,
                )
                save_tour(data)
            except Exception as e:
                logging.error(f"❌ Ошибка обработки сообщения {channel}/{msg.id}: {e}")

async def run_collector():
    client = TelegramClient(StringSession(SESSION_B64), API_ID, API_HASH)
    await client.start()
    logging.info("✅ Collector запущен")
    while True:
        try:
            await collect_once(client)
        except Exception as e:
            logging.error(f"❌ Ошибка в коллекторе: {e}")
        await asyncio.sleep(900)  # каждые 15 минут

if __name__ == "__main__":
    asyncio.run(run_collector())
