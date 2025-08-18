import os
import asyncio
from telethon import TelegramClient, events

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNELS = [c.strip() for c in (os.getenv("CHANNELS", "")).split(",") if c.strip()]

# берем локальный файл сессии, который ты закоммитил: tour_session.session
client = TelegramClient("tour_session", API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNELS if CHANNELS else None))
async def handler(event):
    try:
        text = event.raw_text or ""
        chat = (await event.get_chat())
        title = getattr(chat, "title", str(chat))
        print(f"[{title}] {text[:200].replace('\\n',' ')}")
        # тут дальше парсишь цены/направления и пишешь в таблицу/кэш
    except Exception as e:
        print("handler error:", e)

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        print("❌ Сессия не авторизована (tour_session.session не подходит). Останов.")
        return
    print("✅ Collector online. Listening…")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
