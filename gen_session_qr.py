# gen_session_qr.py
import asyncio, base64
from telethon import TelegramClient

API_ID = int(input("API_ID: ").strip())
API_HASH = input("API_HASH: ").strip()

async def main():
    client = TelegramClient("tour_session", API_ID, API_HASH)
    await client.connect()

    qr = await client.qr_login()
    print("\n============= ССЫЛКА ДЛЯ QR =============")
    print(qr.url)
    print("=========================================\n")
    print("Открой Telegram на телефоне → Настройки → Устройства → Подключить устройство → Сканируй QR.")

    await qr.wait()        # ждём, пока подтвердишь вход на телефоне
    print("✅ Вход выполнен")

    # сохраняем бинарную сессию -> base64
    with open("tour_session.session", "rb") as f:
        raw = f.read()
    b64 = base64.b64encode(raw).decode("utf-8")
    with open("session.b64", "w") as f:
        f.write(b64)
    print("✅ Saved to session.b64")

    await client.disconnect()

asyncio.run(main())
