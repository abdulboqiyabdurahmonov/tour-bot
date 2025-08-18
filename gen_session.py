from telethon import TelegramClient
import base64

API_ID = int(input("API_ID: "))
API_HASH = input("API_HASH: ")
PHONE = input("PHONE: ")

client = TelegramClient("tg_session", API_ID, API_HASH)

async def main():
    await client.start(PHONE)
    print("✔ Logged in successfully")

    # Прочитаем сессию как байты
    with open("tg_session.session", "rb") as f:
        raw = f.read()
    b64 = base64.b64encode(raw).decode()
    with open("session.b64", "w") as f:
        f.write(b64)
    print("✔ Saved to session.b64")

with client:
    client.loop.run_until_complete(main())
