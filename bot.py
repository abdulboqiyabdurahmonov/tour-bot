import os
from telethon import TelegramClient, events

# 🔑 Эти данные берёшь с https://my.telegram.org
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH"))

# 👇 Здесь Telethon создаст файл tour.session автоматически при первом запуске
client = TelegramClient("tour", API_ID, API_HASH)

@client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    await event.respond("Привет! Я тур-бот. Всё работает ✅")

print("Бот запускается...")
client.start()   # тут он спросит код только первый раз
client.run_until_disconnected()
