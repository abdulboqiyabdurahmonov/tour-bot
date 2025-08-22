import os
import logging
import asyncio
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# ================= ЛОГИ =================
logging.basicConfig(level=logging.INFO)

# ================= ENV =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY не найден в переменных окружения!")

# ================= БОТ =================
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)
dp = Dispatcher()
app = FastAPI()


# ================= GPT =================
async def ask_gpt(prompt: str, premium: bool = False) -> str:
    """
    GPT-ответ строго в рамках тематики путешествий.
    Бесплатный → без источника.
    Премиум → со ссылкой на источник.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": (
                            "Ты — AI-консультант по путешествиям из экосистемы TripleA. "
                            "Отвечай красиво, дружелюбно, но информативно. "
                            "Советы, туры, отели, лайфхаки, погода, цены, культура. "
                            "Не уходи от тематики путешествий."
                        )},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.7,
                },
            )

        data = response.json()
        answer = data["choices"][0]["message"]["content"].strip()

        # Доп. логика Free / Premium
        if premium:
            answer += "\n\n🔗 *Источник тура:* [Перейти](https://t.me/triplea_channel)"
        else:
            answer += "\n\n✨ Хочешь видеть прямые ссылки на источники туров? Подключи Premium доступ TripleA."

        # aiogram может ругаться на очень длинные сообщения
        MAX_LEN = 3800
        if len(answer) > MAX_LEN:
            parts = [answer[i:i+MAX_LEN] for i in range(0, len(answer), MAX_LEN)]
            return parts
        return [answer]

    except Exception as e:
        logging.error(f"Ошибка GPT: {e}")
        return ["⚠️ Упс! Произошла ошибка при обращении к AI. Попробуйте ещё раз."]


# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    intro = (
        "🌍 Привет! Я — *TripleA Travel Bot* ✈️\n\n"
        "Я помогу тебе найти *актуальные туры, советы по странам, лайфхаки путешественников*.\n\n"
        "💡 Просто напиши, что тебя интересует:\n"
        "• Тур в Турцию в сентябре\n"
        "• Какая погода в Бали в октябре\n"
        "• Лучшие отели для двоих в Дубае\n\n"
        "✨ Доступно: вся информация по турам\n"
        "🔒 Premium: прямая ссылка на источник тура\n\n"
        "Что тебе подсказать? 😊"
    )
    await message.answer(intro)


@dp.message(F.text)
async def handle_message(message: Message):
    user_text = message.text.strip()

    # Логика Premium (например, VIP id-шники)
    premium_users = {123456789, 987654321}
    is_premium = message.from_user.id in premium_users

    replies = await ask_gpt(user_text, premium=is_premium)
    for part in replies:
        await message.answer(part)


# ================= WEBHOOK =================
@app.get("/")
async def root():
    """Эндпоинт для health-check Render"""
    return {"status": "ok", "message": "TripleA Travel Bot is running!"}


@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        await asyncio.sleep(0)  # не блокируем event loop
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    return JSONResponse({"status": "ok"})


@app.on_event("startup")
async def on_startup():
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}")
    else:
        logging.warning("WEBHOOK_URL не указан — бот не получит апдейты.")


@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()
