# handlers/settings.py
from aiogram import Router, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

router = Router(name="settings")

LANGS = {"ru": "🇷🇺 Русский", "uz": "🇺🇿 O‘zbekcha", "en": "🇬🇧 English"}

def lang_keyboard(current: str | None = None) -> InlineKeyboardMarkup:
    rows = []
    for code, title in LANGS.items():
        mark = " ✅" if code == current else ""
        rows.append([InlineKeyboardButton(text=title + mark, callback_data=f"lang:{code}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# --- Заглушки: потом подключим БД
def get_user_lang(user_id: int) -> str | None: ...
def save_user_lang(user_id: int, code: str) -> None: ...

async def entry_settings(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌐 Выбрать язык", callback_data="open:lang")],
    ])
    await message.answer("⚙️ Настройки", reply_markup=kb)

@router.callback_query(F.data == "open:lang")
async def open_lang(cb: CallbackQuery):
    current = get_user_lang(cb.from_user.id)
    await cb.message.edit_text("Выберите язык:", reply_markup=lang_keyboard(current))
    await cb.answer()

@router.callback_query(F.data.startswith("lang:"))
async def set_lang(cb: CallbackQuery):
    code = cb.data.split(":")[1]
    save_user_lang(cb.from_user.id, code)
    await cb.answer("Язык сохранён ✅", show_alert=True)
    await cb.message.edit_reply_markup(reply_markup=lang_keyboard(code))

@router.callback_query(F.data == "back:settings")
async def back_settings(cb: CallbackQuery):
    await entry_settings(cb.message)
    await cb.answer()
