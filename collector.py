# db_init.py  (или вставь в нужный файл вместо старого кода)

import os
import psycopg
from psycopg.rows import dict_row
from db_init import get_conn

# Берём строку подключения из переменной окружения Render/Heroku и т.п.
DATABASE_URL = os.environ["DATABASE_URL"]  # пример: postgres://USER:PASSWORD@HOST:PORT/DB

def get_conn():
    """
    Открывает подключение к PostgreSQL.
    row_factory=dict_row — удобно, если будешь читать строки как dict.
    sslmode='require' — для Render/внешних БД.
    """
    return psycopg.connect(DATABASE_URL, sslmode="require", row_factory=dict_row)

def init_db():
    """
    Создаёт таблицу tours, если её ещё нет.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tours (
                    id SERIAL PRIMARY KEY,
                    country     TEXT,
                    city        TEXT,
                    hotel       TEXT,
                    price       INTEGER,
                    dates       TEXT,
                    description TEXT
                )
            """)
        conn.commit()
    print("✅ Таблица tours готова")

def add_test_tours():
    """
    Добавляет несколько тестовых туров.
    Повторы сейчас не фильтруются (для простоты).
    """
    tours = [
        ("Турция", "Анталья", "Hotel Sun",       550, "20.08–30.08", "Море, all inclusive"),
        ("Египет", "Хургада", "Red Sea Resort",  480, "22.08–29.08", "Песчаный пляж, 4*"),
        ("ОАЭ",   "Дубай",   "Palm Hotel",       850, "25.08–01.09", "Город + пляж"),
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO tours (country, city, hotel, price, dates, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, tours)
        conn.commit()
    print("✅ Тестовые туры добавлены")

if __name__ == "__main__":
    # Запуск как скрипта: создаём таблицу и (опционально) заливаем тестовые данные
    init_db()
    if os.getenv("SEED_TOURS", "0") == "1":
        add_test_tours()

def get_all_tours():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tours")
            return cur.fetchall()

