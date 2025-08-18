# db_init.py
import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]  # в Render = строка из PostgreSQL add-on

def get_conn():
    # единая точка подключения
    return psycopg.connect(DATABASE_URL, sslmode="require")

def init_db():
    # создаём таблицу, если её нет
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

def seed_test():
    # тестовые данные (можно выключить переменной SEED_TEST_TOURS=0)
    tours = [
        ("Турция", "Анталья", "Hotel Sun", 550, "20.08–30.08", "Море, all inclusive"),
        ("Египет", "Хургада", "Red Sea Resort", 480, "22.08–29.08", "Песчаный пляж, 4*"),
        ("ОАЭ", "Дубай", "Palm Hotel", 850, "25.08–01.09", "Город + пляж"),
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO tours (country, city, hotel, price, dates, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, tours)
            conn.commit()

if __name__ == "__main__":
    init_db()
    if os.getenv("SEED_TEST_TOURS", "1") == "1":
        seed_test()
    print("✅ Таблица tours готова")

