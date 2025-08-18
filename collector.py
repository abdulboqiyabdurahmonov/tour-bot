import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

def init_db():
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tours (
        id SERIAL PRIMARY KEY,
        country TEXT,
        city TEXT,
        hotel TEXT,
        price INTEGER,
        dates TEXT,
        description TEXT
    )
    """)
    conn.commit()
    cur.close()
    conn.close()

def add_test_tours():
    tours = [
        ("Турция", "Анталья", "Hotel Sun", 550, "20.08–30.08", "Море, all inclusive"),
        ("Египет", "Хургада", "Red Sea Resort", 480, "22.08–29.08", "Песчаный пляж, 4*"),
        ("ОАЭ", "Дубай", "Palm Hotel", 850, "25.08–01.09", "Город + пляж"),
    ]
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO tours (country, city, hotel, price, dates, description)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, tours)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Тестовые туры добавлены!")

if __name__ == "__main__":
    init_db()
    add_test_tours()
