import os
import logging
from psycopg import connect
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    """Создание таблиц/индексов, если их нет"""
    with get_conn() as conn, conn.cursor() as cur:
        # Пользователи
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                full_name TEXT
            );
        """)

        # Логи запросов к GPT
        cur.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                query TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Туры (+ photo_url для карточек)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tours (
                id SERIAL PRIMARY KEY,
                country TEXT,
                city TEXT,
                hotel TEXT,
                price NUMERIC,
                currency TEXT,
                dates TEXT,
                description TEXT,
                source_chat TEXT,
                message_id BIGINT,
                source_url TEXT,
                photo_url TEXT,
                posted_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(message_id, source_chat)
            );
        """)

        # Избранное
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT,
                tour_id INT REFERENCES tours(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY(user_id, tour_id)
            );
        """)

        # Лиды (заявки)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                tour_id INT REFERENCES tours(id) ON DELETE SET NULL,
                phone TEXT,
                note TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Индексы для скорости поиска
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_posted_at ON tours (posted_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_country ON tours (LOWER(country));")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_city ON tours (LOWER(city));")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_hotel ON tours (LOWER(hotel));")

    logging.info("📦 База данных инициализирована")

def save_user(user):
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, full_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user.id, full_name))

def save_request(user_id: int, query: str, response: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO requests (user_id, query, response)
            VALUES (%s, %s, %s);
        """, (user_id, query, response))

def search_tours(query: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM tours
            WHERE (country ILIKE %s OR city ILIKE %s OR hotel ILIKE %s)
            ORDER BY posted_at DESC
            LIMIT 5;
        """, (f"%{query}%", f"%{query}%", f"%{query}%"))
        return cur.fetchall()
