# db_init.py

import os
import logging
from psycopg import connect
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    """Создание таблиц/индексов и лёгкие миграции."""
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

        # Туры (включая photo_url)
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

        # --- миграции внутри функции, а не на модульном уровне! ---
        cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS photo_url TEXT;")
        cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS stable_key TEXT;")
        cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS board TEXT;")
        cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS includes TEXT;")

        # Индексы
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_posted_at ON tours (posted_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_country ON tours (LOWER(country));")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_city ON tours (LOWER(city));")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_hotel ON tours (LOWER(hotel));")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_tours_stable_key ON tours (stable_key);")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_tours_source_msg ON tours (source_chat, message_id);")

    logging.info("📦 База данных инициализирована")

# Остальные функции (save_user, save_request, search_tours, get_config, set_config) — без изменений

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

def get_config(key: str, default: str | None = None) -> str | None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT val FROM app_config WHERE key=%s;", (key,))
        row = cur.fetchone()
        return row["val"] if row else default

def set_config(key: str, val: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO app_config(key, val) VALUES (%s, %s)
            ON CONFLICT(key) DO UPDATE SET val=EXCLUDED.val;
        """, (key, val))

# db_init.py — добавь в init_db(), рядом с остальными ALTER-ами:
cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS board TEXT;")
cur.execute("ALTER TABLE tours ADD COLUMN IF NOT EXISTS includes TEXT;")

# (по желанию индексы)
# cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_board ON tours ((LOWER(board)));")

