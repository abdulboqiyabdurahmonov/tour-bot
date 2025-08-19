import os
import psycopg
from psycopg.rows import dict_row

# URL базы из переменной окружения (Render → Internal Database URL)
DB_URL = os.environ["DATABASE_URL"]

def get_conn():
    """
    Возвращает подключение к базе.
    """
    return psycopg.connect(DB_URL)

def init_db():
    """
    Создает таблицы, если их ещё нет.
    """
    with get_conn() as conn, conn.cursor() as cur:
        # Таблица туров
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tours (
            id SERIAL PRIMARY KEY,
            country TEXT,
            city TEXT,
            hotel TEXT,
            price INTEGER,
            currency TEXT,
            dates TEXT,
            description TEXT,
            source_chat TEXT,
            message_id BIGINT,
            posted_at TIMESTAMPTZ,
            source_url TEXT,
            UNIQUE(source_chat, message_id)
        )
        """)

        # Таблица чекпоинтов (для хранения последнего ID сообщений)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            chat TEXT PRIMARY KEY,
            last_id BIGINT
        )
        """)

        # Индексы для ускорения поиска
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_country_city ON tours(country, city)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_posted_at ON tours(posted_at DESC)")

        conn.commit()
