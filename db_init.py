import os
import psycopg
from psycopg.rows import dict_row

# --- ENV ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан в переменных окружения")


def get_conn():
    """
    Возвращает подключение к базе (autocommit=True)
    """
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
    """
    Создаёт таблицы, если их ещё нет.
    """
    with get_conn() as conn, conn.cursor() as cur:
        # таблица туров
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tours (
                id SERIAL PRIMARY KEY,
                country TEXT,
                city TEXT,
                hotel TEXT,
                price INT,
                dates TEXT,
                description TEXT,
                source_chat TEXT NOT NULL,
                message_id BIGINT NOT NULL,
                posted_at TIMESTAMPTZ NOT NULL,
                source_url TEXT,
                UNIQUE (source_chat, message_id)
            )
        """)

        # таблица для чекпоинтов (откуда collector возобновляет)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                chat TEXT PRIMARY KEY,
                last_id BIGINT
            )
        """)

        print("✅ Таблицы инициализированы")


if __name__ == "__main__":
    init_db()
