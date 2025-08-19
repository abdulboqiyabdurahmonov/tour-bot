import os
import psycopg
import logging

logger = logging.getLogger("db_init")

# ---------- ENV ----------
DB_URL = os.getenv("DATABASE_URL")  # формат: postgres://user:pass@host:port/dbname

# ---------- connector ----------
def get_conn():
    """
    Создаём новое подключение к базе (используй with get_conn() as conn).
    """
    return psycopg.connect(DB_URL, autocommit=False)

# ---------- init ----------
def init_db():
    """
    Создаём таблицы, если их ещё нет.
    """
    with get_conn() as conn, conn.cursor() as cur:
        # таблица туров
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tours (
                country     text,
                city        text,
                hotel       text,
                price       int,
                currency    text,
                dates       text,
                description text,
                source_chat text NOT NULL,
                message_id  bigint NOT NULL,
                posted_at   timestamptz,
                source_url  text,
                PRIMARY KEY (source_chat, message_id)
            )
        """)
        # чекпоинты
        cur.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                chat    text PRIMARY KEY,
                last_id bigint
            )
        """)
        conn.commit()
        logger.info("✅ DB initialized")
