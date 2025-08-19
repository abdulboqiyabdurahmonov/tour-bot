import os
import psycopg
from psycopg.rows import dict_row

DB_URL = os.environ["DATABASE_URL"]

def get_conn():
    return psycopg.connect(DB_URL)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        # сначала сносим старые таблицы
        cur.execute("DROP TABLE IF EXISTS tours CASCADE;")
        cur.execute("DROP TABLE IF EXISTS checkpoints CASCADE;")

        # создаём заново
        cur.execute("""
        CREATE TABLE tours (
            id SERIAL PRIMARY KEY,
            country TEXT,
            city TEXT,
            hotel TEXT,
            price INTEGER,
            currency TEXT,
            dates TEXT,
            description TEXT,
            source_chat TEXT NOT NULL,
            message_id BIGINT NOT NULL,
            posted_at TIMESTAMPTZ,
            source_url TEXT,
            UNIQUE(source_chat, message_id)
        );
        """)

        cur.execute("""
        CREATE TABLE checkpoints (
            chat TEXT PRIMARY KEY,
            last_id BIGINT
        );
        """)

        conn.commit()
        print("✅ Database initialized")
