import os
import psycopg

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg.connect(DB_URL)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
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
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            chat TEXT PRIMARY KEY,
            last_id BIGINT
        );
        """)
        conn.commit()
