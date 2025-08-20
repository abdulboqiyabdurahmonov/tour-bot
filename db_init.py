# db_init.py
import os
from psycopg import connect

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_premium BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tours (
            id SERIAL PRIMARY KEY,
            country TEXT,
            city TEXT,
            hotel TEXT,
            price INT,
            currency TEXT,
            dates TEXT,
            description TEXT,
            source_chat TEXT,
            message_id BIGINT,
            posted_at TIMESTAMP,
            source_url TEXT,
            UNIQUE (source_chat, message_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            chat TEXT PRIMARY KEY,
            last_id BIGINT
        );
        """)
        conn.commit()
