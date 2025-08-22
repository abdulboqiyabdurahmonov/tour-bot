from psycopg import connect
from psycopg.rows import dict_row
import logging
import os

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        # USERS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            is_premium BOOLEAN DEFAULT FALSE,
            premium_until TIMESTAMP,
            searches_today INT DEFAULT 0,
            last_search_date DATE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # REQUESTS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS requests (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            query TEXT,
            response TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # TOURS
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
            posted_at TIMESTAMP DEFAULT NOW()
        );
        """)

    logging.info("✅ Таблицы users, requests и tours готовы")
