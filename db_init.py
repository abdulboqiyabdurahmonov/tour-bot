from psycopg import connect
from psycopg.rows import dict_row
import logging
import os

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

import os
import logging
from psycopg import connect
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    """Создание таблиц, если их нет"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                full_name TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                query TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
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
    logging.info("📦 База данных инициализирована")

def save_user(user):
    """Сохраняем пользователя в БД (id + ФИО)"""
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, full_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user.id, full_name))

        """)

    logging.info("✅ Таблицы users, requests и tours готовы")
