# db_init.py — расширено таблицами orders/subscriptions/payment_transactions

import os
import logging
from psycopg import connect
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        # users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                full_name TEXT
            );
        """)

        # requests (GPT)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                query TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # tours
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
                stable_key TEXT,
                board TEXT,
                includes TEXT,
                UNIQUE(message_id, source_chat)
            );
        """)

        # favorites
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT,
                tour_id INT REFERENCES tours(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY(user_id, tour_id)
            );
        """)

        # leads
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

        # app_config
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                val TEXT
            );
        """)

        # ====== PAYMENTS ======
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                provider TEXT NOT NULL,              -- 'click' | 'payme'
                plan_code TEXT NOT NULL,
                amount BIGINT NOT NULL,
                currency TEXT NOT NULL,
                kind TEXT NOT NULL,                  -- 'oneoff' | 'recurring'
                status TEXT NOT NULL DEFAULT 'pending',  -- pending|paid|failed|canceled
                provider_trx_id TEXT,
                paid_at TIMESTAMP,
                raw JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT PRIMARY KEY,
                plan_code TEXT NOT NULL,
                provider TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',     -- active|past_due|canceled
                current_period_end TIMESTAMP NOT NULL,
                payment_token TEXT,                        -- для рекуррента (если используете vault)
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS payment_transactions (
                id BIGSERIAL PRIMARY KEY,
                order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                status TEXT NOT NULL,         -- paid|failed|callback|...
                payload JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tours_posted_at ON tours (posted_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_end ON subscriptions (current_period_end);")

    logging.info("📦 База данных инициализирована/мигрирована")

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

if __name__ == "__main__":
    init_db()
