def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        # users
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_premium BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        # проверим и добавим недостающие поля
        try:
            cur.execute("ALTER TABLE users ADD COLUMN premium_until TIMESTAMP;")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE users ADD COLUMN searches_today INT DEFAULT 0;")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE users ADD COLUMN last_search_date DATE;")
        except Exception:
            pass

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
            source_url TEXT,
            posted_at TIMESTAMP DEFAULT NOW()
        );
        """)
