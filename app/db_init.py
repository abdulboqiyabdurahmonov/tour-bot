def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        # 1. создаём таблицу, если её нет
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_premium BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # 2. список недостающих колонок
        columns = [
            ("username", "TEXT"),
            ("first_name", "TEXT"),
            ("last_name", "TEXT"),
            ("full_name", "TEXT"),  # 👈 вот её не хватало
            ("premium_until", "TIMESTAMP"),
            ("searches_today", "INT DEFAULT 0"),
            ("last_search_date", "DATE")
        ]

        # 3. проверяем и добавляем недостающие поля
        for name, col_type in columns:
            cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'users'
                      AND column_name = '{name}'
                ) THEN
                    ALTER TABLE users ADD COLUMN {name} {col_type};
                END IF;
            END$$;
            """)

        conn.commit()
