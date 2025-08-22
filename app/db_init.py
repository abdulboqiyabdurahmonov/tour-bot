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
        
        # 2. проверяем и добавляем нужные поля
        columns = [
            ("premium_until", "TIMESTAMP"),
            ("searches_today", "INT DEFAULT 0"),
            ("last_search_date", "DATE")
        ]

        for name, col_type in columns:
            try:
                cur.execute(f"ALTER TABLE users ADD COLUMN {name} {col_type};")
            except Exception:
                # колонка уже есть — игнорируем ошибку
                pass
