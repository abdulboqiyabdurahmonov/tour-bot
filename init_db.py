import os
import psycopg

DATABASE_URL = os.getenv("DATABASE_URL")

schema = """
CREATE TABLE IF NOT EXISTS tours (
    id SERIAL PRIMARY KEY,
    country TEXT NOT NULL,
    city TEXT,
    hotel TEXT,
    price NUMERIC,
    dates TEXT,
    source_channel TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute(schema)
        conn.commit()

print("✅ Таблица tours успешно создана")
