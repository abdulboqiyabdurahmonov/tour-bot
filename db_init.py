import os
import psycopg
from psycopg.rows import dict_row

def main():
    url = os.getenv("DATABASE_URL")
    conn = psycopg.connect(url, row_factory=dict_row)  # ✅ psycopg3
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS leads (
        id SERIAL PRIMARY KEY,
        full_name TEXT NOT NULL,
        phone TEXT NOT NULL,
        company TEXT,
        tariff TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    cur.close()
    conn.close()
