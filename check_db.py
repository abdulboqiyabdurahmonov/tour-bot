import os
import logging
from psycopg import connect
from psycopg.rows import dict_row

# ЛОГИ
logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не найден в переменных окружения!")

def check_db():
    try:
        with connect(DATABASE_URL, autocommit=True, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = 'tours'
                    ORDER BY ordinal_position;
                """)
                rows = cur.fetchall()
                logging.info("✅ Подключение успешно. Таблица tours имеет колонки:")
                for r in rows:
                    logging.info(f" - {r['column_name']} ({r['data_type']})")
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке БД: {e}")

if __name__ == "__main__":
    check_db()
