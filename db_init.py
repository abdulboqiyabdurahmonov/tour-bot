# db_init.py
import os, psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS channels (
  id SERIAL PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  title TEXT,
  added_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_messages (
  id BIGINT PRIMARY KEY,               -- message id
  channel TEXT NOT NULL,
  posted_at TIMESTAMPTZ,
  text TEXT,
  full JSONB,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

-- под готовые карточки туров (парсер заполнил)
CREATE TABLE IF NOT EXISTS tours (
  id BIGSERIAL PRIMARY KEY,
  source_channel TEXT,
  message_id BIGINT,
  title TEXT,
  price NUMERIC,
  currency TEXT,
  dates TEXT,
  nights INT,
  people INT,
  hotel TEXT,
  city TEXT,
  country TEXT,
  link TEXT,
  raw JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
"""

def main():
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    print("✅ DB schema ready")

if __name__ == "__main__":
    main()
