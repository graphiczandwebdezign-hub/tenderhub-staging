import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_DSN = os.environ["DB_DSN"]

def main():
    schema_path = Path("schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError("schema.sql not found")

    sql = schema_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Database schema initialized successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()