import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_DSN = os.environ["DB_DSN"]


def main():
    conn = psycopg2.connect(DB_DSN)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tenders
                SET status = 'closed',
                    is_active = FALSE,
                    closing_soon = FALSE,
                    updated_at = NOW()
                WHERE is_active = TRUE
                  AND closing_at IS NOT NULL
                  AND closing_at < NOW()
                """
            )
            print(f"Closed {cur.rowcount} expired tenders")
    conn.close()


if __name__ == "__main__":
    main()
