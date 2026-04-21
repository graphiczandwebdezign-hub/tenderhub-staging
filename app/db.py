import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    dsn = os.getenv("DB_DSN")
    if not dsn:
        raise RuntimeError("DB_DSN is not set")
    return psycopg2.connect(dsn)


def fetch_all(query, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


def fetch_one(query, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchone()
