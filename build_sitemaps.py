import os
import psycopg2
from xml.sax.saxutils import escape

import os
from dotenv import load_dotenv
load_dotenv()

DB_DSN = os.getenv("DB_DSN")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:5000")


def write_urlset(path, urls):
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        for loc, lastmod in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{escape(loc)}</loc>\n")

            if lastmod:
                f.write(f"    <lastmod>{lastmod.strftime('%Y-%m-%d')}</lastmod>\n")

            f.write("  </url>\n")

        f.write('</urlset>\n')


def fetch_tenders():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    cur.execute("""
        SELECT slug, updated_at
        FROM tenders
        ORDER BY updated_at DESC
        LIMIT 1000
    """)

    rows = cur.fetchall()
    conn.close()

    urls = []
    for slug, updated_at in rows:
        url = f"{BASE_URL}/tender/{slug}"
        urls.append((url, updated_at))

    return urls


def main():
    os.makedirs("sitemaps", exist_ok=True)

    tender_urls = fetch_tenders()

    write_urlset("sitemaps/tenders.xml", tender_urls)

    print("✅ Sitemap generated: sitemaps/tenders.xml")


if __name__ == "__main__":
    main()