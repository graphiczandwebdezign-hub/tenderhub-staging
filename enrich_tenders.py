import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_DSN = os.environ["DB_DSN"]
SITE_URL = os.getenv("SITE_URL", "http://127.0.0.1:5000").rstrip("/")


def main():
    conn = psycopg2.connect(DB_DSN)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tenders
                SET closing_soon = CASE
                    WHEN closing_at IS NOT NULL
                     AND closing_at >= NOW()
                     AND closing_at <= NOW() + INTERVAL '7 days'
                    THEN TRUE ELSE FALSE END,
                    searchable_text = concat_ws(' ',
                        title,
                        coalesce(reference_number, ''),
                        coalesce(organ_of_state, ''),
                        coalesce(category, ''),
                        coalesce(province, ''),
                        coalesce(description, '')
                    ),
                    canonical_url = %s || '/tender/' || slug,
                    meta_title = left(concat(title, ' | ', coalesce(organ_of_state, 'Tender'), ' | TenderHub SA'), 255),
                    meta_description = left(
                        concat(
                            'View tender details for ', title, '. ',
                            'Published by ', coalesce(organ_of_state, 'an organ of state'), '. ',
                            'Closing date: ', coalesce(to_char(closing_at, 'DD Mon YYYY'), 'TBC'), '.'
                        ), 160
                    ),
                    seo_noindex = CASE WHEN is_active = FALSE AND closing_at < NOW() - INTERVAL '180 days' THEN TRUE ELSE FALSE END,
                    updated_at = NOW()
                """,
                (SITE_URL,),
            )
            print(f"Enriched {cur.rowcount} tenders")

            cur.execute("DELETE FROM tender_facets")
            cur.execute(
                """
                INSERT INTO tender_facets (facet_type, facet_key, facet_label, item_count, updated_at)
                SELECT 'province', province_slug, province, COUNT(*), NOW()
                FROM tenders
                WHERE is_active = TRUE AND province_slug IS NOT NULL
                GROUP BY province_slug, province
                """
            )
            cur.execute(
                """
                INSERT INTO tender_facets (facet_type, facet_key, facet_label, item_count, updated_at)
                SELECT 'category', category_slug, category, COUNT(*), NOW()
                FROM tenders
                WHERE is_active = TRUE AND category_slug IS NOT NULL
                GROUP BY category_slug, category
                """
            )
    conn.close()


if __name__ == "__main__":
    main()
