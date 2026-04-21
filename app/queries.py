from app.db import fetch_all, fetch_one, get_conn


def get_stats():
    return fetch_one(
        '''
        SELECT
          COUNT(*) AS active_tenders_count,
          COUNT(DISTINCT province_slug) FILTER (WHERE province_slug IS NOT NULL) AS provinces_count,
          COUNT(DISTINCT category_slug) FILTER (WHERE category_slug IS NOT NULL) AS categories_count
        FROM tenders
        '''
    )


def get_latest_tenders(limit=20):
    return fetch_all(
        '''
        SELECT
            id,
            title,
            slug,
            summary,
            organ_of_state,
            category,
            province,
            esubmission,
            published_at,
            closing_at,
            status,
            is_active
        FROM tenders
        ORDER BY
            is_active DESC,
            COALESCE(closing_at, published_at, created_at) DESC
        LIMIT %s
        ''',
        (limit,),
    )


def get_closing_soon(limit=8):
    return fetch_all(
        '''
        SELECT
            id,
            title,
            slug,
            summary,
            organ_of_state,
            category,
            province,
            closing_at,
            status,
            is_active
        FROM tenders
        ORDER BY
            is_active DESC,
            COALESCE(closing_at, published_at, created_at) DESC
        LIMIT %s
        ''',
        (limit,),
    )


def get_facet_list(facet_type, limit=12):
    return fetch_all(
        '''
        SELECT facet_key, facet_label, item_count
        FROM tender_facets
        WHERE facet_type = %s
        ORDER BY item_count DESC, facet_label ASC
        LIMIT %s
        ''',
        (facet_type, limit),
    )


def search_tenders(q=None, province=None, category=None, active_only=True, limit=100):
    clauses = []
    params = []
    if active_only:
        clauses.append("is_active = TRUE")
    if q:
        clauses.append("(searchable_text ILIKE %s OR title ILIKE %s OR coalesce(reference_number,'') ILIKE %s)")
        like = f"%{q}%"
        params += [like, like, like]
    if province:
        clauses.append("province_slug = %s")
        params.append(province)
    if category:
        clauses.append("category_slug = %s")
        params.append(category)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    return fetch_all(
        f'''
        SELECT id, title, slug, summary, organ_of_state, category, province, esubmission,
               published_at, closing_at, reference_number, status, is_active
        FROM tenders
        {where}
        ORDER BY COALESCE(closing_at, published_at, created_at) ASC NULLS LAST
        LIMIT %s
        ''',
        params,
    )


def get_tender_by_slug(slug):
    return fetch_one("SELECT * FROM tenders WHERE slug = %s LIMIT 1", (slug,))


def get_documents(tender_id):
    return fetch_all(
        "SELECT url, title, mime_type, file_name FROM tender_documents WHERE tender_id = %s ORDER BY id ASC",
        (tender_id,),
    )


def get_related_tenders(tender_id, category_slug=None, province_slug=None, limit=8):
    return fetch_all(
        '''
        SELECT slug, title, closing_at
        FROM tenders
        WHERE id <> %s
          AND (
            (%s IS NOT NULL AND category_slug = %s)
            OR
            (%s IS NOT NULL AND province_slug = %s)
          )
        ORDER BY is_active DESC, closing_at ASC NULLS LAST, published_at DESC NULLS LAST
        LIMIT %s
        ''',
        (tender_id, category_slug, category_slug, province_slug, province_slug, limit),
    )


def get_facet_page(facet_type, facet_key, limit=100):
    tender_field = 'province_slug' if facet_type == 'province' else 'category_slug'
    facet = fetch_one(
        "SELECT facet_key, facet_label, item_count FROM tender_facets WHERE facet_type = %s AND facet_key = %s LIMIT 1",
        (facet_type, facet_key),
    )
    items = fetch_all(
        f'''
        SELECT id, title, slug, summary, organ_of_state, category, province, esubmission,
               published_at, closing_at, reference_number, status, is_active
        FROM tenders
        WHERE {tender_field} = %s
          AND coalesce(seo_noindex, FALSE) = FALSE
        ORDER BY is_active DESC, COALESCE(closing_at, published_at, created_at) ASC NULLS LAST
        LIMIT %s
        ''',
        (facet_key, limit),
    )
    return facet, items


def get_combined_page(province_slug, category_slug, limit=100):
    province = fetch_one(
        "SELECT facet_key, facet_label, item_count FROM tender_facets WHERE facet_type = 'province' AND facet_key = %s LIMIT 1",
        (province_slug,),
    )
    category = fetch_one(
        "SELECT facet_key, facet_label, item_count FROM tender_facets WHERE facet_type = 'category' AND facet_key = %s LIMIT 1",
        (category_slug,),
    )
    items = fetch_all(
        '''
        SELECT id, title, slug, summary, organ_of_state, category, province, esubmission,
               published_at, closing_at, reference_number, status, is_active
        FROM tenders
        WHERE province_slug = %s
          AND category_slug = %s
          AND coalesce(seo_noindex, FALSE) = FALSE
        ORDER BY is_active DESC, COALESCE(closing_at, published_at, created_at) ASC NULLS LAST
        LIMIT %s
        ''',
        (province_slug, category_slug, limit),
    )
    return province, category, items
