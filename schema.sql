CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE sources (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    base_url TEXT,
    api_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tenders (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    external_id TEXT,
    ocid TEXT,
    tender_number TEXT,
    reference_number TEXT,
    title TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    description TEXT,
    summary TEXT,
    buyer_name TEXT,
    organ_of_state TEXT,
    category TEXT,
    category_slug TEXT,
    province TEXT,
    province_slug TEXT,
    published_at TIMESTAMPTZ,
    closing_at TIMESTAMPTZ,
    briefing_at TIMESTAMPTZ,
    status VARCHAR(50) NOT NULL DEFAULT 'open',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    esubmission BOOLEAN,
    source_url TEXT,
    notice_url TEXT,
    searchable_text TEXT,
    closing_soon BOOLEAN NOT NULL DEFAULT FALSE,
    canonical_url TEXT,
    meta_title TEXT,
    meta_description TEXT,
    last_indexed_at TIMESTAMPTZ,
    seo_noindex BOOLEAN NOT NULL DEFAULT FALSE,
    raw_data JSONB NOT NULL,
    fingerprint CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tenders_fingerprint_unique UNIQUE (fingerprint)
);

CREATE INDEX idx_tenders_status ON tenders(status);
CREATE INDEX idx_tenders_is_active ON tenders(is_active);
CREATE INDEX idx_tenders_closing_at ON tenders(closing_at DESC);
CREATE INDEX idx_tenders_published_at ON tenders(published_at DESC);
CREATE INDEX idx_tenders_category_slug ON tenders(category_slug);
CREATE INDEX idx_tenders_province_slug ON tenders(province_slug);
CREATE INDEX idx_tenders_source_id ON tenders(source_id);
CREATE INDEX idx_tenders_closing_soon ON tenders(closing_soon);
CREATE INDEX idx_tenders_raw_data_gin ON tenders USING GIN (raw_data);
CREATE INDEX idx_tenders_searchable_text ON tenders USING GIN (to_tsvector('simple', coalesce(searchable_text, '')));

CREATE TABLE tender_documents (
    id BIGSERIAL PRIMARY KEY,
    tender_id BIGINT NOT NULL REFERENCES tenders(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title TEXT,
    mime_type TEXT,
    file_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tender_documents_tender_id ON tender_documents(tender_id);

CREATE TABLE ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    run_type VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    items_fetched INTEGER NOT NULL DEFAULT 0,
    items_inserted INTEGER NOT NULL DEFAULT 0,
    items_updated INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE tender_facets (
    id BIGSERIAL PRIMARY KEY,
    facet_type VARCHAR(50) NOT NULL,
    facet_key TEXT NOT NULL,
    facet_label TEXT NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (facet_type, facet_key)
);

CREATE TABLE publish_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type VARCHAR(50) NOT NULL,
    payload JSONB,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

INSERT INTO sources (code, name, base_url, api_url)
VALUES (
    'etender',
    'South Africa eTender / Transparency Portal',
    'https://www.etenders.gov.za/',
    'https://ocds-api.etenders.gov.za/'
)
ON CONFLICT (code) DO NOTHING;
