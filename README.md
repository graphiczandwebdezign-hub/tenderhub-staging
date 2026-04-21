# TenderHub SA Starter

A fuller starter project for a South African tenders website built around eTender ingestion, auto-sync jobs, and SEO-friendly pages.

## Included
- PostgreSQL schema with SEO and publishing fields
- eTender ingestion script (`ingest_etender.py`)
- enrichment job (`enrich_tenders.py`)
- reconciliation job (`reconcile_tenders.py`)
- sitemap builder (`build_sitemaps.py`)
- Flask app with homepage, tender detail, province, category, and combined pages
- Jinja templates and responsive CSS
- `.env.example`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
createdb tenderhub
psql -d tenderhub -f schema.sql
cp .env.example .env
```

Fill in `.env` with your `DB_DSN` and either `ETENDER_API_URL` or `ETENDER_DOWNLOAD_URL`.

## Run the initial import

```bash
python ingest_etender.py
python enrich_tenders.py
```

## Start the site

```bash
flask --app app.main run --debug
```

Open `http://127.0.0.1:5000`

## Build sitemaps

```bash
python build_sitemaps.py
```

This writes XML files into `generated_sitemaps/`.

## Suggested cron jobs

```bash
*/30 * * * * cd /path/to/tenderhub_complete_starter && /path/to/venv/bin/python ingest_etender.py >> logs/ingest.log 2>&1
10 2 * * * cd /path/to/tenderhub_complete_starter && /path/to/venv/bin/python reconcile_tenders.py >> logs/reconcile.log 2>&1
20 2 * * * cd /path/to/tenderhub_complete_starter && /path/to/venv/bin/python enrich_tenders.py >> logs/enrich.log 2>&1
30 2 * * * cd /path/to/tenderhub_complete_starter && /path/to/venv/bin/python build_sitemaps.py >> logs/sitemaps.log 2>&1
```

## Notes
- You still need to plug in the exact live eTender API endpoint or monthly JSON download URL.
- Closed tender detail pages can still be useful for long-tail SEO, while stale low-value pages are marked noindex by the enrichment job.
- Province + category combined pages are intended for stronger long-tail ranking.
