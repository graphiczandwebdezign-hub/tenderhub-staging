import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DB_DSN = os.environ["DB_DSN"]
ETENDER_API_URL = os.getenv("ETENDER_API_URL", "").strip()
ETENDER_DOWNLOAD_URL = os.getenv("ETENDER_DOWNLOAD_URL", "").strip()
SOURCE_CODE = "etender"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s_]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")[:180]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        try:
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d %B %Y", "%d %b %Y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def normalize_province(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not value:
        return None, None

    mapping = {
        "eastern cape": "Eastern Cape",
        "free state": "Free State",
        "gauteng": "Gauteng",
        "kwazulu-natal": "KwaZulu-Natal",
        "kzn": "KwaZulu-Natal",
        "kwa-zulu natal": "KwaZulu-Natal",
        "limpopo": "Limpopo",
        "mpumalanga": "Mpumalanga",
        "national": "National",
        "north west": "North West",
        "northern cape": "Northern Cape",
        "western cape": "Western Cape",
        "south africa": "South Africa",
    }

    key = value.strip().lower()
    normalized = mapping.get(key, value.strip())
    return normalized, slugify(normalized)


def infer_status(closing_at: Optional[datetime]) -> str:
    if closing_at and closing_at < now_utc():
        return "closed"
    return "open"


def _looks_like_ocds_record(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = set(obj.keys())
    strong = {"ocid", "tender", "buyer", "parties", "id", "title", "referenceNumber", "tenderNumber"}
    return bool(keys & strong)


def _find_record_list(payload: Any) -> List[Dict[str, Any]]:
    best: List[Dict[str, Any]] = []

    def score(items: List[Dict[str, Any]]) -> Tuple[int, int]:
        return (len(items), sum(1 for x in items if _looks_like_ocds_record(x)))

    def walk(node: Any):
        nonlocal best
        if isinstance(node, list):
            dict_items = [x for x in node if isinstance(x, dict)]
            if dict_items and score(dict_items) > score(best):
                best = dict_items
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            if "releases" in node and isinstance(node["releases"], list):
                rels = [x for x in node["releases"] if isinstance(x, dict)]
                if rels and score(rels) >= score(best):
                    best = rels
            if "records" in node and isinstance(node["records"], list):
                compiled = []
                for rec in node["records"]:
                    if isinstance(rec, dict) and isinstance(rec.get("compiledRelease"), dict):
                        compiled.append(rec["compiledRelease"])
                if compiled and score(compiled) >= score(best):
                    best = compiled
            for value in node.values():
                walk(value)

    walk(payload)
    return best


def extract_documents(record: Dict[str, Any]) -> List[Dict[str, Optional[str]]]:
    docs: List[Dict[str, Optional[str]]] = []

    tender = record.get("tender") or {}
    documents = tender.get("documents") or []
    for doc in documents:
        docs.append({
            "url": doc.get("url"),
            "title": doc.get("title") or doc.get("description"),
            "mime_type": doc.get("format"),
            "file_name": None,
        })

    for key in ("documents", "attachments"):
        maybe = record.get(key)
        if isinstance(maybe, list):
            for doc in maybe:
                if isinstance(doc, dict) and doc.get("url"):
                    docs.append({
                        "url": doc.get("url"),
                        "title": doc.get("title") or doc.get("description"),
                        "mime_type": doc.get("format"),
                        "file_name": None,
                    })

    deduped = []
    seen = set()
    for d in docs:
        url = d.get("url")
        if url and url not in seen:
            deduped.append(d)
            seen.add(url)
    return deduped


def normalize_ocds_record(record: Dict[str, Any]) -> Dict[str, Any]:
    tender = record.get("tender") or {}
    buyer = record.get("buyer") or {}
    parties = record.get("parties") or []

    title = (tender.get("title") or record.get("title") or "Untitled Tender").strip()
    description = tender.get("description") or record.get("description")
    summary = description[:300] if description else None

    ocid = record.get("ocid")
    external_id = record.get("id")
    tender_number = tender.get("id") or record.get("tenderNumber")
    reference_number = tender.get("referenceNumber") or record.get("referenceNumber") or tender_number

    buyer_name = buyer.get("name")
    if not buyer_name:
        for p in parties:
            if isinstance(p, dict) and p.get("name"):
                buyer_name = p["name"]
                break

    province_raw = (
        tender.get("province")
        or record.get("province")
        or tender.get("deliveryLocation")
        or record.get("deliveryLocation")
    )
    province, province_slug = normalize_province(province_raw)

    category = (
        tender.get("mainProcurementCategory")
        or tender.get("classification")
        or record.get("category")
        or tender.get("procurementMethodDetails")
    )
    category_slug = slugify(category) if category else None

    published_at = (
        parse_dt(tender.get("datePublished"))
        or parse_dt(record.get("date"))
        or parse_dt(record.get("publishedDate"))
        or parse_dt(record.get("releaseDate"))
    )
    closing_at = (
        parse_dt((tender.get("tenderPeriod") or {}).get("endDate"))
        or parse_dt(record.get("closingDate"))
    )
    briefing_at = (
        parse_dt(record.get("briefingDate"))
        or parse_dt((tender.get("enquiryPeriod") or {}).get("endDate"))
    )

    notice_url = record.get("url") or tender.get("url") or record.get("noticeUrl")
    organ_of_state = record.get("organOfState") or buyer_name
    esubmission = record.get("eSubmission")
    if esubmission is None:
        esubmission = record.get("esubmission")

    status = record.get("status") or infer_status(closing_at)

    slug_base_parts = [title, reference_number or "", province or ""]
    slug_base = " ".join(part for part in slug_base_parts if part).strip()
    slug = slugify(slug_base) or sha256_text(title)[:24]

    fingerprint_input = "||".join([
        SOURCE_CODE,
        ocid or "",
        external_id or "",
        reference_number or "",
        title or "",
        closing_at.isoformat() if closing_at else "",
    ])
    fingerprint = sha256_text(fingerprint_input)

    if not province:
        province = "South Africa"
        province_slug = "south-africa"
    if not category:
        category = "Other"
        category_slug = "other"

    return {
        "external_id": external_id,
        "ocid": ocid,
        "tender_number": tender_number,
        "reference_number": reference_number,
        "title": title,
        "slug": slug,
        "description": description,
        "summary": summary,
        "buyer_name": buyer_name,
        "organ_of_state": organ_of_state,
        "category": category,
        "category_slug": category_slug,
        "province": province,
        "province_slug": province_slug,
        "published_at": published_at,
        "closing_at": closing_at,
        "briefing_at": briefing_at,
        "status": status,
        "is_active": status == "open",
        "esubmission": esubmission,
        "source_url": notice_url,
        "notice_url": notice_url,
        "raw_data": record,
        "fingerprint": fingerprint,
        "documents": extract_documents(record),
    }


def fetch_json_from_api(url: str) -> List[Dict[str, Any]]:
    logging.info("Fetching API data from %s", url)
    r = requests.get(url, timeout=180, headers={"User-Agent": "TenderHubBot/1.0"})
    if not r.ok:
        raise ValueError(f"API request failed: {r.status_code} {r.text[:500]}")
    payload = r.json()

    records = _find_record_list(payload)
    if records:
        return records

    if isinstance(payload, dict) and _looks_like_ocds_record(payload):
        return [payload]

    raise ValueError(f"Unsupported API payload format: {str(payload)[:500]}")


def fetch_all_api_pages(base_url: str, page_size: int = 1000, max_pages: int = 20) -> List[Dict[str, Any]]:
    all_records: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        url = base_url
        if "PageNumber=" in url:
            url = re.sub(r"PageNumber=\d+", f"PageNumber={page}", url)
        else:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}PageNumber={page}"

        if "PageSize=" in url:
            url = re.sub(r"PageSize=\d+", f"PageSize={page_size}", url)
        else:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}PageSize={page_size}"

        records = fetch_json_from_api(url)
        if not records:
            logging.info("No records on page %s, stopping.", page)
            break

        logging.info("Fetched %s records from API page %s", len(records), page)
        all_records.extend(records)

        if len(records) < page_size:
            break

    return all_records


def fetch_json_from_download(url: str) -> List[Dict[str, Any]]:
    logging.info("Fetching downloadable JSON from %s", url)
    r = requests.get(url, timeout=120, headers={"User-Agent": "TenderHubBot/1.0"})
    r.raise_for_status()
    text = r.text.strip()

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if not payload:
            raise ValueError(f"Unsupported download payload format. First 500 chars: {text[:500]}")

    records = _find_record_list(payload)
    if records:
        return records
    if isinstance(payload, dict) and _looks_like_ocds_record(payload):
        return [payload]
    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        return payload

    raise ValueError(f"Unsupported download payload format. First 500 chars: {text[:500]}")


def get_conn():
    return psycopg2.connect(DB_DSN)


def get_source_id(cur) -> int:
    cur.execute("SELECT id FROM sources WHERE code = %s", (SOURCE_CODE,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Source {SOURCE_CODE} not found in sources table")
    return row[0]


def start_ingestion_run(cur, source_id: int, run_type: str) -> int:
    cur.execute(
        """
        INSERT INTO ingestion_runs (source_id, run_type, status)
        VALUES (%s, %s, 'running')
        RETURNING id
        """,
        (source_id, run_type),
    )
    return cur.fetchone()[0]


def finish_ingestion_run(cur, run_id: int, status: str, fetched: int, inserted: int, updated: int, error_message: Optional[str] = None):
    cur.execute(
        """
        UPDATE ingestion_runs
        SET completed_at = NOW(),
            status = %s,
            items_fetched = %s,
            items_inserted = %s,
            items_updated = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, fetched, inserted, updated, error_message, run_id),
    )


def upsert_tender(cur, source_id: int, item: Dict[str, Any]) -> Tuple[int, str]:
    cur.execute(
        """
        INSERT INTO tenders (
            source_id, external_id, ocid, tender_number, reference_number,
            title, slug, description, summary,
            buyer_name, organ_of_state,
            category, category_slug, province, province_slug,
            published_at, closing_at, briefing_at,
            status, is_active, esubmission,
            source_url, notice_url, raw_data, fingerprint,
            first_seen_at, last_seen_at, created_at, updated_at
        )
        VALUES (
            %(source_id)s, %(external_id)s, %(ocid)s, %(tender_number)s, %(reference_number)s,
            %(title)s, %(slug)s, %(description)s, %(summary)s,
            %(buyer_name)s, %(organ_of_state)s,
            %(category)s, %(category_slug)s, %(province)s, %(province_slug)s,
            %(published_at)s, %(closing_at)s, %(briefing_at)s,
            %(status)s, %(is_active)s, %(esubmission)s,
            %(source_url)s, %(notice_url)s, %(raw_data)s::jsonb, %(fingerprint)s,
            NOW(), NOW(), NOW(), NOW()
        )
        ON CONFLICT (fingerprint)
        DO UPDATE SET
            external_id = EXCLUDED.external_id,
            ocid = EXCLUDED.ocid,
            tender_number = EXCLUDED.tender_number,
            reference_number = EXCLUDED.reference_number,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            summary = EXCLUDED.summary,
            buyer_name = EXCLUDED.buyer_name,
            organ_of_state = EXCLUDED.organ_of_state,
            category = EXCLUDED.category,
            category_slug = EXCLUDED.category_slug,
            province = EXCLUDED.province,
            province_slug = EXCLUDED.province_slug,
            published_at = EXCLUDED.published_at,
            closing_at = EXCLUDED.closing_at,
            briefing_at = EXCLUDED.briefing_at,
            status = EXCLUDED.status,
            is_active = EXCLUDED.is_active,
            esubmission = EXCLUDED.esubmission,
            source_url = EXCLUDED.source_url,
            notice_url = EXCLUDED.notice_url,
            raw_data = EXCLUDED.raw_data,
            last_seen_at = NOW(),
            updated_at = NOW()
        RETURNING id, xmax
        """,
        {
            "source_id": source_id,
            **{
                k: json.dumps(v) if k == "raw_data" else v
                for k, v in item.items()
                if k != "documents"
            },
        },
    )
    tender_id, xmax = cur.fetchone()
    action = "inserted" if xmax == 0 else "updated"

    cur.execute("DELETE FROM tender_documents WHERE tender_id = %s", (tender_id,))
    docs = item.get("documents") or []
    if docs:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO tender_documents (tender_id, url, title, mime_type, file_name)
            VALUES %s
            """,
            [
                (tender_id, d.get("url"), d.get("title"), d.get("mime_type"), d.get("file_name"))
                for d in docs
            ],
        )

    return tender_id, action


def refresh_facets(cur):
    cur.execute("DELETE FROM tender_facets")

    cur.execute(
        """
        INSERT INTO tender_facets (facet_type, facet_key, facet_label, item_count, updated_at)
        SELECT 'province', province_slug, province, COUNT(*), NOW()
        FROM tenders
        WHERE province IS NOT NULL
          AND province_slug IS NOT NULL
        GROUP BY province_slug, province
        """
    )

    cur.execute(
        """
        INSERT INTO tender_facets (facet_type, facet_key, facet_label, item_count, updated_at)
        SELECT 'category', category_slug, category, COUNT(*), NOW()
        FROM tenders
        WHERE category IS NOT NULL
          AND category_slug IS NOT NULL
        GROUP BY category_slug, category
        """
    )


def main():
    if not ETENDER_API_URL and not ETENDER_DOWNLOAD_URL:
        raise RuntimeError("Set ETENDER_API_URL or ETENDER_DOWNLOAD_URL in .env")

    run_type = "api" if ETENDER_API_URL else "download"
    source_records = fetch_all_api_pages(ETENDER_API_URL, page_size=100, max_pages=10) if ETENDER_API_URL else fetch_json_from_download(ETENDER_DOWNLOAD_URL)

    fetched = len(source_records)
    inserted = 0
    updated = 0

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            source_id = get_source_id(cur)
            run_id = start_ingestion_run(cur, source_id, run_type)
            conn.commit()

            try:
                with conn.cursor() as cur2:
                    for raw in source_records:
                        item = normalize_ocds_record(raw)
                        _, action = upsert_tender(cur2, source_id, item)
                        if action == "inserted":
                            inserted += 1
                        else:
                            updated += 1

                    cur2.execute(
                        """
                        UPDATE tenders
                        SET status = 'closed',
                            is_active = FALSE,
                            updated_at = NOW()
                        WHERE source_id = %s
                          AND closing_at IS NOT NULL
                          AND closing_at < NOW()
                          AND status <> 'closed'
                        """,
                        (source_id,),
                    )

                    refresh_facets(cur2)
                    finish_ingestion_run(cur2, run_id, "success", fetched, inserted, updated)
                conn.commit()
                logging.info("Done. fetched=%s inserted=%s updated=%s", fetched, inserted, updated)

            except Exception as inner_exc:
                conn.rollback()
                with conn.cursor() as cur3:
                    finish_ingestion_run(cur3, run_id, "failed", fetched, inserted, updated, str(inner_exc))
                conn.commit()
                raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
