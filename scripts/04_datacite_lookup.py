"""Bulk lookup openICPSR mappings from DataCite and append new repo_mappings rows."""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATACITE_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR, RATE_LIMITS
    from db import get_connection, init_db
except ImportError:  # pragma: no cover - supports direct script execution and package imports
    from scripts.config import DATACITE_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR, RATE_LIMITS
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)
PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 60
RATE_LIMIT_SLEEP = 0.25
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.0
DATACITE_QUERY = (
    "prefix:10.3886 AND "
    "relatedIdentifiers.relationType:IsSupplementTo AND "
    "relatedIdentifiers.relatedIdentifier:10.1257*"
)
DATACITE_RAW_DIR = RAW_DIR / "datacite"
ICPSR_PROJECT_ID_RE = re.compile(r"10\.3886/[Ee](\d+)")
DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
)


def configure_logging() -> None:
    """Configure application logging to stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    """Return a configured session for DataCite API requests."""
    session = requests.Session()
    contact = OPENALEX_EMAIL or "unknown@example.com"
    session.headers.update(
        {
            "Accept": "application/vnd.api+json, application/json;q=0.9, */*;q=0.8",
            "User-Agent": f"AEA-Replication-Tracker/1.0 (contact: {contact})",
        }
    )
    return session


def normalize_doi(doi: str | None) -> str | None:
    """Normalize DOI text to lowercase bare DOI form."""
    if doi is None:
        return None

    normalized = doi.strip().lower()
    if not normalized:
        return None

    for prefix in DOI_PREFIXES:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
            break

    return normalized or None


def extract_icpsr_project_id(repo_doi: str | None) -> str | None:
    """Extract the openICPSR project id from a DataCite DOI."""
    if not repo_doi:
        return None

    match = ICPSR_PROJECT_ID_RE.search(repo_doi)
    if not match:
        return None
    return match.group(1)


def cache_page(page_number: int, payload: dict[str, Any]) -> None:
    """Persist a raw DataCite response page to disk."""
    DATACITE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = DATACITE_RAW_DIR / f"page_{page_number}.json"
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")



def parse_cursor_from_url(url: str | None) -> str | None:
    """Extract DataCite page[cursor] from a URL for logging/debugging."""
    if not url:
        return None

    parsed = urlparse(url)
    values = parse_qs(parsed.query).get("page[cursor]")
    if not values:
        return None
    return values[0]



def fetch_datacite_page(
    session: requests.Session,
    *,
    url: str | None = None,
    query: str | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Fetch one DataCite page with retry and exponential backoff."""
    if url is None:
        if query is None or cursor is None:
            raise ValueError("Either url or both query and cursor must be provided")
        request_url = f"{DATACITE_BASE_URL}/dois"
        params: dict[str, Any] | None = {
            "query": query,
            "page[size]": PAGE_SIZE,
            "page[cursor]": cursor,
        }
        cursor_label = cursor
    else:
        request_url = url
        params = None
        cursor_label = parse_cursor_from_url(url) or "<next>"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(request_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in {429, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("DataCite response was not a JSON object")
            return payload
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise
            sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning(
                "DataCite request failed for cursor=%s (attempt %d/%d): %s; retrying in %.2fs",
                cursor_label,
                attempt,
                MAX_RETRIES,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("Unreachable retry loop in fetch_datacite_page")



def fetch_all_datacite_records(session: requests.Session) -> list[dict[str, Any]]:
    """Fetch all DataCite pages using cursor pagination and cache raw pages."""
    records: list[dict[str, Any]] = []
    page_number = 1
    next_url: str | None = None

    while True:
        if next_url is None:
            payload = fetch_datacite_page(session, query=DATACITE_QUERY, cursor="1")
        else:
            payload = fetch_datacite_page(session, url=next_url)

        cache_page(page_number, payload)

        page_records = payload.get("data") or []
        if not isinstance(page_records, list):
            raise ValueError("DataCite response field 'data' was not a list")
        records.extend(page_records)

        meta = payload.get("meta") or {}
        total = meta.get("total")
        total_pages = meta.get("totalPages")
        LOGGER.info(
            "Fetched DataCite page %d with %d records (meta.total=%s, meta.totalPages=%s)",
            page_number,
            len(page_records),
            total,
            total_pages,
        )
        if isinstance(total, int) and total >= 10000:
            LOGGER.warning(
                "DataCite reported total=%d, near/at deep pagination limits; verify completeness.",
                total,
            )

        next_url = (payload.get("links") or {}).get("next")
        if not page_records or not next_url:
            break

        page_number += 1
        time.sleep(RATE_LIMIT_SLEEP)

    return records



def repo_doi_preference(repo_doi: str) -> tuple[int, str]:
    """Return a sort key preferring version DOIs, then parent DOIs, then anything else."""
    normalized = normalize_doi(repo_doi) or repo_doi.lower()
    suffix = normalized.split("10.3886/", 1)[-1]

    if re.fullmatch(r"e\d+v\d+", suffix):
        return (0, normalized)
    if re.fullmatch(r"e\d+", suffix):
        return (1, normalized)
    return (2, normalized)



def extract_mappings(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Extract deduplicated article→repository mappings from DataCite records."""
    best_by_key: dict[tuple[str, str], dict[str, str]] = {}

    for record in records:
        attributes = record.get("attributes") or {}
        repo_doi = normalize_doi(record.get("id") or attributes.get("doi"))
        project_id = extract_icpsr_project_id(repo_doi)
        if not repo_doi or not project_id:
            continue

        related_identifiers = attributes.get("relatedIdentifiers") or []
        if not isinstance(related_identifiers, list):
            continue

        article_dois: set[str] = set()
        for related in related_identifiers:
            if not isinstance(related, dict):
                continue
            if related.get("relationType") != "IsSupplementTo":
                continue
            if related.get("relatedIdentifierType") != "DOI":
                continue

            article_doi = normalize_doi(related.get("relatedIdentifier"))
            if article_doi and article_doi.startswith("10.1257"):
                article_dois.add(article_doi)

        for article_doi in article_dois:
            key = (article_doi, project_id)
            candidate = {
                "paper_doi": article_doi,
                "repo_doi": repo_doi,
                "icpsr_project_id": project_id,
            }
            incumbent = best_by_key.get(key)
            if incumbent is None or repo_doi_preference(candidate["repo_doi"]) < repo_doi_preference(
                incumbent["repo_doi"]
            ):
                best_by_key[key] = candidate

    return sorted(best_by_key.values(), key=lambda row: (row["paper_doi"], row["icpsr_project_id"]))



def store_new_mappings(conn, mappings: list[dict[str, str]]) -> dict[str, int]:
    """Insert only new FK-safe mappings, skipping papers already mapped from any source."""
    mapped_papers = {
        row[0]
        for row in conn.execute("SELECT DISTINCT paper_doi FROM repo_mappings WHERE paper_doi IS NOT NULL")
    }
    valid_papers = {
        row[0]
        for row in conn.execute("SELECT doi FROM papers WHERE doi IS NOT NULL")
    }

    already_mapped = 0
    unmatched = 0
    eligible = 0

    for mapping in mappings:
        paper_doi = mapping["paper_doi"]
        if paper_doi in mapped_papers:
            already_mapped += 1
        elif paper_doi not in valid_papers:
            unmatched += 1
        else:
            eligible += 1

    before_changes = conn.total_changes
    with conn:
        conn.executemany(
            """
            INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, source)
            SELECT ?, ?, ?, 'datacite'
            WHERE EXISTS (
                SELECT 1
                FROM papers
                WHERE doi = ?
            )
              AND NOT EXISTS (
                SELECT 1
                FROM repo_mappings
                WHERE paper_doi = ?
            )
            """,
            [
                (
                    mapping["paper_doi"],
                    mapping["repo_doi"],
                    mapping["icpsr_project_id"],
                    mapping["paper_doi"],
                    mapping["paper_doi"],
                )
                for mapping in mappings
            ],
        )
    inserted = conn.total_changes - before_changes

    return {
        "unique_mappings": len(mappings),
        "eligible_new": eligible,
        "inserted": inserted,
        "already_mapped": already_mapped,
        "unmatched": unmatched,
    }



def main() -> int:
    """Run the DataCite bulk lookup and append new mappings."""
    configure_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATACITE_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        with build_session() as session:
            records = fetch_all_datacite_records(session)

        mappings = extract_mappings(records)
        store_stats = store_new_mappings(conn, mappings)

        LOGGER.info("Total DataCite records fetched: %d", len(records))
        LOGGER.info("Unique mappings extracted: %d", store_stats["unique_mappings"])
        LOGGER.info("New mappings inserted: %d", store_stats["inserted"])
        LOGGER.info("Skipped already mapped papers: %d", store_stats["already_mapped"])
        LOGGER.info("Unmatched papers (not in papers table): %d", store_stats["unmatched"])
        LOGGER.info("Eligible unmapped papers before SQL insert guard: %d", store_stats["eligible_new"])
        LOGGER.info("Cached raw DataCite pages under: %s", DATACITE_RAW_DIR)
        return 0
    except requests.HTTPError as exc:
        LOGGER.error("DataCite HTTP request failed: %s", exc)
        return 1
    except requests.RequestException as exc:
        LOGGER.error("DataCite request failed: %s", exc)
        return 1
    except (OSError, ValueError) as exc:
        LOGGER.error("Failed to process DataCite results: %s", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
