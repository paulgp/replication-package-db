"""DataCite lookup for finance journal deposits (JoF / JFE / RFS).

Generalizes 04_datacite_lookup.py (which is openICPSR-only and AEA-only) to:
  - any DataCite record (any repository prefix)
  - filtering by `types.resourceTypeGeneral` in {Dataset, Software, Collection,
    Workflow, OutputManagementPlan, PhysicalObject}
  - related identifiers matching finance journal DOI prefixes

Writes into `repo_mappings` with source='datacite_finance'. Uses the existing
`icpsr_project_id` column to stash the host label (openicpsr / dataverse /
zenodo / mendeley / figshare / osf / github / other) until the repo_host
schema migration lands.
"""

from __future__ import annotations

import argparse
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
    from config import DATACITE_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import DATACITE_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)
PAGE_SIZE = 1000
REQUEST_TIMEOUT = 60
RATE_LIMIT_SLEEP = 0.25
MAX_RETRIES = 5
BACKOFF_BASE = 1.0

DATACITE_RAW_DIR = RAW_DIR / "datacite_finance"

# Prefix -> journal ISSN for matching results back to papers
FINANCE_PREFIX_ISSN = {
    "10.1016/j.jfineco": "0304-405X",
    "10.1111/jofi": "0022-1082",
    "10.1093/rfs": "0893-9454",
}

# Resource types that indicate a replication deposit (exclude journal articles
# that also happen to cite finance papers)
DEPOSIT_RESOURCE_TYPES = {
    "Dataset",
    "Software",
    "Collection",
    "Workflow",
    "OutputManagementPlan",
    "PhysicalObject",
}

HOST_PATTERNS = [
    ("openicpsr", re.compile(r"10\.3886/[eE]\d+", re.IGNORECASE)),
    ("icpsr", re.compile(r"10\.3886/icpsr", re.IGNORECASE)),
    ("dataverse", re.compile(r"10\.7910/", re.IGNORECASE)),
    ("zenodo", re.compile(r"10\.5281/zenodo", re.IGNORECASE)),
    ("figshare", re.compile(r"10\.6084/", re.IGNORECASE)),
    ("osf", re.compile(r"10\.17605/osf", re.IGNORECASE)),
    ("mendeley", re.compile(r"10\.17632/", re.IGNORECASE)),
]

DOI_PREFIXES = ("https://doi.org/", "http://doi.org/",
                "https://dx.doi.org/", "http://dx.doi.org/", "doi:")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    session = requests.Session()
    contact = OPENALEX_EMAIL or "unknown@example.com"
    session.headers.update({
        "Accept": "application/vnd.api+json, application/json;q=0.9",
        "User-Agent": f"ReplicationTracker/1.0 (mailto:{contact})",
    })
    return session


def normalize_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    d = doi.strip().lower()
    for pref in DOI_PREFIXES:
        if d.startswith(pref):
            d = d[len(pref):].strip()
            break
    return d or None


def classify_host(repo_doi: str, publisher: str | None = None) -> str:
    for label, pat in HOST_PATTERNS:
        if pat.search(repo_doi):
            return label
    if publisher:
        pl = publisher.lower()
        if "dataverse" in pl:
            return "dataverse"
        if "zenodo" in pl:
            return "zenodo"
        if "icpsr" in pl:
            return "openicpsr"
        if "figshare" in pl:
            return "figshare"
        if "mendeley" in pl:
            return "mendeley"
        if "osf" in pl or "open science framework" in pl:
            return "osf"
    return "other"


def parse_cursor_from_url(url: str | None) -> str | None:
    if not url:
        return None
    p = urlparse(url)
    vals = parse_qs(p.query).get("page[cursor]")
    return vals[0] if vals else None


def fetch_page(session: requests.Session, *, url: str | None = None,
               query: str | None = None, cursor: str | None = None) -> dict[str, Any]:
    if url is None:
        if query is None or cursor is None:
            raise ValueError("need either url or (query, cursor)")
        request_url = f"{DATACITE_BASE_URL}/dois"
        params: dict[str, Any] | None = {
            "query": query,
            "page[size]": PAGE_SIZE,
            "page[cursor]": cursor,
        }
        label = cursor
    else:
        request_url = url
        params = None
        label = parse_cursor_from_url(url) or "<next>"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(request_url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code in {429, 500, 502, 503, 504}:
                resp.raise_for_status()
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError("response not a JSON object")
            return payload
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise
            sleep_s = BACKOFF_BASE * (2 ** (attempt - 1))
            LOGGER.warning(
                "DataCite request failed (cursor=%s, attempt %d/%d): %s; retrying in %.1fs",
                label, attempt, MAX_RETRIES, exc, sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError("unreachable")


def fetch_all_for_prefix(session: requests.Session, prefix: str,
                         cache_dir: Path) -> list[dict[str, Any]]:
    """Fetch all DataCite records where relatedIdentifier matches the paper prefix."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    query = f"relatedIdentifiers.relatedIdentifier:{prefix}*"
    records: list[dict[str, Any]] = []
    page = 1
    next_url: str | None = None
    while True:
        if next_url is None:
            payload = fetch_page(session, query=query, cursor="1")
        else:
            payload = fetch_page(session, url=next_url)

        (cache_dir / f"page_{page}.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        page_records = payload.get("data") or []
        records.extend(page_records)
        meta = payload.get("meta") or {}
        LOGGER.info(
            "  prefix=%s page=%d records=%d (total=%s)",
            prefix, page, len(page_records), meta.get("total"),
        )
        next_url = (payload.get("links") or {}).get("next")
        if not page_records or not next_url:
            break
        page += 1
        time.sleep(RATE_LIMIT_SLEEP)
    return records


def is_deposit(record: dict[str, Any]) -> bool:
    attrs = record.get("attributes") or {}
    types = attrs.get("types") or {}
    rtg = types.get("resourceTypeGeneral")
    return rtg in DEPOSIT_RESOURCE_TYPES


def extract_mappings(records: list[dict[str, Any]],
                     prefix: str) -> list[dict[str, str]]:
    """Return list of {paper_doi, repo_doi, host} for deposit records."""
    out: list[dict[str, str]] = []
    for rec in records:
        if not is_deposit(rec):
            continue
        attrs = rec.get("attributes") or {}
        repo_doi = normalize_doi(rec.get("id") or attrs.get("doi"))
        if not repo_doi:
            continue
        host = classify_host(repo_doi, attrs.get("publisher"))
        # Find the finance paper(s) this record relates to
        paper_dois: set[str] = set()
        for ri in attrs.get("relatedIdentifiers") or []:
            if not isinstance(ri, dict):
                continue
            if ri.get("relatedIdentifierType") != "DOI":
                continue
            rid = normalize_doi(ri.get("relatedIdentifier"))
            if rid and rid.startswith(prefix):
                paper_dois.add(rid)
        for paper_doi in paper_dois:
            out.append({
                "paper_doi": paper_doi,
                "repo_doi": repo_doi,
                "host": host,
            })
    return out


def store_mappings(conn, mappings: list[dict[str, str]]) -> dict[str, int]:
    # Clear prior rows from this source (idempotent re-run)
    with conn:
        conn.execute("DELETE FROM repo_mappings WHERE source = 'datacite_finance'")

    valid = {row[0] for row in conn.execute(
        "SELECT doi FROM papers WHERE doi IS NOT NULL")}
    inserted = 0
    fk_missing = 0
    duplicates = 0
    seen: set[tuple[str, str]] = set()
    with conn:
        for m in mappings:
            key = (m["paper_doi"], m["repo_doi"])
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            if m["paper_doi"] not in valid:
                fk_missing += 1
                continue
            conn.execute(
                """
                INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, repo_host, source)
                VALUES (?, ?, NULL, ?, 'datacite_finance')
                """,
                (m["paper_doi"], m["repo_doi"], m["host"]),
            )
            inserted += 1
    return {
        "inserted": inserted,
        "fk_missing": fk_missing,
        "duplicates": duplicates,
    }


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prefixes",
        default=",".join(FINANCE_PREFIX_ISSN.keys()),
        help="Comma-separated paper DOI prefixes to query",
    )
    args = parser.parse_args()
    prefixes = [p.strip() for p in args.prefixes.split(",") if p.strip()]

    DATACITE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    db_path = init_db()
    conn = get_connection(db_path)

    try:
        all_mappings: list[dict[str, str]] = []
        per_prefix_raw: dict[str, int] = {}
        per_prefix_deposits: dict[str, int] = {}

        with build_session() as session:
            for prefix in prefixes:
                LOGGER.info("Querying DataCite for prefix %s", prefix)
                safe_prefix = prefix.replace("/", "_")
                records = fetch_all_for_prefix(
                    session, prefix, DATACITE_RAW_DIR / safe_prefix,
                )
                per_prefix_raw[prefix] = len(records)
                mappings = extract_mappings(records, prefix)
                per_prefix_deposits[prefix] = len(mappings)
                all_mappings.extend(mappings)
                LOGGER.info(
                    "  %s: %d records, %d deposit mappings extracted",
                    prefix, len(records), len(mappings),
                )

        stats = store_mappings(conn, all_mappings)

        LOGGER.info("=" * 60)
        LOGGER.info("DataCite finance lookup complete")
        LOGGER.info("-" * 40)
        for prefix in prefixes:
            LOGGER.info(
                "  %-20s raw=%d  deposit_mappings=%d",
                prefix, per_prefix_raw.get(prefix, 0),
                per_prefix_deposits.get(prefix, 0),
            )
        LOGGER.info("-" * 40)
        LOGGER.info("Rows inserted: %d", stats["inserted"])
        LOGGER.info("FK missing (paper not in DB): %d", stats["fk_missing"])
        LOGGER.info("Duplicate (paper_doi, repo_doi) pairs skipped: %d", stats["duplicates"])

        # Host breakdown
        rows = conn.execute(
            """
            SELECT icpsr_project_id AS host, COUNT(*)
            FROM repo_mappings WHERE source='datacite_finance'
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall()
        LOGGER.info("Host breakdown:")
        for r in rows:
            LOGGER.info("  %-12s %d", r[0], r[1])

        # Journal breakdown
        rows = conn.execute(
            """
            SELECT p.journal_name, COUNT(DISTINCT rm.paper_doi)
            FROM repo_mappings rm JOIN papers p ON p.doi = rm.paper_doi
            WHERE rm.source='datacite_finance'
            GROUP BY p.journal_name ORDER BY 2 DESC
            """
        ).fetchall()
        LOGGER.info("Journal breakdown:")
        for r in rows:
            LOGGER.info("  %-40s %d papers", r[0], r[1])

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
