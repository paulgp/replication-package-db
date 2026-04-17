"""Extract replication-deposit links from CrossRef's `relation` field.

Unlike 05_crossref_lookup.py (which mines references for openICPSR self-citations),
this script reads CrossRef's first-class metadata for data/supplement relations:

  - relation.has-dataset           -> dataset DOI or URL
  - relation.is-supplemented-by    -> supplemental material (often data repos)
  - relation.has-supplementary-material
  - relation.has-part              -> occasional usage for code packages
  - relation.has-version

It classifies the target host (openicpsr / dataverse / zenodo / mendeley /
github / figshare / osf / other) and inserts rows into repo_mappings with
source='crossref_relation' and the host stashed in icpsr_project_id as a
temporary classification until the repo_host schema column lands.

Focus: papers in finance journals (ISSNs 0022-1082 / 0304-405X / 0893-9454)
with publication_year >= policy_start_year from journals.json.
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
from urllib.parse import urlparse

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import CROSSREF_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import CROSSREF_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

RATE_LIMIT_SLEEP = 0.15
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_BASE = 1.0

CROSSREF_RAW_DIR = RAW_DIR / "crossref_relations"

# Which CrossRef relation types we treat as potential data/code deposits.
RELATION_KEYS = (
    "has-dataset",
    "is-supplemented-by",
    "has-supplementary-material",
    "has-part",
    "has-version",
)

# Host classifier: URL fragment -> host label stored in `source`.
HOST_PATTERNS = [
    ("openicpsr", re.compile(r"(?:openicpsr\.org|10\.3886/[eE]\d+)", re.IGNORECASE)),
    ("icpsr", re.compile(r"10\.3886/icpsr", re.IGNORECASE)),
    ("dataverse", re.compile(r"(?:dataverse\.harvard\.edu|10\.7910/DVN|dataverse\.)", re.IGNORECASE)),
    ("mendeley", re.compile(r"data\.mendeley\.com|mendeley\.com/datasets", re.IGNORECASE)),
    ("zenodo", re.compile(r"zenodo\.org|10\.5281/zenodo", re.IGNORECASE)),
    ("figshare", re.compile(r"figshare\.com|10\.6084/m9\.figshare", re.IGNORECASE)),
    ("osf", re.compile(r"osf\.io|10\.17605/osf", re.IGNORECASE)),
    ("github", re.compile(r"github\.com", re.IGNORECASE)),
]


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
        "Accept": "application/json",
        "User-Agent": f"ReplicationTracker/1.0 (mailto:{contact})",
    })
    return session


def classify_host(target: str) -> str:
    """Map a URL or DOI to a host label."""
    low = target.lower()
    for label, pat in HOST_PATTERNS:
        if pat.search(low):
            return label
    return "other"


def extract_doi_from_url(url: str) -> str | None:
    """If the URL encodes a DOI, return the bare DOI; else None."""
    parsed = urlparse(url)
    path = parsed.path
    # Common forms: https://doi.org/10.xxxx/yyy, https://dx.doi.org/..., some handle.net
    if parsed.netloc.endswith("doi.org") or "dx.doi.org" in parsed.netloc:
        doi = path.lstrip("/")
        return doi.lower() if doi.startswith("10.") else None
    m = re.search(r"(10\.\d{4,9}/[-._;()/:A-Za-z0-9]+)", url)
    if m:
        return m.group(1).lower()
    return None


def fetch_crossref(session: requests.Session, doi: str) -> dict[str, Any] | None:
    url = f"{CROSSREF_BASE_URL}/works/{requests.utils.quote(doi, safe='')}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            if resp.status_code in {429, 500, 502, 503, 504}:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else BACKOFF_BASE * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    LOGGER.warning(
                        "CrossRef %d for %s (%d/%d); retry in %.1fs",
                        resp.status_code, doi, attempt, MAX_RETRIES, sleep_s,
                    )
                    time.sleep(sleep_s)
                    continue
                LOGGER.error("CrossRef %d for %s after retries; skipping", resp.status_code, doi)
                return None
            resp.raise_for_status()
            return resp.json().get("message")
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))
            else:
                LOGGER.warning("CrossRef error for %s: %s; skipping", doi, exc)
                return None
    return None


def extract_deposits(message: dict[str, Any]) -> list[dict[str, str]]:
    """Return a list of {target, rel_type, host} entries extracted from relation."""
    out: list[dict[str, str]] = []
    rel = message.get("relation") or {}
    for key in RELATION_KEYS:
        entries = rel.get(key)
        if not entries:
            continue
        if not isinstance(entries, list):
            entries = [entries]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            target = entry.get("id")
            if not target:
                continue
            id_type = (entry.get("id-type") or "").lower()
            # Normalize DOIs
            if id_type == "doi":
                target_norm = target.lower()
            else:
                target_norm = target
            host = classify_host(target_norm)
            out.append({
                "target": target_norm,
                "rel_type": key,
                "host": host,
                "id_type": id_type or "uri",
            })
    return out


def load_finance_papers(conn, journal_issns: list[str]) -> list[str]:
    placeholders = ",".join("?" * len(journal_issns))
    rows = conn.execute(
        f"""
        SELECT DISTINCT p.doi FROM papers p
        WHERE p.journal_issn IN ({placeholders})
          AND p.doi IS NOT NULL
        ORDER BY p.doi
        """,
        journal_issns,
    ).fetchall()
    return [r[0] for r in rows]


def doi_cache_name(doi: str) -> str:
    return doi.replace("/", "_") + ".json"


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--issns", default="0022-1082,0304-405X,0893-9454",
                        help="Comma-separated ISSNs to process")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max papers to process (for testing)")
    args = parser.parse_args()
    issns = [s.strip() for s in args.issns.split(",") if s.strip()]

    CROSSREF_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        dois = load_finance_papers(conn, issns)
        if args.limit:
            dois = dois[:args.limit]
        total = len(dois)
        LOGGER.info("Processing %d papers across ISSNs %s", total, issns)

        # Clear old rows from this source so re-runs are idempotent.
        with conn:
            conn.execute("DELETE FROM repo_mappings WHERE source = 'crossref_relation'")

        session = build_session()
        with_deposit = 0
        total_deposits = 0
        host_counts: dict[str, int] = {}
        cache_hits = 0
        fetched = 0
        skipped = 0

        for idx, doi in enumerate(dois, 1):
            cache_path = CROSSREF_RAW_DIR / doi_cache_name(doi)
            message: dict[str, Any] | None = None

            if cache_path.exists():
                try:
                    message = json.loads(cache_path.read_text(encoding="utf-8"))
                    cache_hits += 1
                except (OSError, json.JSONDecodeError):
                    message = None

            if message is None:
                if fetched > 0:
                    time.sleep(RATE_LIMIT_SLEEP)
                message = fetch_crossref(session, doi)
                fetched += 1
                if message is None:
                    skipped += 1
                else:
                    try:
                        cache_path.write_text(
                            json.dumps(message, indent=2, sort_keys=True),
                            encoding="utf-8",
                        )
                    except OSError:
                        pass

            if message is None:
                continue

            deposits = extract_deposits(message)
            if deposits:
                with_deposit += 1
                total_deposits += len(deposits)
                # Insert mappings
                with conn:
                    for dep in deposits:
                        host_counts[dep["host"]] = host_counts.get(dep["host"], 0) + 1
                        # If target is a DOI, store in repo_doi; else keep the URL
                        repo_doi = dep["target"] if dep["id_type"] == "doi" else dep["target"]
                        conn.execute(
                            """
                            INSERT INTO repo_mappings
                              (paper_doi, repo_doi, icpsr_project_id, repo_host, source)
                            VALUES (?, ?, NULL, ?, 'crossref_relation')
                            """,
                            (doi, repo_doi, dep["host"]),
                        )

            if idx % 100 == 0:
                LOGGER.info(
                    "Progress: %d/%d | papers_with_deposit=%d | deposits=%d | cache=%d fetch=%d",
                    idx, total, with_deposit, total_deposits, cache_hits, fetched,
                )

        LOGGER.info("=" * 60)
        LOGGER.info("Processed:               %d", total)
        LOGGER.info("Papers with deposit:     %d (%.1f%%)", with_deposit, 100 * with_deposit / max(total, 1))
        LOGGER.info("Total deposits extracted: %d", total_deposits)
        LOGGER.info("Cache hits: %d | Fetched: %d | 404/errors: %d", cache_hits, fetched, skipped)
        LOGGER.info("-" * 40)
        LOGGER.info("Deposits by host:")
        for host, count in sorted(host_counts.items(), key=lambda x: -x[1]):
            LOGGER.info("  %-12s %d", host, count)

        # Per-journal breakdown
        LOGGER.info("-" * 40)
        rows = conn.execute(
            """
            SELECT p.journal_name, COUNT(DISTINCT rm.paper_doi)
            FROM repo_mappings rm JOIN papers p ON p.doi = rm.paper_doi
            WHERE rm.source = 'crossref_relation'
            GROUP BY p.journal_name
            ORDER BY 2 DESC
            """
        ).fetchall()
        LOGGER.info("Deposit-mapped papers by journal:")
        for r in rows:
            LOGGER.info("  %-40s %d", r[0], r[1])

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
