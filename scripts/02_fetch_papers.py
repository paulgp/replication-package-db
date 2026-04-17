"""Fetch all AEA journal papers from OpenAlex and store in the papers table."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# Editorial / front-back-matter titles to drop. Matches observed JoF/JFE/RFS
# noise (Report of the Editor, Issue Information, Editorial Board, etc.).
EDITORIAL_TITLE_PATTERN = re.compile(
    r"(?i)"
    r"\bissue\s+information\b|"
    r"\b(?:front|back)\s+matter\b|"
    r"\bmasthead\b|"
    r"\b(?:errata|erratum|corrigendum|corrigenda)\b|"
    r"\bcall\s+for\s+papers\b|"
    r"\bin\s+memoriam\b|\bobituary\b|"
    r"\bminutes\s+of\s+the\b|"
    r"\breport\s+of\s+the\s+editor\b|"
    r"^\s*editor['\u2019]?s?\s+(?:note|report)\b|"
    r"^\s*editorial\s+board\s*$"
)

# ---------------------------------------------------------------------------
# sys.path / import setup — same pattern as 01_fetch_journals.py
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import (
        DATA_DIR,
        OPENALEX_BASE_URL,
        OPENALEX_EMAIL,
        RAW_DIR,
    )
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import (
        DATA_DIR,
        OPENALEX_BASE_URL,
        OPENALEX_EMAIL,
        RAW_DIR,
    )
    from scripts.db import get_connection, init_db

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 3.0
RATE_LIMIT_SLEEP_SECONDS = 0.25  # ~4 req/s, well under polite pool limit
PER_PAGE = 200  # OpenAlex max

JOURNALS_PATH = DATA_DIR / "journals.json"
PIPELINE_STATE_PATH = DATA_DIR / "pipeline_state.json"
RAW_PAPERS_DIR = RAW_DIR / "papers"

# ---------------------------------------------------------------------------
# Upsert SQL — ON CONFLICT(openalex_id) DO UPDATE, NOT INSERT OR REPLACE
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO papers (
    openalex_id, doi, title, authors, journal_name, journal_issn,
    publication_date, publication_year, abstract, paper_type, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(openalex_id) DO UPDATE SET
    doi = excluded.doi,
    title = excluded.title,
    authors = excluded.authors,
    journal_name = excluded.journal_name,
    journal_issn = excluded.journal_issn,
    publication_date = excluded.publication_date,
    publication_year = excluded.publication_year,
    abstract = excluded.abstract,
    paper_type = excluded.paper_type,
    updated_at = CURRENT_TIMESTAMP
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def configure_logging() -> None:
    """Configure application logging to stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    """Return a configured session for OpenAlex requests."""
    session = requests.Session()
    user_agent = "AEA-Replication-Tracker/1.0"
    if OPENALEX_EMAIL:
        user_agent = f"{user_agent} ({OPENALEX_EMAIL})"
    else:
        LOGGER.warning("OPENALEX_EMAIL is not set; requests may not enter OpenAlex polite pool")
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": user_agent,
    })
    return session


def write_json(path: Path, payload: Any) -> None:
    """Write JSON to disk with deterministic formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def atomic_write_json(path: Path, payload: Any) -> None:
    """Write JSON atomically by renaming a temp file into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tf:
        json.dump(payload, tf, indent=2, sort_keys=True)
        tf.write("\n")
        temp_path = Path(tf.name)
    os.replace(temp_path, path)


# ---------------------------------------------------------------------------
# Data transforms
# ---------------------------------------------------------------------------


def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """Convert an OpenAlex abstract_inverted_index to plain text.

    The inverted index maps each word to a list of positions where it appears.
    We sort all (position, word) pairs by position and join with spaces.
    """
    if not inverted_index:
        return None

    pairs: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            pairs.append((pos, word))
    if not pairs:
        return None

    pairs.sort(key=lambda p: p[0])
    return " ".join(word for _, word in pairs)


def extract_openalex_id(url: str) -> str:
    """Strip the https://openalex.org/ prefix from an OpenAlex URL."""
    prefix = "https://openalex.org/"
    if url.startswith(prefix):
        return url[len(prefix):]
    return url


def normalize_doi(doi_url: str | None) -> str | None:
    """Strip https://doi.org/ prefix and lowercase; return None if missing."""
    if not doi_url:
        return None
    prefix = "https://doi.org/"
    if doi_url.startswith(prefix):
        return doi_url[len(prefix):].lower()
    return doi_url.lower()


def extract_authors(authorships: list[dict]) -> str:
    """Extract author names and first institution from authorships array.

    Returns a JSON string of [{name, institution}, ...].
    """
    authors: list[dict[str, str | None]] = []
    for authorship in authorships:
        author_obj = authorship.get("author", {}) or {}
        name = author_obj.get("display_name")
        if not name:
            continue

        institution: str | None = None
        institutions = authorship.get("institutions") or []
        if institutions:
            institution = institutions[0].get("display_name")

        authors.append({"name": name, "institution": institution})

    return json.dumps(authors, ensure_ascii=False)


# ---------------------------------------------------------------------------
# API interaction
# ---------------------------------------------------------------------------


class PaperFetchError(RuntimeError):
    """Raised when an API request fails after retries."""


def fetch_works_page(
    session: requests.Session,
    source_id: str,
    cursor: str,
    extra_filters: str | None = None,
) -> dict:
    """Fetch a single page of works from OpenAlex with retry logic."""
    url = f"{OPENALEX_BASE_URL}/works"

    filter_parts = [
        f"primary_location.source.id:{source_id}",
        "type:article",
    ]
    if extra_filters:
        filter_parts.append(extra_filters)

    params: dict[str, Any] = {
        "filter": ",".join(filter_parts),
        "sort": "publication_date:desc",
        "per-page": PER_PAGE,
        "cursor": cursor,
    }
    if OPENALEX_EMAIL:
        params["mailto"] = OPENALEX_EMAIL

    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if attempt > MAX_RETRIES:
                raise PaperFetchError(f"Request failed for source {source_id}: {exc}") from exc
            delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning(
                "Request error for source %s, attempt %s/%s: %s; retrying in %.1fs",
                source_id, attempt, MAX_RETRIES + 1, exc, delay,
            )
            time.sleep(delay)
            continue

        if response.status_code == 429 or 500 <= response.status_code < 600:
            if attempt > MAX_RETRIES:
                raise PaperFetchError(
                    f"OpenAlex returned HTTP {response.status_code} for source {source_id} after retries"
                )
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    delay = max(float(retry_after), 0.0)
                except ValueError:
                    delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            else:
                delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning(
                "OpenAlex returned HTTP %s for source %s, attempt %s/%s; retrying in %.1fs",
                response.status_code, source_id, attempt, MAX_RETRIES + 1, delay,
            )
            time.sleep(delay)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise PaperFetchError(
                f"OpenAlex returned HTTP {response.status_code} for source {source_id}"
            ) from exc

        return response.json()


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------


def upsert_papers(conn: sqlite3.Connection, papers: list[tuple]) -> int:
    """Batch upsert papers into the DB. Returns count of rows affected."""
    cursor = conn.cursor()
    count = 0
    for row in papers:
        try:
            cursor.execute(UPSERT_SQL, row)
            count += 1
        except sqlite3.IntegrityError as exc:
            # Handle potential doi UNIQUE conflicts across different openalex_ids
            LOGGER.warning("Skipping paper due to integrity error: %s (openalex_id=%s)", exc, row[0])
    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Page-by-page fetch and store loop
# ---------------------------------------------------------------------------


def transform_work(work: dict, journal_name: str, journal_issn: str) -> tuple | None:
    """Transform a single OpenAlex work into a DB row tuple.

    Returns None if the work should be skipped.
    """
    # Safety check: skip non-articles
    if work.get("type") != "article":
        LOGGER.debug("Skipping non-article: %s (type=%s)", work.get("id"), work.get("type"))
        return None

    openalex_id = extract_openalex_id(work.get("id", ""))
    if not openalex_id:
        LOGGER.warning("Skipping work with no OpenAlex ID")
        return None

    doi = normalize_doi(work.get("doi"))
    title = work.get("title")
    if title and EDITORIAL_TITLE_PATTERN.search(title):
        LOGGER.debug("Skipping editorial: %s", title)
        return None
    authors = extract_authors(work.get("authorships") or [])
    pub_date = work.get("publication_date")
    pub_year = work.get("publication_year")
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))
    paper_type = work.get("type")

    return (
        openalex_id, doi, title, authors, journal_name, journal_issn,
        pub_date, pub_year, abstract, paper_type,
    )


def fetch_and_store_works(
    session: requests.Session,
    conn: sqlite3.Connection,
    source_id: str,
    journal_name: str,
    journal_issn: str,
    extra_filters: str | None = None,
) -> int:
    """Cursor-paginate through all works for a journal, storing page-by-page.

    Returns total count of papers upserted.
    """
    raw_dir = RAW_PAPERS_DIR / journal_issn
    raw_dir.mkdir(parents=True, exist_ok=True)

    cursor = "*"
    page_num = 0
    total_stored = 0

    while True:
        page_num += 1
        data = fetch_works_page(session, source_id, cursor, extra_filters)

        # Save raw response
        write_json(raw_dir / f"page_{page_num:04d}.json", data)

        results = data.get("results", [])
        if not results:
            LOGGER.info("  No more results at page %d", page_num)
            break

        # Transform results into DB rows
        rows: list[tuple] = []
        for work in results:
            row = transform_work(work, journal_name, journal_issn)
            if row is not None:
                rows.append(row)

        # Upsert this page immediately — no accumulation
        if rows:
            stored = upsert_papers(conn, rows)
            total_stored += stored

        # Log progress every 5 pages
        if page_num % 5 == 0:
            meta = data.get("meta", {})
            total_count = meta.get("count", "?")
            LOGGER.info(
                "  Page %d: %d papers this page, %d total stored so far (API reports %s total)",
                page_num, len(rows), total_stored, total_count,
            )

        # Advance cursor
        meta = data.get("meta", {})
        next_cursor = meta.get("next_cursor")
        if not next_cursor:
            break
        cursor = next_cursor

        # Rate limit between requests
        time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    return total_stored


# ---------------------------------------------------------------------------
# Pipeline state (per-journal timestamps)
# ---------------------------------------------------------------------------


def load_pipeline_state() -> dict:
    """Load pipeline state from disk."""
    if PIPELINE_STATE_PATH.exists():
        try:
            return json.loads(PIPELINE_STATE_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning("Could not load pipeline state: %s; starting fresh", exc)
    return {}


def save_pipeline_state(state: dict) -> None:
    """Save pipeline state atomically."""
    atomic_write_json(PIPELINE_STATE_PATH, state)


def get_journal_last_fetched(state: dict, issn: str) -> str | None:
    """Get the last-fetched date for a specific journal."""
    return state.get("last_fetched", {}).get(issn)


def set_journal_last_fetched(state: dict, issn: str, date_str: str) -> None:
    """Update the last-fetched date for a specific journal."""
    if "last_fetched" not in state:
        state["last_fetched"] = {}
    state["last_fetched"][issn] = date_str


# ---------------------------------------------------------------------------
# Journal loading
# ---------------------------------------------------------------------------


def load_journals() -> list[dict]:
    """Load journal metadata from data/journals.json."""
    if not JOURNALS_PATH.exists():
        LOGGER.error("journals.json not found at %s — run 01_fetch_journals.py first", JOURNALS_PATH)
        sys.exit(1)

    journals = json.loads(JOURNALS_PATH.read_text(encoding="utf-8"))
    if not journals:
        LOGGER.error("journals.json is empty — run 01_fetch_journals.py first")
        sys.exit(1)

    LOGGER.info("Loaded %d journals from %s", len(journals), JOURNALS_PATH)
    return journals


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the papers fetch pipeline."""
    configure_logging()

    parser = argparse.ArgumentParser(description="Fetch AEA journal papers from OpenAlex")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Re-fetch all papers (ignore incremental state)",
    )
    parser.add_argument(
        "--only",
        default=None,
        help="Comma-separated list of ISSNs to fetch (others skipped)",
    )
    args = parser.parse_args()
    only_issns: set[str] | None = (
        {s.strip() for s in args.only.split(",") if s.strip()}
        if args.only
        else None
    )

    journals = load_journals()
    db_path = init_db()
    conn = get_connection(db_path)
    state = load_pipeline_state()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if args.full:
        LOGGER.info("Running FULL fetch — ignoring incremental state")
        # Clear raw papers directory for clean re-fetch
        if RAW_PAPERS_DIR.exists():
            shutil.rmtree(RAW_PAPERS_DIR)
        RAW_PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    else:
        LOGGER.info("Running INCREMENTAL fetch")

    total_papers = 0
    failures: list[str] = []

    with build_session() as session:
        for journal in journals:
            source_id = journal.get("openalex_id")
            display_name = journal.get("display_name", "Unknown")
            issn = journal.get("issn", "unknown")

            if not source_id:
                LOGGER.warning("Skipping %s — no openalex_id", display_name)
                continue

            if only_issns is not None and issn not in only_issns:
                continue

            # Build extra filters: policy window + incremental timestamp
            extra_parts: list[str] = []
            policy_year = journal.get("policy_start_year")
            if policy_year:
                extra_parts.append(f"from_publication_date:{int(policy_year)}-01-01")

            if not args.full:
                last_fetched = get_journal_last_fetched(state, issn)
                if last_fetched:
                    extra_parts.append(f"from_updated_date:{last_fetched}")
                    LOGGER.info(
                        "Fetching %s (%s) — incremental from %s%s",
                        display_name, issn, last_fetched,
                        f", policy_start={policy_year}" if policy_year else "",
                    )
                else:
                    LOGGER.info(
                        "Fetching %s (%s) — no prior state, doing full fetch%s",
                        display_name, issn,
                        f" (policy_start={policy_year})" if policy_year else "",
                    )
            else:
                LOGGER.info(
                    "Fetching %s (%s) — full mode%s",
                    display_name, issn,
                    f" (policy_start={policy_year})" if policy_year else "",
                )

            extra_filter: str | None = ",".join(extra_parts) if extra_parts else None

            try:
                count = fetch_and_store_works(
                    session=session,
                    conn=conn,
                    source_id=source_id,
                    journal_name=display_name,
                    journal_issn=issn,
                    extra_filters=extra_filter,
                )
                total_papers += count
                LOGGER.info("  ✓ Stored %d papers for %s", count, display_name)

                # Update per-journal timestamp only after successful completion
                set_journal_last_fetched(state, issn, today)
                save_pipeline_state(state)

            except PaperFetchError as exc:
                failures.append(issn)
                LOGGER.error("  ✗ Failed to fetch %s (%s): %s", display_name, issn, exc)
            except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                failures.append(issn)
                LOGGER.error("  ✗ Error processing %s (%s): %s", display_name, issn, exc)

    conn.close()

    LOGGER.info("=" * 60)
    LOGGER.info("DONE — Total papers stored: %d", total_papers)
    if failures:
        LOGGER.error("Failed journals: %s", ", ".join(failures))
        return 1

    LOGGER.info("All journals fetched successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
