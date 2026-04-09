"""CrossRef fallback lookup: mine paper references for self-citing openICPSR DOIs."""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import CROSSREF_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR, RATE_LIMITS
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import CROSSREF_BASE_URL, DATA_DIR, OPENALEX_EMAIL, RAW_DIR, RATE_LIMITS
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RATE_LIMIT_SLEEP = 0.125  # ~8 req/s, under CrossRef polite pool limit of 10 req/s
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.0

CROSSREF_RAW_DIR = RAW_DIR / "crossref"

# Space-tolerant regex for ICPSR DOIs (e.g. "10.3886/ E192924V1")
ICPSR_DOI_RE = re.compile(
    r"10\.3886/\s*[eE](\d+)(?:\s*[vV]\s*(\d+))?",
)

# Phrases that indicate a self-referencing replication package citation
SELF_REF_PHRASES = (
    "data and code for",
    "replication package",
    "replication data",
)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def configure_logging() -> None:
    """Configure application logging to stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    """Return a session configured for the CrossRef polite pool."""
    session = requests.Session()
    contact = OPENALEX_EMAIL or "unknown@example.com"
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": f"ReplicationTracker/1.0 (mailto:{contact})",
        }
    )
    return session


def fetch_crossref_work(session: requests.Session, doi: str) -> dict[str, Any] | None:
    """Fetch a single work from CrossRef by DOI.

    Returns the ``message`` dict on success, *None* on 404 or exhausted retries.
    Retries with exponential backoff on 429 / 5xx, honouring ``Retry-After``.
    """
    url = f"{CROSSREF_BASE_URL}/works/{requests.utils.quote(doi, safe='')}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

            if resp.status_code == 404:
                return None  # many old papers – skip silently

            if resp.status_code in {429, 500, 502, 503, 504}:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    sleep_s = float(retry_after)
                else:
                    sleep_s = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    LOGGER.warning(
                        "CrossRef %d for %s (attempt %d/%d); retrying in %.1fs",
                        resp.status_code,
                        doi,
                        attempt,
                        MAX_RETRIES,
                        sleep_s,
                    )
                    time.sleep(sleep_s)
                    continue
                else:
                    LOGGER.error(
                        "CrossRef %d for %s after %d attempts; skipping",
                        resp.status_code,
                        doi,
                        MAX_RETRIES,
                    )
                    return None

            resp.raise_for_status()
            payload = resp.json()
            return payload.get("message")

        except requests.RequestException as exc:
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                LOGGER.warning(
                    "CrossRef request error for %s (attempt %d/%d): %s; retrying in %.1fs",
                    doi,
                    attempt,
                    MAX_RETRIES,
                    exc,
                    sleep_s,
                )
                time.sleep(sleep_s)
            else:
                LOGGER.error(
                    "CrossRef request failed for %s after %d attempts: %s; skipping",
                    doi,
                    MAX_RETRIES,
                    exc,
                )
                return None

    return None  # unreachable safety net


# ---------------------------------------------------------------------------
# Reference mining
# ---------------------------------------------------------------------------
def extract_repo_from_references(
    message: dict[str, Any],
) -> tuple[str, str, int] | None:
    """Extract a self-referencing ICPSR DOI from the CrossRef reference list.

    Returns ``(repo_doi, icpsr_project_id, tier)`` or *None*.

    Tier 1 – author family name matched in reference text.
    Tier 2 – phrase-only match ("data and code for" etc.) without author name.
    """
    authors = message.get("author") or []
    first_family = ""
    if authors and isinstance(authors[0], dict):
        first_family = (authors[0].get("family") or "").strip().lower()

    references = message.get("reference") or []
    best: tuple[str, str, int] | None = None

    for ref in references:
        text = (ref.get("unstructured") or "").lower()
        if "10.3886" not in text:
            continue

        m = ICPSR_DOI_RE.search(text)
        if not m:
            continue

        project_id = m.group(1)
        version = m.group(2)
        if version:
            repo_doi = f"10.3886/E{project_id}V{version}"
        else:
            repo_doi = f"10.3886/E{project_id}"

        # Tier 1: author name match (word-boundary to avoid short-name false positives)
        if first_family and re.search(r'\b' + re.escape(first_family) + r'\b', text):
            return (repo_doi, project_id, 1)

        # Tier 2: phrase match
        if any(phrase in text for phrase in SELF_REF_PHRASES):
            # Keep searching in case a tier 1 match appears later
            if best is None:
                best = (repo_doi, project_id, 2)

    return best


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_unmatched_dois(conn) -> list[str]:
    """Return DOIs of papers not yet in repo_mappings."""
    rows = conn.execute(
        """
        SELECT p.doi
        FROM papers p
        WHERE p.doi IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM repo_mappings rm WHERE rm.paper_doi = p.doi
          )
        ORDER BY p.doi
        """
    ).fetchall()
    return [r[0] for r in rows]


def doi_to_cache_filename(doi: str) -> str:
    """Convert a DOI to a filesystem-safe filename."""
    return doi.replace("/", "_") + ".json"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    configure_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CROSSREF_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        unmatched = get_unmatched_dois(conn)
        total = len(unmatched)
        LOGGER.info("Unmatched papers with DOI: %d", total)

        session = build_session()
        tier1_matches: list[dict[str, str]] = []
        tier2_matches: list[dict[str, str]] = []
        skipped_404 = 0
        skipped_error = 0
        cached_hits = 0

        for idx, doi in enumerate(unmatched, 1):
            # ---------- cache check ----------
            cache_path = CROSSREF_RAW_DIR / doi_to_cache_filename(doi)
            message: dict[str, Any] | None = None

            if cache_path.exists():
                cached_hits += 1
                try:
                    raw = json.loads(cache_path.read_text(encoding="utf-8"))
                    message = raw  # we cache the message dict directly
                except (json.JSONDecodeError, OSError):
                    message = None  # re-fetch on corrupt cache
            
            if message is None and not cache_path.exists():
                # Only sleep + fetch if we haven't loaded from cache
                if idx > 1:
                    time.sleep(RATE_LIMIT_SLEEP)
                message = fetch_crossref_work(session, doi)
                if message is None:
                    skipped_404 += 1
                else:
                    # Cache the raw response
                    try:
                        cache_path.write_text(
                            json.dumps(message, indent=2, sort_keys=True),
                            encoding="utf-8",
                        )
                    except OSError as exc:
                        LOGGER.warning("Failed to cache response for %s: %s", doi, exc)
            elif message is None:
                # cache file existed but was corrupt; re-fetch
                if idx > 1:
                    time.sleep(RATE_LIMIT_SLEEP)
                message = fetch_crossref_work(session, doi)
                if message is None:
                    skipped_404 += 1
                else:
                    try:
                        cache_path.write_text(
                            json.dumps(message, indent=2, sort_keys=True),
                            encoding="utf-8",
                        )
                    except OSError:
                        pass

            if message is None:
                if idx % 100 == 0:
                    LOGGER.info(
                        "Progress: %d/%d queried | T1=%d T2=%d | 404s=%d",
                        idx, total, len(tier1_matches), len(tier2_matches), skipped_404,
                    )
                continue

            # ---------- extract ----------
            result = extract_repo_from_references(message)
            if result:
                repo_doi, project_id, tier = result
                mapping = {
                    "paper_doi": doi,
                    "repo_doi": repo_doi,
                    "icpsr_project_id": project_id,
                }
                if tier == 1:
                    tier1_matches.append(mapping)
                else:
                    tier2_matches.append(mapping)

            # ---------- progress ----------
            if idx % 100 == 0:
                LOGGER.info(
                    "Progress: %d/%d queried | T1=%d T2=%d | 404s=%d",
                    idx, total, len(tier1_matches), len(tier2_matches), skipped_404,
                )

        LOGGER.info("Querying complete. Inserting matches...")

        # ---------- insert tier 1 ----------
        t1_inserted = 0
        with conn:
            for m in tier1_matches:
                cur = conn.execute(
                    """
                    INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, source)
                    SELECT ?, ?, ?, 'crossref'
                    WHERE EXISTS (SELECT 1 FROM papers WHERE doi = ?)
                      AND NOT EXISTS (SELECT 1 FROM repo_mappings WHERE paper_doi = ?)
                    """,
                    (m["paper_doi"], m["repo_doi"], m["icpsr_project_id"],
                     m["paper_doi"], m["paper_doi"]),
                )
                t1_inserted += cur.rowcount

        # ---------- insert tier 2 ----------
        t2_inserted = 0
        with conn:
            for m in tier2_matches:
                cur = conn.execute(
                    """
                    INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, source)
                    SELECT ?, ?, ?, 'crossref'
                    WHERE EXISTS (SELECT 1 FROM papers WHERE doi = ?)
                      AND NOT EXISTS (SELECT 1 FROM repo_mappings WHERE paper_doi = ?)
                    """,
                    (m["paper_doi"], m["repo_doi"], m["icpsr_project_id"],
                     m["paper_doi"], m["paper_doi"]),
                )
                t2_inserted += cur.rowcount

        # ---------- summary ----------
        total_inserted = t1_inserted + t2_inserted

        total_papers = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        total_mapped = conn.execute(
            "SELECT COUNT(DISTINCT paper_doi) FROM repo_mappings"
        ).fetchone()[0]
        overall_rate = (total_mapped / total_papers * 100) if total_papers > 0 else 0.0

        # Per-source breakdown
        source_counts = conn.execute(
            "SELECT source, COUNT(*) FROM repo_mappings GROUP BY source ORDER BY source"
        ).fetchall()

        LOGGER.info("=" * 60)
        LOGGER.info("CrossRef lookup complete")
        LOGGER.info("=" * 60)
        LOGGER.info("Total DOIs queried:       %d", total)
        LOGGER.info("404 / not found:          %d", skipped_404)
        LOGGER.info("Served from cache:        %d", cached_hits)
        LOGGER.info("-" * 40)
        LOGGER.info("Tier 1 matches (author):  %d (inserted %d)", len(tier1_matches), t1_inserted)
        LOGGER.info("Tier 2 matches (phrase):  %d (inserted %d)", len(tier2_matches), t2_inserted)
        LOGGER.info("Total new inserts:        %d", total_inserted)
        LOGGER.info("-" * 40)
        LOGGER.info("Repo mappings by source:")
        for row in source_counts:
            LOGGER.info("  %-20s %d", row[0], row[1])
        LOGGER.info("-" * 40)
        LOGGER.info("Total mapped papers:      %d / %d (%.1f%%)", total_mapped, total_papers, overall_rate)
        LOGGER.info("Cached raw responses:     %s", CROSSREF_RAW_DIR)

        # Log tier-2 matches for spot-checking
        if tier2_matches:
            LOGGER.info("-" * 40)
            LOGGER.info("Tier 2 (phrase-only) matches for spot-checking:")
            for m in tier2_matches[:20]:
                LOGGER.info("  %s -> %s", m["paper_doi"], m["repo_doi"])
            if len(tier2_matches) > 20:
                LOGGER.info("  ... and %d more", len(tier2_matches) - 20)

        return 0

    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        return 130
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
