"""Recover missing paper->repo mappings via DataCite title search.

Many openICPSR repos have no `IsSupplementTo` relation in DataCite, so the
bulk relation lookup (04_datacite_lookup.py) misses them. This script looks
up each unmapped paper by searching DataCite for openICPSR repos whose title
matches the paper title.

Usage:
    python scripts/03d_datacite_title_search.py
    python scripts/03d_datacite_title_search.py --min-year 2005 --limit 100
    python scripts/03d_datacite_title_search.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATACITE_BASE_URL, OPENALEX_EMAIL
    from db import get_connection, init_db
except ImportError:
    from scripts.config import DATACITE_BASE_URL, OPENALEX_EMAIL
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

RATE_LIMIT_SLEEP = 0.3
REQUEST_TIMEOUT = 30
SIMILARITY_THRESHOLD = 0.72

ICPSR_RE = re.compile(r"10\.3886/[eE](\d+)")
STOPWORDS = {
    "the", "and", "for", "from", "with", "that", "this", "are", "not", "but",
    "evidence", "data", "effect", "effects", "study", "paper", "using",
    "empirical", "analysis", "impact", "role", "case", "versus",
    "some", "into", "how", "why", "when", "where",
}
REPO_PREFIX_RE = re.compile(
    r"^(data and code for|code and data for|code for|data for|"
    r"replication (code|data|files|package) for|"
    r"data and code|code and data|"
    r"programs? and data for|programs? for|software for)\s*[:\-]?\s*",
    re.IGNORECASE,
)
QUOTES_RE = re.compile(r'[“”"\'‘’‘’]')


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    s = requests.Session()
    contact = OPENALEX_EMAIL or "unknown@example.com"
    s.headers.update({
        "Accept": "application/vnd.api+json",
        "User-Agent": f"AEA-Replication-Tracker/1.0 (contact: {contact})",
    })
    return s


def _main_title(t: str) -> str:
    """Strip the main title at the first `:`, `?`, or `;`."""
    for sep in (":", "?", ";"):
        if sep in t:
            t = t.split(sep, 1)[0]
    return t.strip()


def normalize_title(t: str) -> str:
    """Lowercase, strip repo-prefixes, strip quotes and punctuation."""
    t = QUOTES_RE.sub("", t)
    t = REPO_PREFIX_RE.sub("", t).strip()
    t = _main_title(t)
    return re.sub(r"\s+", " ", t).lower()


def extract_keywords(title: str, max_n: int = 5) -> list[str]:
    main = _main_title(title)
    words = re.findall(r"[a-zA-Z]+", main)
    distinctive = [w for w in words if len(w) > 3 and w.lower() not in STOPWORDS]
    return distinctive[:max_n]


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio()


def _safe_get(session: requests.Session, url: str, params: dict | None = None) -> dict | None:
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if attempt == 2:
                LOGGER.warning("DataCite request failed after retries: %s", exc)
                return None
            time.sleep(2 ** attempt)
    return None


def search_datacite_by_title(
    session: requests.Session, title: str
) -> list[dict[str, Any]]:
    """Try progressively broader DataCite queries for a paper title."""
    # Strategy 1: exact phrase (main title before colon/question/semicolon)
    main = _main_title(title)
    q1 = f'prefix:10.3886 AND titles.title:"{main}"'
    data = _safe_get(
        session, f"{DATACITE_BASE_URL}/dois",
        {"query": q1, "page[size]": 5},
    )
    hits = (data or {}).get("data", [])
    if hits:
        return hits

    # Strategy 2: AND of distinctive keywords
    kws = extract_keywords(title, max_n=5)
    if len(kws) >= 2:
        kw_query = " AND ".join(kws)
        q2 = f"prefix:10.3886 AND titles.title:({kw_query})"
        data = _safe_get(
            session, f"{DATACITE_BASE_URL}/dois",
            {"query": q2, "page[size]": 10},
        )
        hits = (data or {}).get("data", [])
        if hits:
            return hits

    # Strategy 3: AND of fewer, more distinctive keywords
    if len(kws) >= 3:
        kw_query = " AND ".join(kws[:3])
        q3 = f"prefix:10.3886 AND titles.title:({kw_query})"
        data = _safe_get(
            session, f"{DATACITE_BASE_URL}/dois",
            {"query": q3, "page[size]": 15},
        )
        return (data or {}).get("data", [])

    return []


def pick_best_match(
    paper_title: str,
    candidates: list[dict[str, Any]],
    threshold: float = SIMILARITY_THRESHOLD,
) -> tuple[str, str, float] | None:
    """Return (repo_doi, icpsr_project_id, score) for the best-matching candidate."""
    best: tuple[str, str, float] | None = None
    for c in candidates:
        attrs = c.get("attributes") or {}
        repo_doi = (c.get("id") or attrs.get("doi") or "").lower()
        m = ICPSR_RE.search(repo_doi)
        if not m:
            continue
        project_id = m.group(1)

        for t in attrs.get("titles") or []:
            t_text = t.get("title") or ""
            if not t_text:
                continue
            score = similarity(paper_title, t_text)
            if score >= threshold and (best is None or score > best[2]):
                best = (repo_doi, project_id, score)

    return best


def version_rank(repo_doi: str) -> tuple[int, str]:
    """Prefer versioned DOIs over parent DOIs."""
    suffix = repo_doi.split("10.3886/", 1)[-1]
    if re.fullmatch(r"e\d+v\d+", suffix):
        return (0, suffix)
    if re.fullmatch(r"e\d+", suffix):
        return (1, suffix)
    return (2, suffix)


def get_unmapped_papers(conn, min_year: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT p.doi, p.title, p.publication_year
        FROM papers p
        WHERE p.publication_year >= ?
          AND p.title IS NOT NULL
          AND p.journal_name IN (
              'American Economic Review',
              'American Economic Review Insights',
              'American Economic Journal Applied Economics',
              'American Economic Journal Economic Policy',
              'American Economic Journal Macroeconomics',
              'American Economic Journal Microeconomics'
          )
          AND NOT EXISTS (
              SELECT 1 FROM repo_mappings rm WHERE rm.paper_doi = p.doi
          )
        ORDER BY p.publication_year DESC, p.doi
        """,
        (min_year,),
    ).fetchall()
    return [dict(r) for r in rows]


def insert_mapping(conn, paper_doi: str, repo_doi: str, project_id: str) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO repo_mappings
                (paper_doi, repo_doi, icpsr_project_id, source, repo_host)
            VALUES (?, ?, ?, 'datacite_title', 'openicpsr')
            """,
            (paper_doi, repo_doi, project_id),
        )


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Recover paper->repo mappings via DataCite title search")
    parser.add_argument("--min-year", type=int, default=2005)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--threshold", type=float, default=SIMILARITY_THRESHOLD)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_connection(init_db())
    try:
        papers = get_unmapped_papers(conn, args.min_year)
        if args.limit:
            papers = papers[: args.limit]
        LOGGER.info("Unmapped papers to check: %d", len(papers))

        stats = {"searched": 0, "matched": 0, "inserted": 0, "miss": 0}
        miss_log = []

        with build_session() as session:
            for idx, p in enumerate(papers, 1):
                candidates = search_datacite_by_title(session, p["title"])
                stats["searched"] += 1
                time.sleep(RATE_LIMIT_SLEEP)

                if not candidates:
                    stats["miss"] += 1
                    miss_log.append({"doi": p["doi"], "title": p["title"], "reason": "no_candidates"})
                    continue

                best = pick_best_match(p["title"], candidates, threshold=args.threshold)
                if not best:
                    stats["miss"] += 1
                    best_score = max(
                        (similarity(p["title"], t.get("title", ""))
                         for c in candidates
                         for t in (c.get("attributes") or {}).get("titles", [])),
                        default=0.0,
                    )
                    miss_log.append({
                        "doi": p["doi"], "title": p["title"],
                        "reason": "low_similarity", "best_score": round(best_score, 2),
                    })
                    continue

                repo_doi, project_id, score = best
                stats["matched"] += 1

                if args.dry_run:
                    LOGGER.info(
                        "[dry] %s -> openicpsr/%s (score=%.2f)",
                        p["doi"], project_id, score,
                    )
                else:
                    try:
                        insert_mapping(conn, p["doi"], repo_doi, project_id)
                        stats["inserted"] += 1
                    except Exception as exc:
                        LOGGER.warning("Insert failed for %s: %s", p["doi"], exc)

                if idx % 50 == 0:
                    LOGGER.info(
                        "Progress: %d/%d | matched=%d miss=%d",
                        idx, len(papers), stats["matched"], stats["miss"],
                    )

        LOGGER.info("=" * 60)
        LOGGER.info("DataCite title search complete%s", " (dry run)" if args.dry_run else "")
        LOGGER.info("=" * 60)
        for k, v in stats.items():
            LOGGER.info("  %-12s %d", k, v)

        # Save miss log for review
        miss_path = Path(__file__).parent.parent / "data" / "datacite_title_misses.json"
        miss_path.write_text(json.dumps(miss_log, indent=2))
        LOGGER.info("Miss log -> %s (%d entries)", miss_path, len(miss_log))

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
