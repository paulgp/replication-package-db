"""Probe the Harvard RFS Dataverse collection and match deposits to RFS papers.

Enumerates https://dataverse.harvard.edu/dataverse/rfs, pulls dataset metadata
for each entry, and attempts to link deposits to papers in the local DB by:
  1. An explicit DOI found in the dataset citation (rare), else
  2. Fuzzy title match against RFS papers in the `papers` table.

Populates `repo_mappings` rows with source='rfs_dataverse'. Also writes a
summary report to data/raw/rfs_dataverse_report.json for manual review.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATA_DIR, RAW_DIR
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import DATA_DIR, RAW_DIR
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

DATAVERSE_BASE = "https://dataverse.harvard.edu"
COLLECTION_ALIAS = "rfs"
REQUEST_TIMEOUT = 30
RATE_LIMIT_SLEEP = 0.25  # ~4 req/s, polite
TITLE_MATCH_THRESHOLD = 0.85
RFS_ISSN = "0893-9454"

RAW_OUT_DIR = RAW_DIR / "rfs_dataverse"

# Patterns used to strip replication boilerplate from dataset titles before
# matching them against paper titles.
DATASET_TITLE_STRIP = re.compile(
    r"^(?:"
    r"(?:matlab|python|stata|r|julia)\s+replication\s+files?\s+for\s*|"
    r"data\s+samples?\s+and\s+replication(?:\s+of\s+analysis)?\s+codes?\s+for\s*|"
    r"replication\s+for\s*|"
    r"replication\s+(?:data|code|codes|package|files?|archive|materials?)"
    r"(?:\s+and\s+(?:data|code|codes|pseudo-data))?\s*(?:for)?[:\-]?\s*"
    r")+",
    re.IGNORECASE,
)
TITLE_NOISE = re.compile(r'["\u201c\u201d\u2018\u2019\'`]|<[^>]+>')


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "AEA-Replication-Tracker/1.0 (paulgp@gmail.com)",
    })
    return session


def list_collection(session: requests.Session) -> list[dict[str, Any]]:
    """Return all dataset entries in the RFS Dataverse collection."""
    url = f"{DATAVERSE_BASE}/api/dataverses/{COLLECTION_ALIAS}/contents"
    LOGGER.info("Listing collection %s", COLLECTION_ALIAS)
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    datasets = [d for d in payload.get("data", []) if d.get("type") == "dataset"]
    LOGGER.info("Collection has %d datasets", len(datasets))
    return datasets


def fetch_dataset(session: requests.Session, pid: str) -> dict[str, Any] | None:
    """Fetch a single dataset's full metadata by persistent ID."""
    url = f"{DATAVERSE_BASE}/api/datasets/:persistentId"
    params = {"persistentId": pid}
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json().get("data")


def extract_citation_fields(dataset: dict[str, Any]) -> dict[str, Any]:
    """Flatten the citation metadata block into a {typeName: value} dict."""
    latest = dataset.get("latestVersion") or {}
    blocks = latest.get("metadataBlocks") or {}
    citation = blocks.get("citation") or {}
    fields: dict[str, Any] = {}
    for f in citation.get("fields", []):
        fields[f["typeName"]] = f.get("value")
    return fields


DOI_RE = re.compile(r"10\.[0-9]{4,9}/[-._;()/:A-Za-z0-9]+")
RFS_DOI_HOSTS = re.compile(r"(?i)rfs/|rof/|10\.1093/rfs|10\.1093/rof|oup\.com/rfs")


def extract_paper_doi(fields: dict[str, Any]) -> str | None:
    """Try to pull an RFS paper DOI out of the dataset citation fields."""
    haystack_parts: list[str] = []

    pub = fields.get("publication")
    if isinstance(pub, list):
        for entry in pub:
            for sub in entry.values():
                v = sub.get("value")
                if isinstance(v, str):
                    haystack_parts.append(v)

    for key in ("alternativeURL", "notesText"):
        v = fields.get(key)
        if isinstance(v, str):
            haystack_parts.append(v)

    for h in haystack_parts:
        for m in DOI_RE.finditer(h):
            doi = m.group(0).rstrip(".,;)")
            low = doi.lower()
            if "10.1093/rfs" in low or "10.1093/rof" in low:
                return low
            # Any DOI at all, if the surrounding hints look RFS-y
            if RFS_DOI_HOSTS.search(h):
                return low
    return None


def normalize_title(title: str) -> str:
    """Normalize a title for fuzzy comparison: unicode fold, strip punct, lowercase."""
    t = unicodedata.normalize("NFKD", title)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = TITLE_NOISE.sub(" ", t)
    t = DATASET_TITLE_STRIP.sub("", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    # drop trailing volume/issue/year annotations sometimes attached
    t = re.sub(r"\s*[\[\(]?(?:forthcoming|accepted|in\s+press)[^\]\)]*[\]\)]?\s*$", "", t)
    return t


def score_title(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def load_rfs_papers(conn) -> list[tuple[str, str, str]]:
    """Return (doi, title, normalized_title) tuples for RFS papers in the DB."""
    rows = conn.execute(
        "SELECT doi, title FROM papers WHERE journal_issn = ? AND doi IS NOT NULL AND title IS NOT NULL",
        (RFS_ISSN,),
    ).fetchall()
    out = []
    for r in rows:
        out.append((r["doi"], r["title"], normalize_title(r["title"])))
    return out


SUBSTRING_MIN_LEN = 25


def fuzzy_match(
    candidate: str, papers: list[tuple[str, str, str]]
) -> tuple[str, float, str] | None:
    """Return (paper_doi, score, method) for the best match or None.

    Tries SequenceMatcher ratio first, falling back to substring containment
    when the normalized paper title is long enough to be distinctive and is
    a substring of the dataset title (or vice versa).
    """
    best_ratio: tuple[str, float] | None = None
    substring_hit: str | None = None
    for doi, _title, norm in papers:
        if not norm:
            continue
        s = score_title(candidate, norm)
        if best_ratio is None or s > best_ratio[1]:
            best_ratio = (doi, s)
        if substring_hit is None and len(norm) >= SUBSTRING_MIN_LEN and (
            norm in candidate or candidate in norm
        ):
            substring_hit = doi

    if best_ratio and best_ratio[1] >= TITLE_MATCH_THRESHOLD:
        return (best_ratio[0], best_ratio[1], "fuzzy_title")
    if substring_hit:
        return (substring_hit, 1.0, "substring_title")
    return None


def store_mapping(conn, paper_doi: str, repo_doi: str) -> None:
    conn.execute(
        """
        INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, repo_host, source)
        VALUES (?, ?, NULL, 'dataverse', 'rfs_dataverse')
        """,
        (paper_doi, repo_doi),
    )


def main() -> int:
    configure_logging()
    RAW_OUT_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    rfs_papers = load_rfs_papers(conn)
    LOGGER.info("Loaded %d RFS papers from DB", len(rfs_papers))
    if not rfs_papers:
        LOGGER.error("No RFS papers in DB — run 02_fetch_papers.py first")
        return 1

    # Clear existing rfs_dataverse rows so this run is idempotent.
    conn.execute("DELETE FROM repo_mappings WHERE source = 'rfs_dataverse'")
    conn.commit()

    results: list[dict[str, Any]] = []
    matched = 0
    no_match = 0
    doi_match = 0
    fuzzy_match_count = 0
    substring_match_count = 0

    with build_session() as session:
        datasets = list_collection(session)
        # Cache the collection listing
        (RAW_OUT_DIR / "collection_contents.json").write_text(
            json.dumps(datasets, indent=2), encoding="utf-8"
        )

        for i, d in enumerate(datasets, 1):
            pid = f"doi:{d['authority']}/{d['identifier']}"
            repo_doi = f"{d['authority']}/{d['identifier']}".lower()

            try:
                full = fetch_dataset(session, pid)
            except requests.RequestException as exc:
                LOGGER.warning("Fetch failed for %s: %s", pid, exc)
                time.sleep(RATE_LIMIT_SLEEP)
                continue

            time.sleep(RATE_LIMIT_SLEEP)
            if not full:
                continue

            fields = extract_citation_fields(full)
            dataset_title = fields.get("title") or ""
            explicit_doi = extract_paper_doi(fields)

            result: dict[str, Any] = {
                "dataset_doi": repo_doi,
                "dataset_title": dataset_title,
                "dataset_pub_date": d.get("publicationDate"),
                "explicit_paper_doi": explicit_doi,
                "matched_paper_doi": None,
                "match_method": None,
                "match_score": None,
            }

            match_doi: str | None = None
            if explicit_doi:
                # Trust DOI if it matches a paper we have
                for pdoi, _t, _n in rfs_papers:
                    if pdoi == explicit_doi:
                        match_doi = pdoi
                        result["match_method"] = "doi"
                        result["match_score"] = 1.0
                        doi_match += 1
                        break

            if match_doi is None and dataset_title:
                norm = normalize_title(dataset_title)
                fm = fuzzy_match(norm, rfs_papers)
                if fm:
                    match_doi, score, method = fm
                    result["match_method"] = method
                    result["match_score"] = round(score, 3)
                    if method == "substring_title":
                        substring_match_count += 1
                    else:
                        fuzzy_match_count += 1

            if match_doi:
                store_mapping(conn, match_doi, repo_doi)
                result["matched_paper_doi"] = match_doi
                matched += 1
            else:
                no_match += 1

            results.append(result)

            if i % 25 == 0:
                LOGGER.info(
                    "  progress: %d/%d datasets; matched=%d (doi=%d, fuzzy=%d, substr=%d), no_match=%d",
                    i, len(datasets), matched, doi_match, fuzzy_match_count,
                    substring_match_count, no_match,
                )
                conn.commit()

    conn.commit()

    report = {
        "total_datasets": len(datasets),
        "matched": matched,
        "matched_via_doi": doi_match,
        "matched_via_fuzzy_title": fuzzy_match_count,
        "matched_via_substring_title": substring_match_count,
        "no_match": no_match,
        "title_match_threshold": TITLE_MATCH_THRESHOLD,
        "substring_min_len": SUBSTRING_MIN_LEN,
        "entries": results,
    }
    (RAW_OUT_DIR / "match_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Datasets probed: %d", len(datasets))
    LOGGER.info(
        "Matched: %d (DOI=%d, fuzzy_title=%d, substring=%d)",
        matched, doi_match, fuzzy_match_count, substring_match_count,
    )
    LOGGER.info("No match: %d", no_match)
    LOGGER.info("Match rate: %.1f%%", 100 * matched / max(len(datasets), 1))
    LOGGER.info("Report written to %s", RAW_OUT_DIR / "match_report.json")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
