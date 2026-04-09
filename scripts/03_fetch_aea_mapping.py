"""Download the AEA DOI-to-openICPSR mapping CSV and populate repo_mappings."""

from __future__ import annotations

import csv
import logging
import re
import sys
from io import StringIO
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATA_DIR, RAW_DIR
    from db import get_connection, init_db
except ImportError:  # pragma: no cover - supports direct script execution and package imports
    from scripts.config import DATA_DIR, RAW_DIR
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)
CSV_URL = (
    "https://raw.githubusercontent.com/AEADataEditor/aea-supplement-migration/"
    "master/data/generated/table.aea.icpsr.mapping.csv"
)
RAW_CACHE_PATH = RAW_DIR / "aea_mapping.csv"
REQUEST_TIMEOUT_SECONDS = 30
ICPSR_PROJECT_ID_RE = re.compile(r"10\.3886/[Ee](\d+)[Vv]\d+")
DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
)


def configure_logging() -> None:
    """Configure application logging to stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def build_session() -> requests.Session:
    """Return a configured session for downloading the mapping CSV."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "text/csv, text/plain;q=0.9, */*;q=0.8",
            "User-Agent": "AEA-Replication-Tracker/1.0",
        }
    )
    return session


def download_csv(session: requests.Session) -> str:
    """Download the mapping CSV and cache it locally."""
    LOGGER.info("Downloading AEA mapping CSV from %s", CSV_URL)
    response = session.get(CSV_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    csv_text = response.text
    RAW_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_CACHE_PATH.write_text(csv_text, encoding="utf-8")
    LOGGER.info("Cached AEA mapping CSV to %s", RAW_CACHE_PATH)
    return csv_text


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


def extract_icpsr_project_id(icpsr_doi: str | None) -> str | None:
    """Extract the openICPSR project id from an ICPSR DOI."""
    if not icpsr_doi:
        return None

    match = ICPSR_PROJECT_ID_RE.search(icpsr_doi.strip())
    if not match:
        return None
    return match.group(1)


def parse_csv(csv_text: str) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Parse the CSV into normalized mapping rows plus parsing stats."""
    reader = csv.DictReader(StringIO(csv_text))
    fieldnames = reader.fieldnames or []
    if "doi" not in fieldnames or "icpsr_doi" not in fieldnames:
        raise ValueError(f"Expected CSV columns 'doi' and 'icpsr_doi'; found {fieldnames}")

    mappings: list[dict[str, str]] = []
    stats = {
        "total_rows": 0,
        "invalid_doi_rows": 0,
        "invalid_icpsr_rows": 0,
        "valid_rows": 0,
    }

    for row in reader:
        stats["total_rows"] += 1

        paper_doi = normalize_doi(row.get("doi"))
        repo_doi = normalize_doi(row.get("icpsr_doi"))
        icpsr_project_id = extract_icpsr_project_id(repo_doi)

        if not paper_doi:
            stats["invalid_doi_rows"] += 1
            continue
        if not repo_doi or not icpsr_project_id:
            stats["invalid_icpsr_rows"] += 1
            continue

        mappings.append(
            {
                "paper_doi": paper_doi,
                "repo_doi": repo_doi,
                "icpsr_project_id": icpsr_project_id,
            }
        )
        stats["valid_rows"] += 1

    return mappings, stats


def store_mappings(conn, mappings: list[dict[str, str]]) -> dict[str, int]:
    """Atomically replace AEA mappings with FK-safe rows only."""
    existing_paper_dois = {
        row[0]
        for row in conn.execute("SELECT doi FROM papers WHERE doi IS NOT NULL")
    }

    matched_rows = [
        (mapping["paper_doi"], mapping["repo_doi"], mapping["icpsr_project_id"])
        for mapping in mappings
        if mapping["paper_doi"] in existing_paper_dois
    ]

    with conn:
        conn.execute("DELETE FROM repo_mappings WHERE source = ?", ("aea_mapping",))
        conn.executemany(
            """
            INSERT INTO repo_mappings (paper_doi, repo_doi, icpsr_project_id, source)
            VALUES (?, ?, ?, 'aea_mapping')
            """,
            matched_rows,
        )

    return {
        "matched_to_db": len(matched_rows),
        "unmatched_to_db": len(mappings) - len(matched_rows),
        "unique_project_ids": len({row[2] for row in matched_rows}),
    }


def main() -> int:
    """Run the AEA mapping ingestion pipeline."""
    configure_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        with build_session() as session:
            csv_text = download_csv(session)

        mappings, parse_stats = parse_csv(csv_text)
        store_stats = store_mappings(conn, mappings)

        LOGGER.info("AEA mapping CSV rows: %d", parse_stats["total_rows"])
        LOGGER.info("Valid parsed mappings: %d", parse_stats["valid_rows"])
        LOGGER.info("Matched to DB and inserted: %d", store_stats["matched_to_db"])
        LOGGER.info("Unmatched to DB (skipped): %d", store_stats["unmatched_to_db"])
        LOGGER.info("Unique project IDs inserted: %d", store_stats["unique_project_ids"])
        if parse_stats["invalid_doi_rows"] or parse_stats["invalid_icpsr_rows"]:
            LOGGER.info(
                "Skipped invalid rows: missing/invalid DOI=%d, missing/invalid ICPSR DOI=%d",
                parse_stats["invalid_doi_rows"],
                parse_stats["invalid_icpsr_rows"],
            )

        return 0
    except requests.HTTPError as exc:
        LOGGER.error("Failed to download AEA mapping CSV from %s: %s", CSV_URL, exc)
        return 1
    except requests.RequestException as exc:
        LOGGER.error("Request failed while downloading AEA mapping CSV: %s", exc)
        return 1
    except (OSError, ValueError) as exc:
        LOGGER.error("Failed to process AEA mapping CSV: %s", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
