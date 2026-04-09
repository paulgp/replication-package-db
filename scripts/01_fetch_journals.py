"""Fetch AEA journal metadata from OpenAlex and save normalized results."""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
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
    from config import (
        AEA_JOURNALS,
        DATA_DIR,
        OPENALEX_BASE_URL,
        OPENALEX_EMAIL,
        RATE_LIMITS,
        RAW_DIR,
    )
except ImportError:  # pragma: no cover - supports direct script execution and package imports
    from scripts.config import (
        AEA_JOURNALS,
        DATA_DIR,
        OPENALEX_BASE_URL,
        OPENALEX_EMAIL,
        RATE_LIMITS,
        RAW_DIR,
    )

LOGGER = logging.getLogger(__name__)
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.0
RATE_LIMIT_SLEEP_SECONDS = 0.15
RAW_JOURNALS_DIR = RAW_DIR / "journals"
OUTPUT_PATH = DATA_DIR / "journals.json"


class JournalFetchError(RuntimeError):
    """Raised when a journal cannot be fetched after retries."""


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

    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": user_agent,
        }
    )
    return session


def extract_openalex_id(openalex_url: str | None) -> str | None:
    """Extract the OpenAlex source identifier from a full URL."""
    if not openalex_url:
        return None

    path = urlparse(openalex_url).path.rstrip("/")
    if not path:
        return None
    return path.rsplit("/", maxsplit=1)[-1] or None


def write_json(path: Path, payload: Any) -> None:
    """Write JSON to disk with deterministic formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def atomic_write_json(path: Path, payload: Any) -> None:
    """Write JSON atomically by renaming a temp file into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temp_file:
        json.dump(payload, temp_file, indent=2, sort_keys=True)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)

    os.replace(temp_path, path)


def fetch_journal(issn: str, session: requests.Session) -> dict[str, Any]:
    """Query OpenAlex for a single journal by ISSN with retries."""
    url = f"{OPENALEX_BASE_URL}/sources"
    params = {"filter": f"issn:{issn}"}
    if OPENALEX_EMAIL:
        params["mailto"] = OPENALEX_EMAIL

    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if attempt > MAX_RETRIES:
                raise JournalFetchError(f"request failed for ISSN {issn}: {exc}") from exc
            delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            LOGGER.warning(
                "Request error for ISSN %s on attempt %s/%s: %s; retrying in %.1fs",
                issn,
                attempt,
                MAX_RETRIES + 1,
                exc,
                delay,
            )
            time.sleep(delay)
            continue

        if response.status_code == 429 or 500 <= response.status_code < 600:
            if attempt > MAX_RETRIES:
                raise JournalFetchError(
                    f"OpenAlex returned HTTP {response.status_code} for ISSN {issn} after retries"
                )

            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header is not None:
                try:
                    delay = max(float(retry_after_header), 0.0)
                except ValueError:
                    delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            else:
                delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))

            LOGGER.warning(
                "OpenAlex returned HTTP %s for ISSN %s on attempt %s/%s; retrying in %.1fs",
                response.status_code,
                issn,
                attempt,
                MAX_RETRIES + 1,
                delay,
            )
            time.sleep(delay)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise JournalFetchError(f"OpenAlex returned HTTP {response.status_code} for ISSN {issn}") from exc

        return response.json()


def normalize_journal(result: dict[str, Any], fallback_issn: str) -> dict[str, Any]:
    """Normalize a journal record into the target output schema."""
    return {
        "openalex_id": extract_openalex_id(result.get("id")),
        "display_name": result.get("display_name"),
        "issn": result.get("issn_l") or fallback_issn,
        "works_count": result.get("works_count"),
        "homepage_url": result.get("homepage_url"),
    }


def fetch_all_journals() -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch metadata for all configured AEA journals."""
    RAW_JOURNALS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    journals: list[dict[str, Any]] = []
    failures: list[str] = []

    with build_session() as session:
        for journal_name, issn in AEA_JOURNALS.items():
            LOGGER.info("Fetching %s (%s)", journal_name, issn)
            try:
                payload = fetch_journal(issn=issn, session=session)
                write_json(RAW_JOURNALS_DIR / f"{issn}.json", payload)

                results = payload.get("results", [])
                if not results:
                    LOGGER.warning("No OpenAlex results returned for %s (%s)", journal_name, issn)
                    time.sleep(RATE_LIMIT_SLEEP_SECONDS)
                    continue

                journal = normalize_journal(results[0], fallback_issn=issn)
                journals.append(journal)
                LOGGER.info(
                    "Fetched %s -> %s (%s works)",
                    journal_name,
                    journal.get("display_name"),
                    journal.get("works_count"),
                )
            except JournalFetchError as exc:
                failures.append(issn)
                LOGGER.error("Failed to fetch %s (%s): %s", journal_name, issn, exc)
            except (OSError, ValueError, TypeError) as exc:
                failures.append(issn)
                LOGGER.error("Failed to process %s (%s): %s", journal_name, issn, exc)
            finally:
                time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    return journals, failures


def main() -> int:
    """Run the journal metadata fetch pipeline."""
    configure_logging()
    journals, failures = fetch_all_journals()
    atomic_write_json(OUTPUT_PATH, journals)

    LOGGER.info("Wrote %s journal entries to %s", len(journals), OUTPUT_PATH)
    if failures:
        LOGGER.error("Encountered failures for %s journal(s): %s", len(failures), ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
