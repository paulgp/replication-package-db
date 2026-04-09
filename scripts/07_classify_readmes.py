"""Download READMEs from openICPSR and classify data availability.

Connects to a running Chrome instance via CDP (remote debugging port 9222).
Launch Chrome first:
    /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\
        --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-icpsr

Then log into openICPSR manually, and run this script.

Usage:
    python scripts/07_classify_readmes.py              # process all repos
    python scripts/07_classify_readmes.py --limit 50   # process first 50 unprocessed
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

from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATA_DIR, RAW_DIR, RESTRICTION_INDICATORS
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import DATA_DIR, RAW_DIR, RESTRICTION_INDICATORS
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPOS_RAW_DIR = RAW_DIR / "repos"
READMES_RAW_DIR = RAW_DIR / "readmes"
CDP_URL = "http://localhost:9222"

README_RE = re.compile(r"^readme", re.IGNORECASE)

# How long to wait between page loads (seconds)
PAGE_DELAY = 1.0
DOWNLOAD_WAIT = 5.0


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# Data availability classification
# ---------------------------------------------------------------------------

# Explicit "no data" — data cannot be included
NO_DATA_PHRASES = [
    "the data for this project are confidential",
    "all data are confidential",
    "data cannot be made publicly available",
    "data are not publicly available",
    "data is not publicly available",
    "not possible to make the data directly available",
    "not possible to make the data available",
    "cannot share the data",
    "required to destroy the data",
    "we are not allowed to share",
    "we are not able to share",
    "the data cannot be posted",
    "data cannot be redistributed",
    "not permitted to redistribute",
]

# Explicit "partial data" — some included, some restricted
PARTIAL_DATA_PHRASES = [
    "some data cannot be made publicly available",
    "not all data can be made",
    "some of the data is restricted",
    "some of the data are restricted",
    "partially restricted",
    "some data are confidential",
    "some data is confidential",
]

# Explicit "all data" — everything included
ALL_DATA_PHRASES = [
    "all data are publicly available",
    "all data is publicly available",
    "all data are provided",
    "all data and code are provided",
    "all data included in this",
    "all data files are included",
    "all the data used in this paper are included",
    "all necessary data",
]

# Individual restriction indicators (need 2+ to flag as restricted)
RESTRICTION_PHRASES = [
    "data use agreement",
    "data-use agreement",
    "confidential",
    "proprietary",
    "restricted access",
    "restricted-use",
    "available upon request",
    "available from the author",
    "purchased from",
    "purchased data",
    "licensed data",
    "licensed from",
    "cannot be shared",
    "cannot be posted",
    "cannot be redistributed",
    "not publicly available",
]


def classify_data_availability(text: str) -> tuple[str, list[str]]:
    """Classify README text into data availability categories.

    Returns (classification, matched_phrases) where classification is one of:
      - 'no_data': data is not included in the repository
      - 'partial_data': some data included, some restricted
      - 'all_data': all data is included
    """
    tl = text.lower()

    # Check explicit partial-data statements first (most specific)
    for phrase in PARTIAL_DATA_PHRASES:
        if phrase in tl:
            restrictions = [p for p in RESTRICTION_PHRASES if p in tl]
            return "partial_data", restrictions

    # Check explicit no-data statements
    for phrase in NO_DATA_PHRASES:
        if phrase in tl:
            restrictions = [p for p in RESTRICTION_PHRASES if p in tl]
            return "no_data", restrictions

    # Check explicit all-data statements
    for phrase in ALL_DATA_PHRASES:
        if phrase in tl:
            restrictions = [p for p in RESTRICTION_PHRASES if p in tl]
            if restrictions:
                # Says "all data" but also mentions restrictions → partial
                return "partial_data", restrictions
            return "all_data", []

    # Heuristic: count restriction indicator phrases
    restrictions = [p for p in RESTRICTION_PHRASES if p in tl]

    if len(restrictions) >= 2:
        return "no_data", restrictions
    elif len(restrictions) == 1:
        return "partial_data", restrictions
    else:
        return "all_data", []


# ---------------------------------------------------------------------------
# README text extraction
# ---------------------------------------------------------------------------
def extract_readme_text(file_path: Path) -> str | None:
    """Extract text from a README file (PDF, DOCX, TXT, MD, HTML)."""
    ext = file_path.suffix.lower()

    if ext == ".pdf":
        try:
            import fitz

            with fitz.open(str(file_path)) as doc:
                pages = [p.get_text() for p in doc if p.get_text()]
            return "\n\n".join(pages) if pages else None
        except Exception as exc:
            LOGGER.warning("PDF extraction failed for %s: %s", file_path.name, exc)
            return None

    if ext == ".docx":
        try:
            import fitz

            with fitz.open(str(file_path)) as doc:
                pages = [p.get_text() for p in doc if p.get_text()]
            return "\n\n".join(pages) if pages else None
        except Exception as exc:
            LOGGER.warning("DOCX extraction failed for %s: %s", file_path.name, exc)
            return None

    if ext == ".html":
        try:
            raw = file_path.read_text(errors="replace")
            return BeautifulSoup(raw, "html.parser").get_text()
        except Exception:
            return None

    if ext in (".txt", ".md", ""):
        try:
            return file_path.read_text("utf-8")
        except UnicodeDecodeError:
            return file_path.read_text("latin-1")

    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_repos_to_process(conn) -> list[dict[str, str]]:
    """Return repos not yet in readme_analysis."""
    rows = conn.execute(
        """
        SELECT DISTINCT rm.repo_doi, rm.icpsr_project_id
        FROM repo_mappings rm
        WHERE rm.icpsr_project_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM readme_analysis ra WHERE ra.repo_doi = rm.repo_doi
          )
        ORDER BY rm.icpsr_project_id
        """
    ).fetchall()
    return [{"repo_doi": r["repo_doi"], "icpsr_project_id": r["icpsr_project_id"]} for r in rows]


def save_result(
    conn,
    repo_doi: str,
    has_readme: bool,
    readme_text: str | None,
    classification: str | None,
    restriction_flags: list[str] | None,
) -> None:
    """Save classification result to readme_analysis."""
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO readme_analysis
                (repo_doi, has_readme, readme_text, restriction_flags, restriction_count,
                 data_availability)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                repo_doi,
                has_readme,
                readme_text[:5000] if readme_text else None,
                json.dumps(restriction_flags) if restriction_flags else json.dumps([]),
                len(restriction_flags) if restriction_flags else 0,
                classification,
            ),
        )


# ---------------------------------------------------------------------------
# Page scraping helpers
# ---------------------------------------------------------------------------
def find_readme_in_html(html: str) -> tuple[str | None, str | None, list[str]]:
    """Find README filename and file path from a project/folder page HTML.

    Returns (readme_name, file_path, folder_paths).
    readme_name and file_path are for the first README found (or None).
    folder_paths is a list of subfolder path parameters for deeper search.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.table.table-striped")
    if not table:
        return None, None, []

    folders: list[str] = []

    for row in table.select("tbody tr"):
        link = row.select_one("td a")
        if not link:
            continue
        fname = link.get_text(strip=True)
        href = link.get("href", "")

        if "type=folder" in href and "path=" in href:
            folder_path = href.split("path=")[1].split("&")[0]
            folders.append(folder_path)
            continue

        if README_RE.match(fname) and "path=" in href:
            file_path = href.split("path=")[1].split("&")[0]
            return fname, file_path, folders

    return None, None, folders


def find_readme_with_subfolders(
    page: Any, project_id: str, root_html: str
) -> tuple[str | None, str | None]:
    """Search root page and one level of subfolders for a README.

    Returns (readme_name, file_path) or (None, None).
    """
    # Check root first
    readme_name, file_path, folders = find_readme_in_html(root_html)
    if readme_name:
        return readme_name, file_path

    # Check each subfolder (one level deep)
    for folder_path in folders:
        folder_url = (
            f"https://www.openicpsr.org/openicpsr/project/{project_id}"
            f"/version/V1/view?path={folder_path}&type=folder"
        )
        try:
            page.goto(folder_url, wait_until="networkidle", timeout=20000)
            time.sleep(PAGE_DELAY)
            sub_html = page.content()
        except Exception:
            continue

        readme_name, file_path, _ = find_readme_in_html(sub_html)
        if readme_name:
            return readme_name, file_path

    return None, None


def cached_readme_path(project_id: str) -> Path | None:
    """Check if we already have a cached README for this project."""
    for ext in [".pdf", ".txt", ".md", ".docx", ".html"]:
        p = READMES_RAW_DIR / f"{project_id}{ext}"
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    configure_logging()

    parser = argparse.ArgumentParser(description="Download and classify READMEs")
    parser.add_argument("--limit", type=int, default=0, help="Max repos to process (0=all)")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REPOS_RAW_DIR.mkdir(parents=True, exist_ok=True)
    READMES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        repos = get_repos_to_process(conn)
        total = len(repos)
        if args.limit > 0:
            repos = repos[: args.limit]
        LOGGER.info("Repos to process: %d (of %d unprocessed)", len(repos), total)

        if not repos:
            LOGGER.info("Nothing to do.")
            return 0

        # Connect to Chrome via CDP
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            LOGGER.error("playwright not installed")
            return 1

        stats = {
            "processed": 0,
            "page_failed": 0,
            "no_readme": 0,
            "downloaded": 0,
            "from_cache": 0,
            "download_failed": 0,
            "all_data": 0,
            "partial_data": 0,
            "no_data": 0,
        }

        with sync_playwright() as pw:
            try:
                browser = pw.chromium.connect_over_cdp(CDP_URL)
            except Exception as exc:
                LOGGER.error(
                    "Cannot connect to Chrome on %s. "
                    "Launch Chrome with: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome "
                    "--remote-debugging-port=9222 --user-data-dir=/tmp/chrome-icpsr\n"
                    "Error: %s",
                    CDP_URL,
                    exc,
                )
                return 1

            context = browser.contexts[0]
            page = context.new_page()

            # Set download directory via CDP
            cdp = context.new_cdp_session(page)
            cdp.send(
                "Browser.setDownloadBehavior",
                {
                    "behavior": "allowAndName",
                    "downloadPath": str(READMES_RAW_DIR),
                    "eventsEnabled": True,
                },
            )
            LOGGER.info("Connected to Chrome, downloads -> %s", READMES_RAW_DIR)

            for idx, repo in enumerate(repos, 1):
                pid = repo["icpsr_project_id"]
                repo_doi = repo["repo_doi"]

                # --- Check for cached README first ---
                cached = cached_readme_path(pid)
                if cached:
                    text = extract_readme_text(cached)
                    if text:
                        classification, flags = classify_data_availability(text)
                        save_result(conn, repo_doi, True, text, classification, flags)
                        stats["from_cache"] += 1
                        stats[classification] += 1
                        stats["processed"] += 1
                        if idx % 100 == 0:
                            LOGGER.info("Progress: %d/%d", idx, len(repos))
                        continue
                    # Cached file but no text — treat as unanalyzed
                    save_result(conn, repo_doi, True, None, None, [])
                    stats["from_cache"] += 1
                    stats["processed"] += 1
                    continue

                # --- Fetch project page ---
                html_cache = REPOS_RAW_DIR / f"{pid}.html"
                mhtml_cache = REPOS_RAW_DIR / f"{pid}.mhtml"
                html = None

                if html_cache.exists():
                    html = html_cache.read_text(encoding="utf-8")
                elif mhtml_cache.exists():
                    import email as _email

                    raw = mhtml_cache.read_bytes()
                    msg = _email.message_from_bytes(raw)
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            payload = part.get_payload(decode=True)
                            if payload:
                                html = payload.decode(
                                    part.get_content_charset() or "utf-8", errors="replace"
                                )
                                break

                if html is None:
                    try:
                        page.goto(
                            f"https://www.openicpsr.org/openicpsr/project/{pid}/version/V1/view",
                            wait_until="networkidle",
                            timeout=30000,
                        )
                        time.sleep(PAGE_DELAY)
                        html = page.content()
                        try:
                            html_cache.write_text(html, encoding="utf-8")
                        except OSError:
                            pass
                    except Exception:
                        stats["page_failed"] += 1
                        save_result(conn, repo_doi, False, None, None, [])
                        stats["processed"] += 1
                        if idx % 50 == 0:
                            LOGGER.info("Progress: %d/%d", idx, len(repos))
                        continue

                # --- Find README in file listing ---
                readme_name, file_path = find_readme_with_subfolders(page, pid, html)

                if not readme_name:
                    stats["no_readme"] += 1
                    save_result(conn, repo_doi, False, None, None, [])
                    stats["processed"] += 1
                    if idx % 50 == 0:
                        LOGGER.info("Progress: %d/%d", idx, len(repos))
                    continue

                # --- Download README ---
                ext = Path(readme_name).suffix.lower()
                cache_path = READMES_RAW_DIR / f"{pid}{ext}"

                if not cache_path.exists():
                    dl_url = (
                        f"https://www.openicpsr.org/openicpsr/project/{pid}"
                        f"/version/V1/download/file?filePath={file_path}"
                    )

                    before_files = set(f.name for f in READMES_RAW_DIR.iterdir())
                    try:
                        page.goto(dl_url, wait_until="commit", timeout=20000)
                    except Exception:
                        pass  # "Download is starting" is expected
                    time.sleep(DOWNLOAD_WAIT)

                    # Find newly downloaded file and rename
                    after_files = set(f.name for f in READMES_RAW_DIR.iterdir())
                    new_names = after_files - before_files
                    # Filter out .crdownload files
                    new_names = {n for n in new_names if not n.endswith(".crdownload")}

                    if new_names:
                        src = READMES_RAW_DIR / list(new_names)[0]
                        src.rename(cache_path)
                        stats["downloaded"] += 1
                    else:
                        stats["download_failed"] += 1
                        save_result(conn, repo_doi, True, None, None, [])
                        stats["processed"] += 1
                        LOGGER.warning("Download failed for %s (%s)", pid, readme_name)
                        continue

                # --- Extract text and classify ---
                text = extract_readme_text(cache_path)
                if text:
                    classification, flags = classify_data_availability(text)
                    save_result(conn, repo_doi, True, text, classification, flags)
                    stats[classification] += 1
                else:
                    save_result(conn, repo_doi, True, None, None, [])

                stats["processed"] += 1

                if idx % 50 == 0:
                    LOGGER.info(
                        "Progress: %d/%d | dl=%d cache=%d no_readme=%d | "
                        "all=%d partial=%d no=%d",
                        idx,
                        len(repos),
                        stats["downloaded"],
                        stats["from_cache"],
                        stats["no_readme"],
                        stats["all_data"],
                        stats["partial_data"],
                        stats["no_data"],
                    )

            page.close()

        # --------------- Summary ---------------
        LOGGER.info("=" * 60)
        LOGGER.info("README classification complete")
        LOGGER.info("=" * 60)
        LOGGER.info("Processed:        %d", stats["processed"])
        LOGGER.info("Page failures:    %d", stats["page_failed"])
        LOGGER.info("No README:        %d", stats["no_readme"])
        LOGGER.info("Downloaded:       %d", stats["downloaded"])
        LOGGER.info("From cache:       %d", stats["from_cache"])
        LOGGER.info("Download failed:  %d", stats["download_failed"])
        LOGGER.info("-" * 40)
        LOGGER.info("ALL DATA:         %d", stats["all_data"])
        LOGGER.info("PARTIAL DATA:     %d", stats["partial_data"])
        LOGGER.info("NO DATA:          %d", stats["no_data"])

        return 0

    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        return 130
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
