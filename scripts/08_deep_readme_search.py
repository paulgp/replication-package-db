"""Deep README search for repos that came up empty in 07_classify_readmes.

Re-visits repos with has_readme=0 and traverses subfolders up to MAX_DEPTH
levels deep looking for a README. Updates readme_analysis if found.

Connects to a running Chrome instance via CDP (port 9222). Launch Chrome:
    /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\
        --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-icpsr

Usage:
    python scripts/08_deep_readme_search.py
    python scripts/08_deep_readme_search.py --limit 50
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
from urllib.parse import quote

from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATA_DIR, RAW_DIR, RESTRICTION_INDICATORS  # noqa: F401
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import DATA_DIR, RAW_DIR, RESTRICTION_INDICATORS  # noqa: F401
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

REPOS_RAW_DIR = RAW_DIR / "repos"
READMES_RAW_DIR = RAW_DIR / "readmes"
CDP_URL = "http://localhost:9222"
MAX_DEPTH = 3
PAGE_DELAY = 1.0
DOWNLOAD_WAIT = 5.0

README_RE = re.compile(r"^readme", re.IGNORECASE)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# Reuse classification + extraction from 07
# ---------------------------------------------------------------------------
import importlib.util

spec = importlib.util.spec_from_file_location(
    "classify_module", SCRIPT_DIR / "07_classify_readmes.py"
)
classify_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(classify_module)

classify_data_availability = classify_module.classify_data_availability
extract_readme_text = classify_module.extract_readme_text


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_no_readme_repos(conn) -> list[dict[str, str]]:
    """Return repos that came up with no README in the first pass."""
    rows = conn.execute(
        """
        SELECT DISTINCT ra.repo_doi, rm.icpsr_project_id
        FROM readme_analysis ra
        JOIN repo_mappings rm ON ra.repo_doi = rm.repo_doi
        WHERE ra.has_readme = 0
          AND rm.icpsr_project_id IS NOT NULL
        ORDER BY CAST(rm.icpsr_project_id AS INTEGER) DESC
        """
    ).fetchall()
    return [{"repo_doi": r["repo_doi"], "icpsr_project_id": r["icpsr_project_id"]} for r in rows]


def update_readme_result(
    conn,
    repo_doi: str,
    readme_text: str | None,
    classification: str | None,
    restriction_flags: list[str] | None,
) -> None:
    """Update readme_analysis row when we find a README on the deep search."""
    with conn:
        conn.execute(
            """
            UPDATE readme_analysis
            SET has_readme = 1,
                readme_text = ?,
                restriction_flags = ?,
                restriction_count = ?,
                data_availability = ?
            WHERE repo_doi = ?
            """,
            (
                readme_text[:5000] if readme_text else None,
                json.dumps(restriction_flags) if restriction_flags else json.dumps([]),
                len(restriction_flags) if restriction_flags else 0,
                classification,
                repo_doi,
            ),
        )


# ---------------------------------------------------------------------------
# Deep page scraping
# ---------------------------------------------------------------------------
def parse_listing(html: str) -> tuple[str | None, str | None, list[str]]:
    """Parse a project/folder page. Returns (readme_name, readme_path, subfolders)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.table.table-striped")
    if not table:
        return None, None, []

    folders: list[str] = []
    readme_name = None
    readme_path = None

    for row in table.select("tbody tr"):
        link = row.select_one("td a")
        if not link:
            continue
        fname = link.get_text(strip=True)
        href = link.get("href", "")
        if "path=" not in href:
            continue
        path = href.split("path=")[1].split("&")[0]

        if "type=folder" in href:
            folders.append(path)
        elif README_RE.match(fname):
            # Take the first README we find
            if readme_name is None:
                readme_name = fname
                readme_path = path

    return readme_name, readme_path, folders


def fetch_page_html(page: Any, url: str) -> str | None:
    """Fetch a page via Playwright, return HTML or None on failure."""
    try:
        page.goto(url, wait_until="networkidle", timeout=20000)
        time.sleep(PAGE_DELAY)
        return page.content()
    except Exception:
        return None


def search_for_readme(
    page: Any, project_id: str, max_depth: int = MAX_DEPTH
) -> tuple[str | None, str | None]:
    """BFS through subfolders looking for a README, up to max_depth levels.

    Returns (readme_name, readme_path) or (None, None).
    """
    base_view = (
        f"https://www.openicpsr.org/openicpsr/project/{project_id}/version/V1/view"
    )

    # Start with root page
    html = fetch_page_html(page, base_view)
    if html is None:
        return None, None

    readme_name, readme_path, folders = parse_listing(html)
    if readme_name:
        return readme_name, readme_path

    # BFS through subfolders, tracking depth
    queue: list[tuple[str, int]] = [(f, 1) for f in folders]

    while queue:
        folder_path, depth = queue.pop(0)
        if depth > max_depth:
            continue

        folder_url = (
            f"{base_view}?path={quote(folder_path, safe='/:')}&type=folder"
        )
        sub_html = fetch_page_html(page, folder_url)
        if sub_html is None:
            continue

        readme_name, readme_path, subfolders = parse_listing(sub_html)
        if readme_name:
            return readme_name, readme_path

        # Add deeper folders to queue
        for sub in subfolders:
            queue.append((sub, depth + 1))

    return None, None


# ---------------------------------------------------------------------------
# Download README (uses authenticated /download/file URL)
# ---------------------------------------------------------------------------
def download_readme(
    page: Any,
    project_id: str,
    readme_name: str,
    file_path: str,
) -> Path | None:
    """Download README via the /download/file endpoint. Returns cache path or None."""
    ext = Path(readme_name).suffix.lower() or ".txt"
    cache_path = READMES_RAW_DIR / f"{project_id}{ext}"

    if cache_path.exists():
        return cache_path

    dl_url = (
        f"https://www.openicpsr.org/openicpsr/project/{project_id}"
        f"/version/V1/download/file?filePath={file_path}"
    )

    before = set(f.name for f in READMES_RAW_DIR.iterdir())
    try:
        page.goto(dl_url, wait_until="commit", timeout=20000)
    except Exception:
        pass  # "Download is starting" expected
    time.sleep(DOWNLOAD_WAIT)

    after = set(f.name for f in READMES_RAW_DIR.iterdir())
    new = after - before
    new = {n for n in new if not n.endswith(".crdownload")}

    if not new:
        return None

    src = READMES_RAW_DIR / list(new)[0]
    src.rename(cache_path)
    return cache_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Deep README search")
    parser.add_argument("--limit", type=int, default=0, help="Max repos to process")
    args = parser.parse_args()

    READMES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        repos = get_no_readme_repos(conn)
        if args.limit > 0:
            repos = repos[: args.limit]
        LOGGER.info("No-README repos to deep-search: %d", len(repos))

        if not repos:
            LOGGER.info("Nothing to do.")
            return 0

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            LOGGER.error("playwright not installed")
            return 1

        stats = {
            "processed": 0,
            "found": 0,
            "downloaded": 0,
            "still_missing": 0,
            "all_data": 0,
            "partial_data": 0,
            "no_data": 0,
        }

        with sync_playwright() as pw:
            try:
                browser = pw.chromium.connect_over_cdp(CDP_URL)
            except Exception as exc:
                LOGGER.error("Cannot connect to Chrome on %s: %s", CDP_URL, exc)
                return 1

            context = browser.contexts[0]
            page = context.new_page()

            cdp = context.new_cdp_session(page)
            cdp.send(
                "Browser.setDownloadBehavior",
                {
                    "behavior": "allowAndName",
                    "downloadPath": str(READMES_RAW_DIR),
                    "eventsEnabled": True,
                },
            )
            LOGGER.info("Connected to Chrome")

            for idx, repo in enumerate(repos, 1):
                pid = repo["icpsr_project_id"]
                repo_doi = repo["repo_doi"]

                readme_name, file_path = search_for_readme(page, pid)

                if not readme_name:
                    stats["still_missing"] += 1
                    stats["processed"] += 1
                    if idx % 25 == 0:
                        LOGGER.info(
                            "Progress: %d/%d | found=%d still_missing=%d | all=%d partial=%d no=%d",
                            idx,
                            len(repos),
                            stats["found"],
                            stats["still_missing"],
                            stats["all_data"],
                            stats["partial_data"],
                            stats["no_data"],
                        )
                    continue

                stats["found"] += 1
                LOGGER.info("Found README for %s: %s", pid, readme_name)

                # Download
                cache_path = download_readme(page, pid, readme_name, file_path)
                if not cache_path:
                    stats["processed"] += 1
                    LOGGER.warning("Download failed for %s", pid)
                    continue

                stats["downloaded"] += 1

                # Extract and classify
                text = extract_readme_text(cache_path)
                if text:
                    classification, flags = classify_data_availability(text)
                    update_readme_result(conn, repo_doi, text, classification, flags)
                    stats[classification] += 1
                else:
                    update_readme_result(conn, repo_doi, None, None, [])

                stats["processed"] += 1

                if idx % 25 == 0:
                    LOGGER.info(
                        "Progress: %d/%d | found=%d still_missing=%d | all=%d partial=%d no=%d",
                        idx,
                        len(repos),
                        stats["found"],
                        stats["still_missing"],
                        stats["all_data"],
                        stats["partial_data"],
                        stats["no_data"],
                    )

            page.close()

        LOGGER.info("=" * 60)
        LOGGER.info("Deep README search complete")
        LOGGER.info("=" * 60)
        LOGGER.info("Processed:        %d", stats["processed"])
        LOGGER.info("READMEs found:    %d", stats["found"])
        LOGGER.info("Downloaded:       %d", stats["downloaded"])
        LOGGER.info("Still missing:    %d", stats["still_missing"])
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
