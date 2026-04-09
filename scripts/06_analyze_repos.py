"""Fetch root-level file listings from openICPSR repos and extract README content."""

from __future__ import annotations

import email
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
    from config import (
        DATA_DIR,
        FILE_TYPE_CLASSIFICATIONS,
        OPENICPSR_BASE_URL,
        RAW_DIR,
        RESTRICTION_INDICATORS,
    )
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import (
        DATA_DIR,
        FILE_TYPE_CLASSIFICATIONS,
        OPENICPSR_BASE_URL,
        RAW_DIR,
        RESTRICTION_INDICATORS,
    )
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RATE_LIMIT_SLEEP = 1.0
REQUEST_TIMEOUT_MS = 30_000
REPOS_RAW_DIR = RAW_DIR / "repos"
READMES_RAW_DIR = RAW_DIR / "readmes"

README_RE = re.compile(r"^readme\b", re.IGNORECASE)


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
# DB helpers
# ---------------------------------------------------------------------------
def get_repos_to_process(conn) -> list[dict[str, str]]:
    """Return repos with an ICPSR project ID, ready for scraping."""
    rows = conn.execute(
        """
        SELECT DISTINCT repo_doi, icpsr_project_id
        FROM repo_mappings
        WHERE icpsr_project_id IS NOT NULL
        ORDER BY icpsr_project_id
        """
    ).fetchall()
    return [{"repo_doi": r["repo_doi"], "icpsr_project_id": r["icpsr_project_id"]} for r in rows]


def get_already_processed(conn) -> set[str]:
    """Return set of repo_doi values already in repo_files."""
    rows = conn.execute("SELECT DISTINCT repo_doi FROM repo_files").fetchall()
    return {r["repo_doi"] for r in rows}


def insert_repo_files(conn, repo_doi: str, files: list[dict[str, Any]]) -> None:
    """Insert file records for a repository."""
    with conn:
        for f in files:
            conn.execute(
                """
                INSERT INTO repo_files (repo_doi, filename, extension, file_type, size_bytes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    repo_doi,
                    f["filename"],
                    f["extension"],
                    f["file_type"],
                    f["size_bytes"],
                ),
            )


def insert_readme_analysis(
    conn, repo_doi: str, readme_filename: str | None, readme_text: str | None
) -> None:
    """Insert or replace README analysis for a repository."""
    has_readme = readme_filename is not None

    # Scan for restriction indicators
    restriction_flags: list[str] = []
    if readme_text:
        text_lower = readme_text.lower()
        for phrase in RESTRICTION_INDICATORS:
            if phrase.lower() in text_lower:
                restriction_flags.append(phrase)

    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO readme_analysis
                (repo_doi, has_readme, readme_text, restriction_flags, restriction_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                repo_doi,
                has_readme,
                readme_text,
                json.dumps(restriction_flags),
                len(restriction_flags),
            ),
        )


# ---------------------------------------------------------------------------
# MHTML support and Playwright page fetching
# ---------------------------------------------------------------------------
def _extract_html_from_mhtml(mhtml_path: Path) -> str:
    """Extract the HTML body from an MHTML file using the email module."""
    raw = mhtml_path.read_bytes()
    msg = email.message_from_bytes(raw)

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")

    # Non-multipart fallback
    payload = msg.get_payload(decode=True)
    if payload:
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace")

    return ""


def fetch_project_page(browser_context: Any, project_id: str) -> str | None:
    """Fetch the openICPSR project page HTML, using cache when available.

    Checks for cached .html then .mhtml files before making a live request
    via Playwright. Caches the fetched HTML to ``REPOS_RAW_DIR/{project_id}.html``.

    Returns the HTML string, or *None* on failure.
    """
    REPOS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    html_cache = REPOS_RAW_DIR / f"{project_id}.html"
    mhtml_cache = REPOS_RAW_DIR / f"{project_id}.mhtml"

    # Try cached HTML first
    if html_cache.exists():
        LOGGER.debug("Using cached HTML for project %s", project_id)
        return html_cache.read_text(encoding="utf-8")

    # Try cached MHTML
    if mhtml_cache.exists():
        LOGGER.debug("Using cached MHTML for project %s", project_id)
        html = _extract_html_from_mhtml(mhtml_cache)
        if html:
            return html

    # Fetch via Playwright
    url = f"{OPENICPSR_BASE_URL}/{project_id}/version/V1/view"
    LOGGER.info("Fetching project page: %s", url)

    page = None
    try:
        page = browser_context.new_page()
        page.goto(url, wait_until="networkidle", timeout=REQUEST_TIMEOUT_MS)
        page.wait_for_timeout(3000)
        html = page.content()
        page.close()

        # Cache the result
        try:
            html_cache.write_text(html, encoding="utf-8")
        except OSError as exc:
            LOGGER.warning("Failed to cache HTML for project %s: %s", project_id, exc)

        return html

    except Exception as exc:
        LOGGER.warning("Failed to fetch project page %s: %s", project_id, exc)
        if page:
            try:
                page.close()
            except Exception:
                pass
        return None


def fetch_readme_bytes(
    browser_context: Any, project_id: str, filename: str
) -> bytes | None:
    """Fetch raw README bytes via the openICPSR getBinary endpoint.

    Uses ``browser_context.request.get()`` so the session cookies from
    Playwright are included. Caches to ``READMES_RAW_DIR/{project_id}.{ext}``.

    Returns raw bytes on success, *None* on failure.
    """
    READMES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    ext = Path(filename).suffix.lower() or ".txt"
    cache_path = READMES_RAW_DIR / f"{project_id}{ext}"

    if cache_path.exists():
        LOGGER.debug("Using cached README for project %s", project_id)
        return cache_path.read_bytes()

    # Build MIME type for the content type parameter
    mime_map = {
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".md": "text/markdown",
    }
    content_type = mime_map.get(ext, "application/octet-stream")

    # Construct the getBinary URL
    file_path = f"/openicpsr/{project_id}/fcr:versions/V1/{filename}"
    url = (
        f"https://www.openicpsr.org/openicpsr/project/{project_id}"
        f"/version/V1/getBinary?filePath={file_path}"
        f"&contentType={content_type}&jwtToken="
    )

    LOGGER.info("Fetching README: %s", filename)
    try:
        response = browser_context.request.get(url, timeout=REQUEST_TIMEOUT_MS)
        if response.ok:
            raw_bytes = response.body()
            try:
                cache_path.write_bytes(raw_bytes)
            except OSError as exc:
                LOGGER.warning(
                    "Failed to cache README for project %s: %s", project_id, exc
                )
            return raw_bytes
        else:
            LOGGER.warning(
                "Failed to fetch README for project %s: HTTP %d",
                project_id,
                response.status,
            )
            return None
    except Exception as exc:
        LOGGER.warning("Error fetching README for project %s: %s", project_id, exc)
        return None


# ---------------------------------------------------------------------------
# HTML parsing and file classification
# ---------------------------------------------------------------------------
def parse_size_string(size_str: str) -> int:
    """Parse a human-readable size string to bytes.

    Examples: "2.6 KB", "992 bytes", "23.3 MB", "1.2 GB", "500".
    Returns 0 if the string cannot be parsed.
    """
    if not size_str or not size_str.strip():
        return 0

    size_str = size_str.strip()

    # Multipliers for recognized suffixes
    multipliers = {
        "BYTE": 1,
        "BYTES": 1,
        "KB": 1_024,
        "MB": 1_024 ** 2,
        "GB": 1_024 ** 3,
        "TB": 1_024 ** 4,
    }

    # Try matching "<number> <suffix>"
    match = re.match(r"^([\d.,]+)\s*([a-zA-Z]*)\s*$", size_str)
    if not match:
        return 0

    number_str = match.group(1).replace(",", "")
    suffix = match.group(2).upper()

    try:
        number = float(number_str)
    except ValueError:
        return 0

    if not suffix:
        # Bare number — treat as bytes
        return int(number)

    multiplier = multipliers.get(suffix)
    if multiplier is None:
        LOGGER.debug("Unknown size suffix: %r in %r", suffix, size_str)
        return 0

    return int(number * multiplier)


def extract_files_from_html(html: str) -> list[dict[str, Any]]:
    """Parse the openICPSR project page and extract root-level file listings.

    Returns a list of dicts with keys: filename, size_bytes.
    Skips folder rows (href containing ``type=folder``).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.table.table-striped")

    if table is None:
        LOGGER.warning("No file table found in HTML")
        return []

    files: list[dict[str, Any]] = []
    folder_count = 0

    tbody = table.find("tbody")
    if tbody is None:
        LOGGER.warning("No tbody found in file table")
        return []

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # First cell: filename link
        link = cells[0].find("a")
        if link is None:
            continue

        href = link.get("href", "")
        if "type=folder" in href:
            folder_count += 1
            continue

        filename = link.get_text(strip=True)
        if not filename:
            continue

        # Third cell: size
        size_str = cells[2].get_text(strip=True)
        size_bytes = parse_size_string(size_str)

        files.append({"filename": filename, "size_bytes": size_bytes})

    if folder_count > 0:
        LOGGER.info("Skipped %d folder(s) in file listing", folder_count)

    return files


def classify_file(filename: str, size_bytes: int) -> dict[str, Any]:
    """Classify a file by its extension using FILE_TYPE_CLASSIFICATIONS.

    Special cases:
    - README files (matching README_RE) are classified as ``documentation``.
    - ``.txt`` files larger than 100 KB are classified as ``data``.

    Returns a dict with: filename, extension, file_type, size_bytes, is_readme.
    """
    ext = Path(filename).suffix.lower()
    is_readme = bool(README_RE.match(filename))

    if is_readme:
        file_type = "documentation"
    elif ext == ".txt" and size_bytes > 100 * 1024:
        file_type = "data"
    else:
        file_type = FILE_TYPE_CLASSIFICATIONS.get(ext, "other")

    return {
        "filename": filename,
        "extension": ext,
        "file_type": file_type,
        "size_bytes": size_bytes,
        "is_readme": is_readme,
    }


# ---------------------------------------------------------------------------
# README text extraction
# ---------------------------------------------------------------------------
def extract_readme_text(raw_bytes: bytes, filename: str) -> str | None:
    """Extract text content from a README file.

    Supports PDF (via PyMuPDF/fitz) and plain text (.txt, .md).
    Returns the extracted text string, or *None* on failure.
    """
    ext = Path(filename).suffix.lower()

    if ext == ".pdf":
        try:
            import fitz  # PyMuPDF

            pages_text: list[str] = []
            with fitz.open(stream=raw_bytes, filetype="pdf") as doc:
                for page in doc:
                    text = page.get_text()
                    if text:
                        pages_text.append(text)
            if pages_text:
                return "\n\n".join(pages_text)
            LOGGER.warning("No text extracted from PDF: %s", filename)
            return None
        except Exception as exc:
            LOGGER.warning("Failed to extract text from PDF %s: %s", filename, exc)
            return None

    # Plain text / markdown
    if ext in (".txt", ".md", ""):
        try:
            return raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            try:
                return raw_bytes.decode("latin-1")
            except Exception as exc:
                LOGGER.warning(
                    "Failed to decode text file %s: %s", filename, exc
                )
                return None

    LOGGER.warning("Unsupported README format: %s", ext)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    configure_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REPOS_RAW_DIR.mkdir(parents=True, exist_ok=True)
    READMES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        repos = get_repos_to_process(conn)
        already_done = get_already_processed(conn)
        to_process = [r for r in repos if r["repo_doi"] not in already_done]

        total = len(repos)
        remaining = len(to_process)
        LOGGER.info("Total distinct repos: %d", total)
        LOGGER.info("Already processed: %d", len(already_done))
        LOGGER.info("Remaining to process: %d", remaining)

        if remaining == 0:
            LOGGER.info("Nothing to do.")
            return 0

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            LOGGER.error(
                "playwright not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return 1

        processed = 0
        failed = 0
        total_files = 0
        type_counts: dict[str, int] = {}
        readmes_found = 0
        readmes_extracted = 0
        unavailable: list[str] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )

            for idx, repo in enumerate(to_process, 1):
                project_id = repo["icpsr_project_id"]
                repo_doi = repo["repo_doi"]

                if idx > 1:
                    time.sleep(RATE_LIMIT_SLEEP)

                # --- Step 1: Fetch root page and parse file listing ---
                html = fetch_project_page(context, project_id)

                if html is None:
                    failed += 1
                    unavailable.append(project_id)
                    LOGGER.warning("Project %s: page fetch failed", project_id)
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO repo_files
                                (repo_doi, filename, extension, file_type, size_bytes)
                            VALUES (?, '__unavailable__', NULL, 'unavailable', NULL)
                            """,
                            (repo_doi,),
                        )
                    insert_readme_analysis(conn, repo_doi, None, None)
                    continue

                raw_files = extract_files_from_html(html)

                if not raw_files:
                    failed += 1
                    unavailable.append(project_id)
                    LOGGER.warning(
                        "Project %s: no files found in HTML", project_id
                    )
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO repo_files
                                (repo_doi, filename, extension, file_type, size_bytes)
                            VALUES (?, '__unavailable__', NULL, 'unavailable', NULL)
                            """,
                            (repo_doi,),
                        )
                    insert_readme_analysis(conn, repo_doi, None, None)
                    continue

                # --- Step 2: Classify files ---
                classified = [
                    classify_file(f["filename"], f["size_bytes"])
                    for f in raw_files
                ]
                insert_repo_files(conn, repo_doi, classified)
                total_files += len(classified)
                processed += 1

                for f in classified:
                    ft = f["file_type"]
                    type_counts[ft] = type_counts.get(ft, 0) + 1

                # --- Step 3: Find and fetch README ---
                readme_file = None
                for f in classified:
                    if f["is_readme"]:
                        readme_file = f
                        break

                if readme_file:
                    readmes_found += 1
                    readme_name = readme_file["filename"]

                    time.sleep(RATE_LIMIT_SLEEP)
                    raw_bytes = fetch_readme_bytes(context, project_id, readme_name)
                    readme_text = None
                    if raw_bytes:
                        readme_text = extract_readme_text(raw_bytes, readme_name)
                        if readme_text:
                            readmes_extracted += 1

                    insert_readme_analysis(conn, repo_doi, readme_name, readme_text)
                else:
                    insert_readme_analysis(conn, repo_doi, None, None)

                # --- Progress ---
                if idx % 50 == 0:
                    LOGGER.info(
                        "Progress: %d/%d | ok=%d fail=%d files=%d readmes=%d/%d",
                        idx,
                        remaining,
                        processed,
                        failed,
                        total_files,
                        readmes_extracted,
                        readmes_found,
                    )

            context.close()
            browser.close()

        # --------------- Summary ---------------
        LOGGER.info("=" * 60)
        LOGGER.info("Repository analysis complete")
        LOGGER.info("=" * 60)
        LOGGER.info("Repos processed:          %d", processed)
        LOGGER.info("Repos failed/unavailable: %d", failed)
        LOGGER.info("Total root-level files:   %d", total_files)
        LOGGER.info("-" * 40)
        LOGGER.info("Files by type:")
        for ft in sorted(type_counts):
            LOGGER.info("  %-20s %d", ft, type_counts[ft])
        LOGGER.info("-" * 40)
        LOGGER.info("READMEs found:            %d", readmes_found)
        LOGGER.info("READMEs text extracted:   %d", readmes_extracted)
        LOGGER.info("Cached HTML pages:        %s", REPOS_RAW_DIR)
        LOGGER.info("Cached README files:      %s", READMES_RAW_DIR)

        if unavailable:
            LOGGER.info("-" * 40)
            LOGGER.info("Unavailable repos (first 20):")
            for pid in unavailable[:20]:
                LOGGER.info("  %s", pid)
            if len(unavailable) > 20:
                LOGGER.info("  ... and %d more", len(unavailable) - 20)

        return 0

    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        return 130
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
