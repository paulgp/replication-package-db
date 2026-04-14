"""Classify README data availability using Claude Haiku.

Replaces the heuristic phrase-matching classifier with an LLM that can
understand context (e.g. "data on Zenodo" = available, "requires DUA" =
restricted).

Usage:
    python scripts/10_llm_classify.py              # classify all
    python scripts/10_llm_classify.py --limit 50   # test batch
    python scripts/10_llm_classify.py --dry-run    # preview without updating DB
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_CONCURRENT = 10
MAX_TEXT_LEN = 4000

SYSTEM_PROMPT = """You classify README files from academic economics replication packages into data availability categories.

Given a README excerpt, determine which category best describes the data availability:

- **all_data**: All data needed to replicate the paper are included in the repository (or freely available at a linked location like Zenodo/Dataverse/GitHub). No restricted, confidential, or proprietary data is required.

- **partial_data**: Some data is included or publicly available, but some data requires restricted access, purchase, or a data use agreement. Common examples: public data included but WRDS/census/proprietary data is not.

- **no_data**: No data can be shared. The replication package contains only code, or all data is confidential/proprietary with no public component.

Respond with ONLY a JSON object:
{"classification": "all_data"|"partial_data"|"no_data", "reason": "<one sentence>"}"""


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_readmes_to_classify(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT repo_doi, readme_text
        FROM readme_analysis
        WHERE has_readme = 1 AND readme_text IS NOT NULL
        ORDER BY repo_doi
        """
    ).fetchall()
    return [{"repo_doi": r["repo_doi"], "readme_text": r["readme_text"]} for r in rows]


async def classify_one(client, semaphore, repo_doi: str, text: str) -> dict:
    truncated = text[:MAX_TEXT_LEN]
    async with semaphore:
        try:
            response = await client.messages.create(
                model=MODEL,
                max_tokens=150,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": truncated}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            parsed = json.loads(raw)
            return {
                "repo_doi": repo_doi,
                "classification": parsed["classification"],
                "reason": parsed.get("reason", ""),
                "error": None,
            }
        except json.JSONDecodeError:
            return {
                "repo_doi": repo_doi,
                "classification": None,
                "reason": raw if 'raw' in dir() else "",
                "error": "json_parse_error",
            }
        except Exception as exc:
            return {
                "repo_doi": repo_doi,
                "classification": None,
                "reason": "",
                "error": str(exc),
            }


async def classify_batch(readmes: list[dict], dry_run: bool = False) -> list[dict]:
    import anthropic

    client = anthropic.AsyncAnthropic()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    tasks = [
        classify_one(client, semaphore, r["repo_doi"], r["readme_text"])
        for r in readmes
    ]

    results = []
    done = 0
    total = len(tasks)

    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        done += 1
        if done % 100 == 0 or done == total:
            LOGGER.info("Progress: %d/%d", done, total)

    return results


def update_db(conn, results: list[dict]) -> dict:
    stats = {"all_data": 0, "partial_data": 0, "no_data": 0, "errors": 0}

    with conn:
        for r in results:
            if r["error"] or r["classification"] not in ("all_data", "partial_data", "no_data"):
                stats["errors"] += 1
                continue

            conn.execute(
                """
                UPDATE readme_analysis
                SET data_availability = ?,
                    restriction_flags = ?,
                    restriction_count = 0
                WHERE repo_doi = ?
                """,
                (r["classification"], json.dumps([r["reason"]]), r["repo_doi"]),
            )
            stats[r["classification"]] += 1

    return stats


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="LLM-based README classification")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        LOGGER.error("ANTHROPIC_API_KEY not set in .env")
        return 1

    conn = get_connection(init_db())

    try:
        readmes = get_readmes_to_classify(conn)
        if args.limit > 0:
            readmes = readmes[: args.limit]
        LOGGER.info("READMEs to classify: %d", len(readmes))

        if not readmes:
            LOGGER.info("Nothing to do.")
            return 0

        results = asyncio.run(classify_batch(readmes, args.dry_run))

        errors = [r for r in results if r["error"]]
        if errors:
            LOGGER.warning("Errors: %d", len(errors))
            for e in errors[:5]:
                LOGGER.warning("  %s: %s", e["repo_doi"], e["error"])

        if args.dry_run:
            for r in results[:20]:
                print(f"  {r['repo_doi']}: {r['classification']} — {r['reason']}")
            counts = {}
            for r in results:
                c = r["classification"] or "error"
                counts[c] = counts.get(c, 0) + 1
            print(f"\nDry run summary: {counts}")
            return 0

        stats = update_db(conn, results)

        LOGGER.info("=" * 60)
        LOGGER.info("LLM classification complete")
        LOGGER.info("=" * 60)
        LOGGER.info("All data:     %d", stats["all_data"])
        LOGGER.info("Partial data: %d", stats["partial_data"])
        LOGGER.info("No data:      %d", stats["no_data"])
        LOGGER.info("Errors:       %d", stats["errors"])

        return 0

    except KeyboardInterrupt:
        LOGGER.info("Interrupted")
        return 130
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
