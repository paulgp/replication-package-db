"""Compute replication scores for all papers.

Joins papers → repo_mappings → readme_analysis to derive a replication
status per paper, written to the replication_scores table.

Status categories:
  - fully_replicable:    has repo, README says all data is included
  - partially_replicable: has repo, README says some data is restricted
  - not_replicable:      has repo, README says data cannot be shared
  - unanalyzed_repo:     has repo, but no README found / extraction failed
  - no_repository:       no entry in repo_mappings

Usage: python scripts/09_compute_scores.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DATA_DIR
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.config import DATA_DIR
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

SUMMARY_PATH = DATA_DIR / "score_summary.json"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------------
def derive_status(
    has_repo: bool, data_availability: str | None
) -> tuple[str, bool, bool | None, bool | None]:
    """Return (status, has_repo, has_data, no_restrictions)."""
    if not has_repo:
        return "no_repository", False, None, None

    if data_availability == "all_data":
        return "fully_replicable", True, True, True

    if data_availability == "partial_data":
        return "partially_replicable", True, True, False

    if data_availability == "no_data":
        return "not_replicable", True, False, False

    # has_repo but no classification (no README found or extraction failed)
    return "unanalyzed_repo", True, None, None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    configure_logging()

    db_path = init_db()
    conn = get_connection(db_path)

    try:
        # Pull all papers with optional repo + readme info via LEFT JOINs.
        # A paper may have multiple repos; pick the "best" data_availability
        # (all_data > partial_data > no_data > NULL).
        rows = conn.execute(
            """
            WITH best_repo AS (
                SELECT
                    rm.paper_doi,
                    MIN(
                        CASE ra.data_availability
                            WHEN 'all_data'      THEN 1
                            WHEN 'partial_data'  THEN 2
                            WHEN 'no_data'       THEN 3
                            ELSE                      4
                        END
                    ) AS best_rank
                FROM repo_mappings rm
                LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
                WHERE rm.paper_doi IS NOT NULL
                GROUP BY rm.paper_doi
            )
            SELECT
                p.doi,
                p.publication_year,
                p.journal_name,
                CASE WHEN br.paper_doi IS NOT NULL THEN 1 ELSE 0 END AS has_repo,
                CASE br.best_rank
                    WHEN 1 THEN 'all_data'
                    WHEN 2 THEN 'partial_data'
                    WHEN 3 THEN 'no_data'
                    ELSE NULL
                END AS data_availability
            FROM papers p
            LEFT JOIN best_repo br ON p.doi = br.paper_doi
            WHERE p.doi IS NOT NULL
            """
        ).fetchall()

        LOGGER.info("Papers to score: %d", len(rows))

        # Compute scores
        scored: list[tuple] = []
        for r in rows:
            status, has_repo, has_data, no_restrictions = derive_status(
                bool(r["has_repo"]), r["data_availability"]
            )
            scored.append((r["doi"], has_repo, has_data, no_restrictions, status))

        # Insert / replace
        with conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO replication_scores
                    (paper_doi, has_repo, has_data, no_restrictions, replication_status, computed_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                scored,
            )

        # ----- Summary statistics -----
        status_counts: dict[str, int] = {}
        journal_counts: dict[str, dict[str, int]] = {}
        year_counts: dict[int, dict[str, int]] = {}

        for r, s in zip(rows, scored):
            status = s[4]
            status_counts[status] = status_counts.get(status, 0) + 1

            j = r["journal_name"] or "Unknown"
            journal_counts.setdefault(j, {})[status] = (
                journal_counts.setdefault(j, {}).get(status, 0) + 1
            )

            y = r["publication_year"]
            if y is not None:
                year_counts.setdefault(y, {})[status] = (
                    year_counts.setdefault(y, {}).get(status, 0) + 1
                )

        total = len(scored)
        LOGGER.info("=" * 60)
        LOGGER.info("Replication score summary")
        LOGGER.info("=" * 60)
        LOGGER.info("Total papers scored: %d", total)
        LOGGER.info("-" * 40)
        LOGGER.info("Status breakdown:")
        for status in [
            "fully_replicable",
            "partially_replicable",
            "not_replicable",
            "unanalyzed_repo",
            "no_repository",
        ]:
            n = status_counts.get(status, 0)
            pct = 100.0 * n / total if total else 0.0
            LOGGER.info("  %-25s %5d (%5.1f%%)", status, n, pct)

        LOGGER.info("-" * 40)
        LOGGER.info("By journal:")
        for journal in sorted(journal_counts):
            j_total = sum(journal_counts[journal].values())
            full = journal_counts[journal].get("fully_replicable", 0)
            partial = journal_counts[journal].get("partially_replicable", 0)
            none_data = journal_counts[journal].get("not_replicable", 0)
            LOGGER.info(
                "  %-50s %4d papers | full=%3d partial=%3d none=%3d",
                journal[:50],
                j_total,
                full,
                partial,
                none_data,
            )

        # Save summary JSON
        summary = {
            "total_papers": total,
            "by_status": status_counts,
            "by_journal": journal_counts,
            "by_year": {str(y): year_counts[y] for y in sorted(year_counts)},
        }
        SUMMARY_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True))
        LOGGER.info("-" * 40)
        LOGGER.info("Summary written to %s", SUMMARY_PATH)

        return 0

    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
        return 130
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
