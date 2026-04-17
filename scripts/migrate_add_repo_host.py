"""Add `repo_host` column to repo_mappings / repo_files / readme_analysis.

One-time migration for finance-journal work:

  - adds `repo_host TEXT` to three tables (idempotent via PRAGMA check)
  - backfills `repo_mappings.repo_host` from `repo_doi` prefix and source
  - restores `repo_mappings.icpsr_project_id` to its original meaning
    (openICPSR project id for 10.3886 deposits, NULL otherwise). This undoes
    the temporary reuse in 03c_crossref_relations.py and 04b_datacite_finance.py.
  - backfills `repo_files.repo_host` / `readme_analysis.repo_host` from the
    matching `repo_mappings` row (first non-null host).

Host labels:
  openicpsr, dataverse, zenodo, mendeley, figshare, osf, github, other
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from db import get_connection, init_db
except ImportError:  # pragma: no cover
    from scripts.db import get_connection, init_db

LOGGER = logging.getLogger(__name__)

# SQL CASE expression that matches scripts/03c/04b classifier.
HOST_CASE = """
    CASE
      WHEN source = 'rfs_dataverse' THEN 'dataverse'
      WHEN source IN ('crossref_relation', 'datacite_finance')
           AND icpsr_project_id IN
               ('openicpsr','dataverse','zenodo','mendeley','figshare','osf','github','other')
        THEN icpsr_project_id
      WHEN LOWER(repo_doi) LIKE '10.3886/icpsr%' THEN 'icpsr'
      WHEN repo_doi LIKE '10.3886/%'      THEN 'openicpsr'
      WHEN repo_doi LIKE '10.7910/%'      THEN 'dataverse'
      WHEN repo_doi LIKE '10.5281/%'      THEN 'zenodo'
      WHEN repo_doi LIKE '10.6084/%'      THEN 'figshare'
      WHEN repo_doi LIKE '10.17605/%'     THEN 'osf'
      WHEN repo_doi LIKE '10.17632/%'     THEN 'mendeley'
      WHEN LOWER(repo_doi) LIKE 'https://data.mendeley.com%' THEN 'mendeley'
      WHEN LOWER(repo_doi) LIKE 'https://dataverse.harvard.edu%' THEN 'dataverse'
      WHEN LOWER(repo_doi) LIKE 'https://zenodo.org%' THEN 'zenodo'
      WHEN LOWER(repo_doi) LIKE 'https://github.com%' THEN 'github'
      WHEN LOWER(repo_doi) LIKE 'https://osf.io%' THEN 'osf'
      ELSE 'other'
    END
"""


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def column_exists(conn, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def add_column_if_missing(conn, table: str, column: str, coltype: str) -> bool:
    if column_exists(conn, table, column):
        LOGGER.info("  %s.%s already exists — skipping ADD", table, column)
        return False
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
    LOGGER.info("  added %s.%s %s", table, column, coltype)
    return True


def main() -> int:
    configure_logging()
    db_path = init_db()
    conn = get_connection(db_path)

    try:
        LOGGER.info("Schema changes")
        with conn:
            add_column_if_missing(conn, "repo_mappings", "repo_host", "TEXT")
            add_column_if_missing(conn, "repo_files", "repo_host", "TEXT")
            add_column_if_missing(conn, "readme_analysis", "repo_host", "TEXT")

        LOGGER.info("Backfilling repo_mappings.repo_host")
        with conn:
            conn.execute(f"""
                UPDATE repo_mappings
                SET repo_host = {HOST_CASE}
                WHERE repo_host IS NULL
            """)

        LOGGER.info("Restoring repo_mappings.icpsr_project_id (was repurposed as host label)")
        # For non-openICPSR deposits where icpsr_project_id holds a host string,
        # clear it. Only rows with repo_doi matching openICPSR pattern keep a value.
        with conn:
            conn.execute("""
                UPDATE repo_mappings
                SET icpsr_project_id = NULL
                WHERE source IN ('crossref_relation','datacite_finance')
                  AND icpsr_project_id IN
                      ('openicpsr','dataverse','zenodo','mendeley','figshare','osf','github','other')
                  AND repo_doi NOT LIKE '10.3886/%'
            """)
            # For the 'openicpsr' label rows on 10.3886 DOIs, extract the numeric project id.
            conn.execute("""
                UPDATE repo_mappings
                SET icpsr_project_id = (
                  SELECT substr(
                    substr(repo_doi, 9),  -- drop '10.3886/'
                    2,                    -- drop leading 'E' or 'e'
                    CASE
                      WHEN instr(substr(repo_doi, 10), 'V') > 0
                        THEN instr(substr(repo_doi, 10), 'V') - 1
                      WHEN instr(substr(repo_doi, 10), 'v') > 0
                        THEN instr(substr(repo_doi, 10), 'v') - 1
                      ELSE length(substr(repo_doi, 10))
                    END
                  )
                )
                WHERE source IN ('crossref_relation','datacite_finance')
                  AND repo_doi LIKE '10.3886/%'
                  AND icpsr_project_id IN ('openicpsr')
            """)

        LOGGER.info("Backfilling repo_files.repo_host from repo_mappings")
        with conn:
            conn.execute("""
                UPDATE repo_files
                SET repo_host = (
                    SELECT rm.repo_host
                    FROM repo_mappings rm
                    WHERE rm.repo_doi = repo_files.repo_doi
                      AND rm.repo_host IS NOT NULL
                    LIMIT 1
                )
                WHERE repo_host IS NULL
            """)

        LOGGER.info("Backfilling readme_analysis.repo_host from repo_mappings")
        with conn:
            conn.execute("""
                UPDATE readme_analysis
                SET repo_host = (
                    SELECT rm.repo_host
                    FROM repo_mappings rm
                    WHERE rm.repo_doi = readme_analysis.repo_doi
                      AND rm.repo_host IS NOT NULL
                    LIMIT 1
                )
                WHERE repo_host IS NULL
            """)

        LOGGER.info("Final host distributions")
        for table in ("repo_mappings", "repo_files", "readme_analysis"):
            rows = conn.execute(
                f"SELECT repo_host, COUNT(*) FROM {table} GROUP BY repo_host ORDER BY 2 DESC"
            ).fetchall()
            LOGGER.info("  %s", table)
            for r in rows:
                LOGGER.info("    %-10s %d", r[0] if r[0] is not None else "(null)", r[1])
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
