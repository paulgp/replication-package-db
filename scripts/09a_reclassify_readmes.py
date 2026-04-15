"""Re-run classification on all existing README texts using current logic.

Useful after updating the classifier (e.g. adding table detection) to
propagate changes without re-downloading any files.

Usage:
    python scripts/09a_reclassify_readmes.py
    python scripts/09a_reclassify_readmes.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from db import get_connection, init_db
except ImportError:
    from scripts.db import get_connection, init_db

import importlib.util

spec = importlib.util.spec_from_file_location(
    "classify_module", SCRIPT_DIR / "07_classify_readmes.py"
)
classify_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(classify_module)

classify_data_availability = classify_module.classify_data_availability

LOGGER = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Re-classify README texts")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show changes without writing"
    )
    args = parser.parse_args()

    conn = get_connection(init_db())

    rows = conn.execute(
        """
        SELECT repo_doi, readme_text, data_availability, restriction_flags
        FROM readme_analysis
        WHERE has_readme = 1 AND readme_text IS NOT NULL
        """
    ).fetchall()

    changes = {}
    total = 0

    for r in rows:
        new_cls, new_flags = classify_data_availability(r["readme_text"])
        old_cls = r["data_availability"]

        if old_cls != new_cls:
            key = f"{old_cls} -> {new_cls}"
            changes[key] = changes.get(key, 0) + 1
            total += 1

            if not args.dry_run:
                conn.execute(
                    """
                    UPDATE readme_analysis
                    SET data_availability = ?,
                        restriction_flags = ?,
                        restriction_count = ?
                    WHERE repo_doi = ?
                    """,
                    (
                        new_cls,
                        json.dumps(new_flags),
                        len(new_flags),
                        r["repo_doi"],
                    ),
                )

    if not args.dry_run:
        conn.commit()

    LOGGER.info("Total READMEs: %d", len(rows))
    LOGGER.info("Reclassified: %d%s", total, " (dry run)" if args.dry_run else "")
    for key in sorted(changes):
        LOGGER.info("  %s: %d", key, changes[key])

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
