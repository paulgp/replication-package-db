"""Export a DOI → data availability lookup as JSON for use by other projects.

Output: data/replication_lookup.json
{
  "10.1257/aer.20180601": {
    "status": "full_data",
    "icpsr_id": "119743",
    "reason": "All data included in the repository..."
  },
  ...
}
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.db import get_connection, init_db
from scripts.config import DATA_DIR


def main() -> int:
    conn = get_connection(init_db())

    rows = conn.execute(
        """
        SELECT rs.paper_doi, rs.replication_status,
               rm.icpsr_project_id,
               ra.data_availability, ra.restriction_flags
        FROM replication_scores rs
        JOIN repo_mappings rm ON rs.paper_doi = rm.paper_doi
        LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
        WHERE rs.has_repo = 1
          AND rm.icpsr_project_id IS NOT NULL
        ORDER BY rs.paper_doi
        """
    ).fetchall()

    lookup = {}
    for r in rows:
        doi = r["paper_doi"]
        if doi in lookup:
            continue
        entry = {
            "status": r["replication_status"],
            "icpsr_id": r["icpsr_project_id"],
        }
        if r["restriction_flags"]:
            flags = json.loads(r["restriction_flags"])
            if flags and flags[0]:
                entry["reason"] = flags[0]
        lookup[doi] = entry

    out_path = DATA_DIR / "replication_lookup.json"
    out_path.write_text(json.dumps(lookup, indent=2, sort_keys=True))
    print(f"Exported {len(lookup)} papers to {out_path}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
