"""Export database data as static JSON files for GitHub Pages deployment."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from config import DB_PATH

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "frontend" / "public" / "data"


def export(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- stats/overview ---
    total_papers = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    status_rows = conn.execute(
        "SELECT replication_status, COUNT(*) AS n FROM replication_scores GROUP BY replication_status"
    ).fetchall()
    write_json(OUTPUT_DIR / "stats-overview.json", {
        "total_papers": total_papers,
        "by_status": {r["replication_status"]: r["n"] for r in status_rows},
    })

    # --- stats/by-year ---
    rows = conn.execute("""
        SELECT p.publication_year AS year, rs.replication_status AS status, COUNT(*) AS n
        FROM papers p JOIN replication_scores rs ON p.doi = rs.paper_doi
        WHERE p.publication_year BETWEEN 2005 AND 2026
        GROUP BY p.publication_year, rs.replication_status
        ORDER BY p.publication_year
    """).fetchall()
    by_year: dict[int, dict] = {}
    for r in rows:
        y = r["year"]
        by_year.setdefault(y, {"year": y})
        by_year[y][r["status"]] = r["n"]
    result = []
    for y in sorted(by_year):
        entry = by_year[y]
        entry["total"] = sum(v for k, v in entry.items() if k != "year" and isinstance(v, int))
        result.append(entry)
    write_json(OUTPUT_DIR / "stats-by-year.json", result)

    # --- stats/by-journal ---
    rows = conn.execute("""
        SELECT p.journal_name AS journal, rs.replication_status AS status, COUNT(*) AS n
        FROM papers p JOIN replication_scores rs ON p.doi = rs.paper_doi
        WHERE p.publication_year BETWEEN 2019 AND 2026
        GROUP BY p.journal_name, rs.replication_status
    """).fetchall()
    by_journal: dict[str, dict] = {}
    for r in rows:
        j = r["journal"] or "Unknown"
        by_journal.setdefault(j, {"journal": j})
        by_journal[j][r["status"]] = r["n"]
    result = []
    for j in sorted(by_journal):
        entry = by_journal[j]
        entry["total"] = sum(v for k, v in entry.items() if k != "journal" and isinstance(v, int))
        result.append(entry)
    write_json(OUTPUT_DIR / "stats-by-journal.json", result)

    # --- journals ---
    rows = conn.execute(
        "SELECT DISTINCT journal_name FROM papers WHERE journal_name IS NOT NULL ORDER BY journal_name"
    ).fetchall()
    write_json(OUTPUT_DIR / "journals.json", [r["journal_name"] for r in rows])

    # --- papers (with repo info, no readme text) ---
    paper_rows = conn.execute("""
        SELECT p.doi, p.title, p.authors, p.publication_year, p.publication_date,
               p.journal_name, rs.replication_status, rs.has_repo, rs.has_data,
               rs.no_restrictions, rs.computed_at
        FROM papers p
        LEFT JOIN replication_scores rs ON p.doi = rs.paper_doi
        ORDER BY p.publication_date DESC
    """).fetchall()

    repo_rows = conn.execute("""
        SELECT rm.paper_doi, rm.repo_doi, rm.icpsr_project_id, rm.source,
               ra.has_readme, ra.data_availability, ra.restriction_count, ra.restriction_flags
        FROM repo_mappings rm
        LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
    """).fetchall()
    repos_by_doi: dict[str, list] = {}
    for r in repo_rows:
        d = dict(r)
        doi = d.pop("paper_doi")
        if d.get("restriction_flags"):
            try:
                d["restriction_flags"] = json.loads(d["restriction_flags"])
            except (json.JSONDecodeError, TypeError):
                pass
        repos_by_doi.setdefault(doi, []).append(d)

    papers = []
    for r in paper_rows:
        d = dict(r)
        if d.get("authors"):
            try:
                d["authors"] = json.loads(d["authors"])
            except (json.JSONDecodeError, TypeError):
                pass
        d["repositories"] = repos_by_doi.get(d["doi"], [])
        papers.append(d)
    write_json(OUTPUT_DIR / "papers.json", papers)

    print(f"Exported {len(papers)} papers to {OUTPUT_DIR}")


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, default=str), encoding="utf-8")


if __name__ == "__main__":
    conn = sqlite3.connect(str(DB_PATH))
    try:
        export(conn)
    finally:
        conn.close()
