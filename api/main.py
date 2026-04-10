"""FastAPI backend for the AEA Replication Tracker MVP."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

API_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = API_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import DB_PATH  # noqa: E402

app = FastAPI(title="AEA Replication Tracker", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


# ---------------------------------------------------------------------------
# Aggregate stats
# ---------------------------------------------------------------------------
@app.get("/api/stats/overview")
def stats_overview() -> dict[str, Any]:
    """Top-level dashboard counts."""
    conn = get_conn()
    try:
        total_papers = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        status_rows = conn.execute(
            """
            SELECT replication_status, COUNT(*) AS n
            FROM replication_scores
            GROUP BY replication_status
            """
        ).fetchall()
        return {
            "total_papers": total_papers,
            "by_status": {r["replication_status"]: r["n"] for r in status_rows},
        }
    finally:
        conn.close()


@app.get("/api/stats/by-year")
def stats_by_year(start: int = 2005, end: int = 2026) -> list[dict[str, Any]]:
    """Status breakdown by year."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                p.publication_year AS year,
                rs.replication_status AS status,
                COUNT(*) AS n
            FROM papers p
            JOIN replication_scores rs ON p.doi = rs.paper_doi
            WHERE p.publication_year BETWEEN ? AND ?
            GROUP BY p.publication_year, rs.replication_status
            ORDER BY p.publication_year
            """,
            (start, end),
        ).fetchall()

        # Pivot into { year: { status: n, total: ... } }
        by_year: dict[int, dict[str, Any]] = {}
        for r in rows:
            y = r["year"]
            by_year.setdefault(y, {"year": y})
            by_year[y][r["status"]] = r["n"]

        result = []
        for y in sorted(by_year):
            entry = by_year[y]
            entry["total"] = sum(
                v for k, v in entry.items() if k != "year" and isinstance(v, int)
            )
            result.append(entry)
        return result
    finally:
        conn.close()


@app.get("/api/stats/by-journal")
def stats_by_journal(start: int = 2019, end: int = 2026) -> list[dict[str, Any]]:
    """Status breakdown by journal for a given year range."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                p.journal_name AS journal,
                rs.replication_status AS status,
                COUNT(*) AS n
            FROM papers p
            JOIN replication_scores rs ON p.doi = rs.paper_doi
            WHERE p.publication_year BETWEEN ? AND ?
            GROUP BY p.journal_name, rs.replication_status
            """,
            (start, end),
        ).fetchall()

        by_journal: dict[str, dict[str, Any]] = {}
        for r in rows:
            j = r["journal"] or "Unknown"
            by_journal.setdefault(j, {"journal": j})
            by_journal[j][r["status"]] = r["n"]

        result = []
        for j in sorted(by_journal):
            entry = by_journal[j]
            entry["total"] = sum(
                v for k, v in entry.items() if k != "journal" and isinstance(v, int)
            )
            result.append(entry)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Papers (browse + detail)
# ---------------------------------------------------------------------------
@app.get("/api/papers")
def list_papers(
    year_start: int | None = Query(None),
    year_end: int | None = Query(None),
    journal: str | None = None,
    status: str | None = None,
    has_readme: bool | None = None,
    data_availability: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Filterable paper list."""
    conn = get_conn()
    try:
        where = []
        params: list[Any] = []

        if year_start is not None:
            where.append("p.publication_year >= ?")
            params.append(year_start)
        if year_end is not None:
            where.append("p.publication_year <= ?")
            params.append(year_end)
        if journal:
            where.append("p.journal_name = ?")
            params.append(journal)
        if status:
            where.append("rs.replication_status = ?")
            params.append(status)
        if has_readme is not None:
            where.append("ra.has_readme = ?")
            params.append(1 if has_readme else 0)
        if data_availability:
            where.append("ra.data_availability = ?")
            params.append(data_availability)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        count_sql = f"""
            SELECT COUNT(DISTINCT p.doi) AS n
            FROM papers p
            LEFT JOIN replication_scores rs ON p.doi = rs.paper_doi
            LEFT JOIN repo_mappings rm ON p.doi = rm.paper_doi
            LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
            {where_sql}
        """
        total = conn.execute(count_sql, params).fetchone()[0]

        list_sql = f"""
            SELECT
                p.doi,
                p.title,
                p.authors,
                p.publication_year,
                p.publication_date,
                p.journal_name,
                rs.replication_status,
                rs.has_repo,
                ra.has_readme,
                ra.data_availability,
                rm.repo_doi,
                rm.icpsr_project_id
            FROM papers p
            LEFT JOIN replication_scores rs ON p.doi = rs.paper_doi
            LEFT JOIN repo_mappings rm ON p.doi = rm.paper_doi
            LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
            {where_sql}
            GROUP BY p.doi
            ORDER BY p.publication_date DESC
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(list_sql, params + [limit, offset]).fetchall()

        items: list[dict[str, Any]] = []
        for r in rows:
            d = row_to_dict(r)
            if d.get("authors"):
                try:
                    d["authors"] = json.loads(d["authors"])
                except (json.JSONDecodeError, TypeError):
                    pass
            items.append(d)

        return {"total": total, "limit": limit, "offset": offset, "items": items}
    finally:
        conn.close()


@app.get("/api/papers/{paper_doi:path}")
def get_paper(paper_doi: str) -> dict[str, Any]:
    """Full detail for a single paper."""
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                p.*,
                rs.replication_status,
                rs.has_repo,
                rs.has_data,
                rs.no_restrictions,
                rs.computed_at
            FROM papers p
            LEFT JOIN replication_scores rs ON p.doi = rs.paper_doi
            WHERE p.doi = ?
            """,
            (paper_doi,),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Paper not found: {paper_doi}")

        paper = row_to_dict(row)
        if paper.get("authors"):
            try:
                paper["authors"] = json.loads(paper["authors"])
            except (json.JSONDecodeError, TypeError):
                pass

        repos = conn.execute(
            """
            SELECT
                rm.repo_doi,
                rm.icpsr_project_id,
                rm.source,
                ra.has_readme,
                ra.data_availability,
                ra.restriction_count,
                ra.restriction_flags,
                ra.readme_text
            FROM repo_mappings rm
            LEFT JOIN readme_analysis ra ON rm.repo_doi = ra.repo_doi
            WHERE rm.paper_doi = ?
            """,
            (paper_doi,),
        ).fetchall()

        repo_list = []
        for r in repos:
            d = row_to_dict(r)
            if d.get("restriction_flags"):
                try:
                    d["restriction_flags"] = json.loads(d["restriction_flags"])
                except (json.JSONDecodeError, TypeError):
                    pass
            repo_list.append(d)
        paper["repositories"] = repo_list

        return paper
    finally:
        conn.close()


@app.get("/api/journals")
def list_journals() -> list[str]:
    """All distinct journals."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT journal_name FROM papers WHERE journal_name IS NOT NULL ORDER BY journal_name"
        ).fetchall()
        return [r["journal_name"] for r in rows]
    finally:
        conn.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
