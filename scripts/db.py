"""SQLite helpers for the AEA Replication Tracker."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DB_PATH
except ImportError:  # pragma: no cover - supports direct script execution and package imports
    from scripts.config import DB_PATH

DDL = """
CREATE TABLE IF NOT EXISTS papers (
    id INTEGER PRIMARY KEY,
    openalex_id TEXT UNIQUE,
    doi TEXT UNIQUE,
    title TEXT,
    authors TEXT,           -- JSON array of {name, institution}
    journal_name TEXT,
    journal_issn TEXT,
    publication_date TEXT,
    publication_year INTEGER,
    abstract TEXT,
    paper_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS repo_mappings (
    id INTEGER PRIMARY KEY,
    paper_doi TEXT REFERENCES papers(doi),
    repo_doi TEXT,
    icpsr_project_id TEXT,
    source TEXT,            -- 'aea_mapping', 'datacite', 'crossref'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS repo_files (
    id INTEGER PRIMARY KEY,
    repo_doi TEXT,
    filename TEXT,
    extension TEXT,
    file_type TEXT,         -- 'data', 'code', 'documentation', 'archive', 'other'
    size_bytes INTEGER
);

CREATE TABLE IF NOT EXISTS readme_analysis (
    id INTEGER PRIMARY KEY,
    repo_doi TEXT UNIQUE,
    has_readme BOOLEAN,
    readme_text TEXT,
    restriction_flags TEXT,    -- JSON array of matched phrases
    restriction_count INTEGER,
    data_availability TEXT     -- 'all_data', 'partial_data', 'no_data', or NULL
);

CREATE TABLE IF NOT EXISTS replication_scores (
    id INTEGER PRIMARY KEY,
    paper_doi TEXT UNIQUE REFERENCES papers(doi),
    has_repo BOOLEAN,
    has_data BOOLEAN,
    no_restrictions BOOLEAN,
    replication_status TEXT, -- 'fully_replicable', 'code_only', 'restricted_data', 'no_repository', 'theoretical'
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal_issn);
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(publication_year);
CREATE INDEX IF NOT EXISTS idx_scores_status ON replication_scores(replication_status);
"""


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Return a SQLite connection configured with Row access."""
    target_path = Path(db_path) if db_path is not None else Path(DB_PATH)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(target_path)
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: str | Path | None = None) -> Path:
    """Initialize the SQLite database schema and return the database path."""
    target_path = Path(db_path) if db_path is not None else Path(DB_PATH)

    with get_connection(target_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        connection.executescript(DDL)
        connection.commit()

    return target_path


if __name__ == "__main__":
    database_path = init_db()
    with get_connection(database_path) as connection:
        tables = [
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
    print(f"Initialized database at {database_path}")
    print("Tables:", ", ".join(tables))
