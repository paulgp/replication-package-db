"""Shared configuration constants for the AEA Replication Tracker."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - allows importing config before deps are installed
    def load_dotenv(*_args, **_kwargs):
        return False

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

OPENALEX_EMAIL = os.getenv("OPENALEX_EMAIL")

AEA_JOURNALS = {
    "American Economic Review": "0002-8282",
    "American Economic Review: Insights": "2640-205X",
    "AEA Papers and Proceedings": "2574-0768",
    "American Economic Journal: Applied Economics": "1945-7782",
    "American Economic Journal: Economic Policy": "1945-7731",
    "American Economic Journal: Macroeconomics": "1945-7707",
    "American Economic Journal: Microeconomics": "1945-7669",
    "Journal of Economic Literature": "0022-0515",
    "Journal of Economic Perspectives": "0895-3309",
}

OPENALEX_BASE_URL = "https://api.openalex.org"
DATACITE_BASE_URL = "https://api.datacite.org"
CROSSREF_BASE_URL = "https://api.crossref.org"
OPENICPSR_BASE_URL = "https://www.openicpsr.org/openicpsr/project"

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "replication_tracker.db"

FILE_TYPE_CLASSIFICATIONS = {
    ".csv": "data",
    ".dta": "data",
    ".feather": "data",
    ".json": "data",
    ".parquet": "data",
    ".rdata": "data",
    ".rds": "data",
    ".sav": "data",
    ".tsv": "data",
    ".txt": "data",
    ".xlsx": "data",
    ".zip": "archive",
    ".tar": "archive",
    ".gz": "archive",
    ".7z": "archive",
    ".rar": "archive",
    ".py": "code",
    ".r": "code",
    ".do": "code",
    ".ipynb": "code",
    ".jl": "code",
    ".m": "code",
    ".mat": "code",
    ".sas": "code",
    ".sh": "code",
    ".pdf": "documentation",
    ".md": "documentation",
    ".doc": "documentation",
    ".docx": "documentation",
    ".rtf": "documentation",
    ".tex": "documentation",
    ".readme": "documentation",
}

RESTRICTION_INDICATORS = [
    "available upon request",
    "data are available upon request",
    "restricted access",
    "restricted-use data",
    "confidential data",
    "proprietary data",
    "cannot be shared",
    "not publicly available",
    "requires a data use agreement",
    "subject to licensing restrictions",
    "by application only",
    "remote access only",
]

RATE_LIMITS = {
    "openalex": {
        "requests_per_second": 10,
        "requests_per_day": 100000,
        "polite_pool_requires_email": True,
    },
    "datacite": {
        "requests_per_second": 5,
    },
    "crossref": {
        "requests_per_second": 50,
        "polite_pool_recommended": True,
    },
    "openicpsr": {
        "requests_per_second": 1,
    },
}
