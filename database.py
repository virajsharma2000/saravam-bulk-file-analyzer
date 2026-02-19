"""
database.py — SQLite persistence layer for the Bulk File Retention Analyzer.
Uses the stdlib sqlite3 module with thread-safe connection handling.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from config import Config
from models import FileRecord, ScannedFile

logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path        TEXT    UNIQUE NOT NULL,
    file_hash        TEXT    NOT NULL,
    file_size        INTEGER NOT NULL,
    last_modified    TEXT    NOT NULL,
    extracted_text   TEXT,
    retention_score  INTEGER,
    category         TEXT,
    suggested_action TEXT,
    confidence       REAL,
    reasoning        TEXT,
    processed_at     TEXT
);
"""


# ── Connection factory ────────────────────────────────────────────────────────

def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Open (or create) a SQLite database and return the connection."""
    path = db_path or Config.DB_PATH
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Row objects behave like dicts
    return conn


# ── Public API ────────────────────────────────────────────────────────────────

def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Initialise the database schema and return an open connection."""
    conn = get_connection(db_path)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.info("Database initialised at '%s'.", db_path or Config.DB_PATH)
    return conn


def insert_or_update_file(conn: sqlite3.Connection, record: FileRecord) -> None:
    """
    Upsert a FileRecord into the `files` table.
    Uses INSERT OR REPLACE to handle the UNIQUE constraint on file_path.
    """
    conn.execute(
        """
        INSERT INTO files
            (file_path, file_hash, file_size, last_modified,
             extracted_text, retention_score, category,
             suggested_action, confidence, reasoning, processed_at)
        VALUES
            (:file_path, :file_hash, :file_size, :last_modified,
             :extracted_text, :retention_score, :category,
             :suggested_action, :confidence, :reasoning, :processed_at)
        ON CONFLICT(file_path) DO UPDATE SET
            file_hash        = excluded.file_hash,
            file_size        = excluded.file_size,
            last_modified    = excluded.last_modified,
            extracted_text   = excluded.extracted_text,
            retention_score  = excluded.retention_score,
            category         = excluded.category,
            suggested_action = excluded.suggested_action,
            confidence       = excluded.confidence,
            reasoning        = excluded.reasoning,
            processed_at     = excluded.processed_at
        """,
        record.model_dump(exclude={"id"}),
    )
    conn.commit()


def get_processed_hashes(conn: sqlite3.Connection) -> Set[str]:
    """Return the set of file hashes already stored in the database."""
    cursor = conn.execute("SELECT file_hash FROM files WHERE processed_at IS NOT NULL")
    return {row["file_hash"] for row in cursor.fetchall()}


def get_unprocessed_files(
    conn: sqlite3.Connection,
    scanned_files: List[ScannedFile],
) -> List[ScannedFile]:
    """
    Filter *scanned_files* to those not yet processed (hash-based skip).
    Files with an unchanged hash that already have a decision are skipped.
    """
    processed_hashes = get_processed_hashes(conn)
    unprocessed = [f for f in scanned_files if f.file_hash not in processed_hashes]
    skipped = len(scanned_files) - len(unprocessed)
    if skipped:
        logger.info("Skipping %d already-processed file(s).", skipped)
    return unprocessed


def get_all_results(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return all rows from the files table as a list of dicts."""
    cursor = conn.execute(
        """
        SELECT id, file_path, file_hash, file_size, last_modified,
               retention_score, category, suggested_action,
               confidence, reasoning, processed_at
        FROM files
        ORDER BY retention_score DESC
        """
    )
    return [dict(row) for row in cursor.fetchall()]


def now_utc() -> str:
    """Current UTC timestamp as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
