"""
retention_engine.py — Orchestrates the extract → classify → store pipeline.
Processes files concurrently using asyncio.gather with a Semaphore for rate limiting.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import AsyncIterator, Callable, List, Optional

import sarvam_client
import llm_client
from config import Config
from database import get_unprocessed_files, insert_or_update_file, now_utc
from models import FileRecord, ScannedFile, ExtractionResult, RetentionDecision

logger = logging.getLogger(__name__)


# ── Single-file processor ─────────────────────────────────────────────────────

async def process_file(
    scanned: ScannedFile,
    conn: sqlite3.Connection,
    semaphore: asyncio.Semaphore,
    progress_callback: Optional[Callable[[str, str], None]] = None,
) -> FileRecord:
    """
    Process a single file end-to-end:
      1. Extract text via Sarvam Document Intelligence API.
      2. Classify via Chat Completion LLM.
      3. Persist result in SQLite.

    The *semaphore* caps concurrent API calls to Config.MAX_CONCURRENCY.
    *progress_callback(file_path, status)* is called at key stages for UI updates.
    """
    def _notify(status: str) -> None:
        if progress_callback:
            try:
                progress_callback(scanned.file_path, status)
            except Exception:
                pass  # Progress updates must never crash the pipeline

    async with semaphore:
        _notify("extracting")
        logger.info("Extracting text from: %s", scanned.file_path)

        extraction: ExtractionResult = await sarvam_client.extract_text(scanned.file_path)

        if not extraction.text:
            logger.warning(
                "Empty extraction for '%s'. Using fallback decision.", scanned.file_path
            )
            decision = RetentionDecision.fallback("No text extracted from document")
        else:
            _notify("classifying")
            logger.info("Classifying: %s", scanned.file_path)
            decision = await llm_client.classify_document(scanned, extraction)

        record = FileRecord.from_scan_and_decision(
            scanned=scanned,
            extraction=extraction,
            decision=decision,
            processed_at=now_utc(),
        )

        # DB write is synchronous — wrap in thread pool to avoid blocking event loop
        await asyncio.get_event_loop().run_in_executor(
            None, insert_or_update_file, conn, record
        )

        _notify("done")
        logger.info(
            "Processed '%s' → action=%s score=%d confidence=%.2f",
            scanned.file_path, decision.suggested_action,
            decision.retention_score, decision.confidence,
        )
        return record


# ── Batch processor ───────────────────────────────────────────────────────────

async def process_all(
    scanned_files: List[ScannedFile],
    conn: sqlite3.Connection,
    progress_callback: Optional[Callable[[str, str], None]] = None,
    concurrency: Optional[int] = None,
) -> List[FileRecord]:
    """
    Process a list of ScannedFiles concurrently.

    - Skips files already present in the DB with the same hash.
    - Limits concurrency via asyncio.Semaphore.
    - Returns processed FileRecord list (already-processed files excluded).

    Args:
        scanned_files: All files discovered by the scanner.
        conn:          Open SQLite connection.
        progress_callback: Optional UI callback(file_path, status).
        concurrency:   Override Config.MAX_CONCURRENCY if set.
    """
    limit = concurrency or Config.MAX_CONCURRENCY
    semaphore = asyncio.Semaphore(limit)

    # Filter out already-processed files (hash-based skip)
    to_process = get_unprocessed_files(conn, scanned_files)

    if not to_process:
        logger.info("All files are already processed. Nothing to do.")
        return []

    logger.info(
        "Processing %d file(s) with concurrency=%d.", len(to_process), limit
    )

    tasks = [
        process_file(f, conn, semaphore, progress_callback)
        for f in to_process
    ]

    results: List[FileRecord] = []
    for coro in asyncio.as_completed(tasks):
        try:
            record = await coro
            results.append(record)
        except Exception as exc:
            # Individual file failure must not abort the batch
            logger.error("Unexpected error processing a file: %s", exc, exc_info=True)

    return results
