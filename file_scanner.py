"""
file_scanner.py — Recursive folder scanner for the Bulk File Retention Analyzer.
Scans for supported file types (jpg, jpeg, png, pdf), computing hash and metadata.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from models import ScannedFile
from utils import compute_sha256

logger = logging.getLogger(__name__)

# Extensions supported by the Sarvam Document Intelligence API
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".pdf"}


def scan_folder(root_path: str) -> List[ScannedFile]:
    """
    Recursively scan *root_path* for supported files.

    Args:
        root_path: Absolute or relative path to the top-level folder.

    Returns:
        A list of :class:`ScannedFile` objects, sorted by file path.
    """
    root = Path(root_path).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Folder not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {root}")

    scanned: List[ScannedFile] = []

    # Collect all matching files recursively
    matched_paths = [
        p
        for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    logger.info(
        "Scanning '%s' — found %d supported file(s).", root, len(matched_paths)
    )

    for file_path in sorted(matched_paths):
        try:
            stat = file_path.stat()
            file_hash = compute_sha256(str(file_path))
            if not file_hash:
                logger.warning("Skipping unreadable file: %s", file_path)
                continue

            last_modified_dt = datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc
            )

            scanned.append(
                ScannedFile(
                    file_path=str(file_path),
                    file_hash=file_hash,
                    file_size=stat.st_size,
                    last_modified=last_modified_dt.isoformat(),
                )
            )
        except OSError as exc:
            logger.error("Error reading '%s': %s", file_path, exc)
            continue

    return scanned
