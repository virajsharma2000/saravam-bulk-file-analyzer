"""
utils.py — Shared helper utilities for the Bulk File Retention Analyzer.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ── Hashing ──────────────────────────────────────────────────────────────────

def compute_sha256(path: str) -> str:
    """
    Compute the SHA-256 digest of a file using a streaming read
    to avoid loading large files fully into memory.
    """
    sha256 = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
    except OSError as exc:
        logger.error("Cannot hash %s: %s", path, exc)
        return ""
    return sha256.hexdigest()


# ── Text helpers ──────────────────────────────────────────────────────────────

def truncate_text(text: str, max_chars: int = 2000) -> str:
    """Return at most *max_chars* characters from *text*."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n... [truncated — {len(text) - max_chars} chars omitted]"


# ── JSON helpers ──────────────────────────────────────────────────────────────

def safe_json_parse(raw: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to parse *raw* as JSON.
    Returns the parsed dict, or None if parsing fails.
    Handles common LLM wrapper patterns like ```json ... ```.
    """
    if not raw:
        return None
    # Strip common markdown fences
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        # Remove first and last fence lines
        cleaned = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
    try:
        result = json.loads(cleaned)
        if isinstance(result, dict):
            return result
        logger.warning("Parsed JSON is not a dict: %s", type(result))
        return None
    except json.JSONDecodeError as exc:
        logger.warning("JSON parse error: %s", exc)
        return None


# ── File size formatting ──────────────────────────────────────────────────────

def format_file_size(size_bytes: int) -> str:
    """Return a human-readable file size string (e.g. '1.2 MB')."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0  # type: ignore[assignment]
    return f"{size_bytes:.1f} PB"


# ── Retry / backoff helper ────────────────────────────────────────────────────

def exponential_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Return wait time in seconds for a given attempt (0-indexed)."""
    delay = min(base * (2 ** attempt), cap)
    return delay
