"""
sarvam_client.py — Async client for Sarvam Document Intelligence API.
Handles base64 encoding, retries on 429 / 5xx, and timeout management.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from pathlib import Path
from typing import Any, Dict

import httpx

from config import Config
from models import ExtractionResult
from utils import exponential_backoff

logger = logging.getLogger(__name__)

# Map file extensions to the API's file_type value
_EXT_TO_TYPE: Dict[str, str] = {
    ".pdf": "pdf",
    ".jpg": "image",
    ".jpeg": "image",
    ".png": "image",
}


def _build_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {Config.SARVAM_API_KEY}",
        "Content-Type": "application/json",
    }


def _build_payload(file_path: str) -> Dict[str, Any]:
    """Read file from disk and encode it into the API request payload."""
    path = Path(file_path)
    suffix = path.suffix.lower()
    file_type = _EXT_TO_TYPE.get(suffix, "image")

    with open(file_path, "rb") as fh:
        encoded = base64.b64encode(fh.read()).decode("utf-8")

    return {
        "file_name": path.name,
        "file_type": file_type,
        "file_content_base64": encoded,
        "extract_tables": True,
        "extract_key_values": True,
    }


async def extract_text(file_path: str) -> ExtractionResult:
    """
    Call the Sarvam Document Intelligence API to extract text from *file_path*.

    Retry strategy:
      - 429 (rate limit): honour Retry-After header or exponential back-off.
      - 5xx (server error): up to Config.MAX_RETRIES attempts with exponential back-off.

    Returns an :class:`ExtractionResult` with an empty text on unrecoverable error.
    """
    try:
        payload = _build_payload(file_path)
    except OSError as exc:
        logger.error("Cannot read file '%s' for extraction: %s", file_path, exc)
        return ExtractionResult(text="", stats={"error": str(exc)})

    last_exc: Exception | None = None

    async with httpx.AsyncClient(timeout=Config.HTTP_TIMEOUT) as client:
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = await client.post(
                    Config.SARVAM_DOC_ENDPOINT,
                    headers=_build_headers(),
                    json=payload,
                )

                if response.status_code == 200:
                    data = response.json()
                    return ExtractionResult(
                        text=data.get("text", ""),
                        stats=data.get("stats", {}),
                    )

                if response.status_code == 429:
                    # Respect Retry-After if present, otherwise back off
                    retry_after = float(
                        response.headers.get("Retry-After", exponential_backoff(attempt))
                    )
                    logger.warning(
                        "Rate-limited on '%s'. Waiting %.1fs (attempt %d/%d).",
                        file_path, retry_after, attempt + 1, Config.MAX_RETRIES,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if response.status_code >= 500:
                    wait = exponential_backoff(attempt)
                    logger.warning(
                        "Server error %d for '%s'. Retrying in %.1fs (attempt %d/%d).",
                        response.status_code, file_path, wait,
                        attempt + 1, Config.MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue

                # 4xx (non-429): not retryable
                logger.error(
                    "Non-retryable error %d for '%s': %s",
                    response.status_code, file_path, response.text[:200],
                )
                return ExtractionResult(
                    text="",
                    stats={"error": f"HTTP {response.status_code}", "detail": response.text[:200]},
                )

            except httpx.TimeoutException as exc:
                wait = exponential_backoff(attempt)
                logger.warning(
                    "Timeout extracting '%s' (attempt %d/%d). Retrying in %.1fs.",
                    file_path, attempt + 1, Config.MAX_RETRIES, wait,
                )
                last_exc = exc
                await asyncio.sleep(wait)

            except httpx.RequestError as exc:
                logger.error("Network error extracting '%s': %s", file_path, exc)
                return ExtractionResult(text="", stats={"error": str(exc)})

    logger.error(
        "All %d extraction attempts failed for '%s'. Last error: %s",
        Config.MAX_RETRIES, file_path, last_exc,
    )
    return ExtractionResult(text="", stats={"error": "Max retries exceeded"})
