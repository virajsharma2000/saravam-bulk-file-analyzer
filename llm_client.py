"""
llm_client.py — Async client for Sarvam Chat Completion API.
Builds the structured retention classification prompt and validates the JSON response.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

import httpx

from config import Config
from models import ExtractionResult, RetentionDecision, ScannedFile
from utils import exponential_backoff, format_file_size, safe_json_parse, truncate_text

logger = logging.getLogger(__name__)

# ── Prompt builder ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are a data retention classification engine. "
    "Always respond in strict JSON with these exact fields:\n"
    "retention_score (integer 0-100), "
    "category (one of: legal, financial, operational, personal, ephemeral, unknown), "
    "suggested_action (one of: delete, archive, retain, review), "
    "confidence (float 0.0-1.0), "
    "reasoning (string with detailed explanation)."
)


def _build_user_message(
    scanned: ScannedFile,
    extraction: ExtractionResult,
) -> str:
    """Compose the user message sent to the LLM for classification."""
    text_preview = truncate_text(extraction.text, Config.MAX_TEXT_CHARS)
    word_count = extraction.stats.get("word_count", "N/A")
    page_count = extraction.stats.get("page_count", "N/A")

    return (
        f"Classify the following document for data retention purposes.\n\n"
        f"**File Metadata:**\n"
        f"- Path: {scanned.file_path}\n"
        f"- Size: {format_file_size(scanned.file_size)}\n"
        f"- Last Modified: {scanned.last_modified}\n"
        f"- Word Count: {word_count}\n"
        f"- Page Count: {page_count}\n\n"
        f"**Extracted Text Preview (first {Config.MAX_TEXT_CHARS} chars):**\n"
        f"```\n{text_preview}\n```\n\n"
        "Based on the above, provide a JSON retention decision."
    )


def _build_payload(user_message: str) -> Dict[str, Any]:
    return {
        "model": Config.LLM_MODEL_NAME,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
    }


def _build_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {Config.SARVAM_API_KEY}",
        "Content-Type": "application/json",
    }


# ── Response validator ────────────────────────────────────────────────────────

def _validate_decision(data: Dict[str, Any]) -> RetentionDecision:
    """
    Validate and parse the raw dictionary into a RetentionDecision.
    Returns a fallback decision if required fields are missing or invalid.
    """
    required = {"retention_score", "category", "suggested_action", "confidence", "reasoning"}
    missing = required - data.keys()
    if missing:
        logger.warning("LLM response missing fields: %s", missing)
        return RetentionDecision.fallback(f"Missing fields: {missing}")

    try:
        return RetentionDecision(**{k: data[k] for k in required})
    except Exception as exc:  # Pydantic ValidationError or type errors
        logger.warning("RetentionDecision validation failed: %s", exc)
        return RetentionDecision.fallback(str(exc))


# ── Main entry point ──────────────────────────────────────────────────────────

async def classify_document(
    scanned: ScannedFile,
    extraction: ExtractionResult,
) -> RetentionDecision:
    """
    Send document metadata + text preview to the Chat Completion API
    and return a validated :class:`RetentionDecision`.

    Retry on 429 and 5xx errors using exponential back-off.
    """
    user_message = _build_user_message(scanned, extraction)
    payload = _build_payload(user_message)
    last_exc: Exception | None = None

    async with httpx.AsyncClient(timeout=Config.HTTP_TIMEOUT) as client:
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = await client.post(
                    Config.SARVAM_CHAT_ENDPOINT,
                    headers=_build_headers(),
                    json=payload,
                )

                if response.status_code == 200:
                    body = response.json()
                    # Extract content from OpenAI-compatible response format
                    raw_content: str = (
                        body.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    )
                    parsed = safe_json_parse(raw_content)
                    if parsed is None:
                        logger.warning(
                            "Could not parse LLM JSON for '%s'. Raw: %.200s",
                            scanned.file_path, raw_content,
                        )
                        return RetentionDecision.fallback("Invalid JSON from LLM")
                    return _validate_decision(parsed)

                if response.status_code == 429:
                    wait = float(
                        response.headers.get("Retry-After", exponential_backoff(attempt))
                    )
                    logger.warning(
                        "LLM rate-limited for '%s', waiting %.1fs (attempt %d/%d).",
                        scanned.file_path, wait, attempt + 1, Config.MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code >= 500:
                    wait = exponential_backoff(attempt)
                    logger.warning(
                        "LLM server error %d for '%s'. Retrying in %.1fs (attempt %d/%d).",
                        response.status_code, scanned.file_path, wait,
                        attempt + 1, Config.MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue

                # Non-retryable 4xx
                logger.error(
                    "LLM non-retryable error %d for '%s': %s",
                    response.status_code, scanned.file_path, response.text[:200],
                )
                return RetentionDecision.fallback(f"HTTP {response.status_code}")

            except httpx.TimeoutException as exc:
                wait = exponential_backoff(attempt)
                logger.warning(
                    "LLM timeout for '%s' (attempt %d/%d). Retrying in %.1fs.",
                    scanned.file_path, attempt + 1, Config.MAX_RETRIES, wait,
                )
                last_exc = exc
                await asyncio.sleep(wait)

            except httpx.RequestError as exc:
                logger.error("LLM network error for '%s': %s", scanned.file_path, exc)
                return RetentionDecision.fallback(f"Network error: {exc}")

    logger.error(
        "All %d LLM attempts failed for '%s'. Last: %s",
        Config.MAX_RETRIES, scanned.file_path, last_exc,
    )
    return RetentionDecision.fallback("Max retries exceeded")
