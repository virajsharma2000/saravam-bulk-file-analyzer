"""
config.py — Central configuration loaded from environment variables.
Create a .env file in the project root or export variables before running.
"""
from __future__ import annotations

import os
try:
    from dotenv import load_dotenv
    # Load variables from a .env file if present
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; rely on OS environment variables only


class Config:
    # ── Sarvam API ──────────────────────────────────────────────────────────
    SARVAM_API_KEY: str = os.getenv("SARVAM_API_KEY", "")

    # Sarvam Document Intelligence endpoint
    SARVAM_DOC_ENDPOINT: str = os.getenv(
        "SARVAM_DOC_ENDPOINT",
        "https://api.sarvam.ai/v1/document-intelligence/extract",
    )

    # Sarvam Chat Completion endpoint
    SARVAM_CHAT_ENDPOINT: str = os.getenv(
        "SARVAM_CHAT_ENDPOINT",
        "https://api.sarvam.ai/v1/chat/completions",
    )

    # LLM model name for chat completion
    LLM_MODEL_NAME: str = os.getenv("LLM_MODEL_NAME", "sarvam-2b")

    # ── Database ─────────────────────────────────────────────────────────────
    DB_PATH: str = os.getenv("DB_PATH", "retention.db")

    # ── Processing ───────────────────────────────────────────────────────────
    # Max concurrent API calls (Semaphore limit)
    MAX_CONCURRENCY: int = int(os.getenv("MAX_CONCURRENCY", "5"))

    # Max characters of extracted text sent to LLM
    MAX_TEXT_CHARS: int = int(os.getenv("MAX_TEXT_CHARS", "2000"))

    # HTTP timeout in seconds for API calls
    HTTP_TIMEOUT: int = int(os.getenv("HTTP_TIMEOUT", "60"))

    # Max retry attempts for 5xx / 429 errors
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

    @classmethod
    def validate(cls) -> list[str]:
        """Return a list of validation error strings (empty = all good)."""
        errors: list[str] = []
        if not cls.SARVAM_API_KEY:
            errors.append("SARVAM_API_KEY is not set.")
        if not cls.SARVAM_DOC_ENDPOINT.startswith("http"):
            errors.append("SARVAM_DOC_ENDPOINT is not a valid URL.")
        if not cls.SARVAM_CHAT_ENDPOINT.startswith("http"):
            errors.append("SARVAM_CHAT_ENDPOINT is not a valid URL.")
        return errors
