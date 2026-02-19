"""
models.py — Pydantic data models for the Bulk File Retention Analyzer.
All models use Pydantic v2 for strict validation.
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field


class ScannedFile(BaseModel):
    """Represents a file discovered during folder scan."""
    file_path: str
    file_hash: str
    file_size: int  # bytes
    last_modified: str  # ISO 8601 string


class ExtractionResult(BaseModel):
    """Result returned by Sarvam Document Intelligence API."""
    text: str = ""
    stats: Dict[str, Any] = Field(default_factory=dict)


class RetentionDecision(BaseModel):
    """Structured retention decision produced by the LLM."""
    retention_score: int = Field(ge=0, le=100)
    category: Literal["legal", "financial", "operational", "personal", "ephemeral", "unknown"] = "unknown"
    suggested_action: Literal["delete", "archive", "retain", "review"] = "review"
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    reasoning: str = ""

    @classmethod
    def fallback(cls, reason: str = "Validation failed") -> "RetentionDecision":
        """Returns a safe fallback when LLM output cannot be parsed/validated."""
        return cls(
            retention_score=50,
            category="unknown",
            suggested_action="review",
            confidence=0.0,
            reasoning=reason,
        )


class FileRecord(BaseModel):
    """Full record as stored in the database."""
    id: Optional[int] = None
    file_path: str
    file_hash: str
    file_size: int
    last_modified: str
    extracted_text: str = ""
    retention_score: int = 0
    category: str = "unknown"
    suggested_action: str = "review"
    confidence: float = 0.0
    reasoning: str = ""
    processed_at: str = ""

    @classmethod
    def from_scan_and_decision(
        cls,
        scanned: ScannedFile,
        extraction: ExtractionResult,
        decision: RetentionDecision,
        processed_at: str,
    ) -> "FileRecord":
        return cls(
            file_path=scanned.file_path,
            file_hash=scanned.file_hash,
            file_size=scanned.file_size,
            last_modified=scanned.last_modified,
            # Security: never persist full extracted text — keep first 500 chars only
            extracted_text=extraction.text[:500] if extraction.text else "",
            retention_score=decision.retention_score,
            category=decision.category,
            suggested_action=decision.suggested_action,
            confidence=decision.confidence,
            reasoning=decision.reasoning,
            processed_at=processed_at,
        )
