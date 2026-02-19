"""
action_engine.py — Safe, non-destructive file action executor.
Supports dry-run (preview) and apply (execute) modes.
Files are NEVER permanently deleted — "delete" moves files to a .trash folder.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from models import FileRecord

logger = logging.getLogger(__name__)

# ── Action constants ──────────────────────────────────────────────────────────

ACTION_DELETE = "delete"
ACTION_ARCHIVE = "archive"
ACTION_RETAIN = "retain"
ACTION_REVIEW = "review"


class ActionResult:
    """Result of applying a single action."""
    __slots__ = ("file_path", "action", "status", "destination", "error")

    def __init__(
        self,
        file_path: str,
        action: str,
        status: str,  # "dry_run" | "success" | "skipped" | "error"
        destination: str = "",
        error: str = "",
    ):
        self.file_path = file_path
        self.action = action
        self.status = status
        self.destination = destination
        self.error = error

    def to_dict(self) -> Dict[str, str]:
        return {
            "file_path": self.file_path,
            "action": self.action,
            "status": self.status,
            "destination": self.destination,
            "error": self.error,
        }


class ActionEngine:
    """
    Applies retention decisions to files.

    Args:
        dry_run: When True, log intended actions without touching the filesystem.
    """

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _move_file(self, source: str, dest_folder_name: str) -> ActionResult:
        """
        Move *source* into a sibling folder named *dest_folder_name*.
        The destination folder is created if it does not exist.
        In dry_run mode, the intended destination is computed but no files are touched.
        """
        src_path = Path(source)
        # Always compute dest_dir so it is available in both branches
        dest_dir = src_path.parent / dest_folder_name

        # ── Dry-run: compute destination path without touching the filesystem ──
        if self.dry_run:
            return ActionResult(
                file_path=source,
                action=dest_folder_name,
                status="dry_run",
                destination=str(dest_dir / src_path.name),
            )

        # ── Apply mode: validate source exists before moving ──
        if not src_path.exists():
            return ActionResult(
                file_path=source,
                action=dest_folder_name,
                status="error",
                error="Source file not found",
            )

        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / src_path.name
            # Handle name collisions by appending a numeric suffix
            counter = 1
            while dest_path.exists():
                dest_path = dest_dir / f"{src_path.stem}_{counter}{src_path.suffix}"
                counter += 1

            shutil.move(str(src_path), str(dest_path))
            logger.info("Moved '%s' → '%s'.", source, dest_path)
            return ActionResult(
                file_path=source,
                action=dest_folder_name,
                status="success",
                destination=str(dest_path),
            )
        except OSError as exc:
            logger.error("Failed to move '%s': %s", source, exc)
            return ActionResult(
                file_path=source,
                action=dest_folder_name,
                status="error",
                error=str(exc),
            )

    # ── Public API ────────────────────────────────────────────────────────────

    def apply_action(self, record: FileRecord) -> ActionResult:
        """
        Apply the FileRecord's suggested_action.

        Actions:
            delete  → Move to .trash/ (NEVER permanently deleted)
            archive → Move to .archive/
            retain  → No-op
            review  → Flag/log only, no filesystem change
        """
        action = record.suggested_action

        if action == ACTION_DELETE:
            # Safety: move to .trash, not rm — applies even in dry_run
            return self._move_file(record.file_path, ".trash")

        if action == ACTION_ARCHIVE:
            return self._move_file(record.file_path, ".archive")

        if action == ACTION_RETAIN:
            logger.debug("Retaining '%s' — no action taken.", record.file_path)
            return ActionResult(
                file_path=record.file_path,
                action=ACTION_RETAIN,
                status="skipped",
                destination="",
            )

        if action == ACTION_REVIEW:
            logger.info("Flagged for review: '%s'.", record.file_path)
            return ActionResult(
                file_path=record.file_path,
                action=ACTION_REVIEW,
                status="dry_run" if self.dry_run else "skipped",
                destination="",
            )

        # Unknown action — treat as review
        logger.warning("Unknown action '%s' for '%s'. Flagging for review.", action, record.file_path)
        return ActionResult(
            file_path=record.file_path,
            action=action,
            status="skipped",
            error=f"Unrecognized action: {action}",
        )

    def apply_all(
        self,
        records: List[FileRecord],
        action_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Apply actions to all *records*, optionally filtered by *action_filter*.

        Args:
            records:       List of FileRecords from the database.
            action_filter: If provided, only process records matching these actions.

        Returns:
            A summary dict with counts by status.
        """
        if action_filter:
            to_apply = [r for r in records if r.suggested_action in action_filter]
        else:
            to_apply = records

        summary: Dict[str, int] = {"success": 0, "dry_run": 0, "skipped": 0, "error": 0}
        results: List[Dict[str, str]] = []

        for record in to_apply:
            result = self.apply_action(record)
            results.append(result.to_dict())
            summary[result.status] = summary.get(result.status, 0) + 1

        mode = "DRY RUN" if self.dry_run else "APPLY"
        logger.info(
            "[%s] %d file(s) processed. Summary: %s", mode, len(to_apply), summary
        )
        return {"summary": summary, "results": results}
