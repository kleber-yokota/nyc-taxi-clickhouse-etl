"""File utility operations for download."""

from __future__ import annotations

import logging
from pathlib import Path

from extract.core.state import ErrorType
from extract.core.state_manager import State

logger = logging.getLogger(__name__)


def backup_existing_file(target_path: Path) -> None:
    """Backup an existing file before overwriting.

    Args:
        target_path: Path to the file to backup.
    """
    backup_path = target_path.with_suffix(target_path.suffix + ".old")
    target_path.rename(backup_path)
    logger.info("Backed up old file: %s -> %s", target_path, backup_path)


def cleanup_stale_tmp(tmp_path: Path) -> None:
    """Remove stale temporary download file.

    Args:
        tmp_path: Path to the temporary file.
    """
    if tmp_path.exists():
        tmp_path.unlink()


def safe_unlink(path: Path) -> None:
    """Safely unlink a file, ignoring errors.

    Args:
        path: Path to the file to delete.
    """
    if path.exists():
        path.unlink()


def handle_network_error(e: Exception, url: str, state: State) -> None:
    """Handle network error (test compatibility).

    Args:
        e: The RequestException.
        url: The URL that failed.
        state: The download state tracker.
    """
    state.log_error(url, ErrorType.NETWORK_ERROR, str(type(e).__name__))
    logger.error("Network error for %s: %s", url, type(e).__name__)
