"""Helper actions for the download run orchestration."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_data_dir(data_dir: str | Path | None) -> Path:
    """Resolve the data directory path.

    Args:
        data_dir: Optional path string or Path object.

    Returns:
        Resolved Path object.
    """
    return Path(data_dir) if data_dir else Path("data")


def apply_mode(state: object, mode: str) -> None:
    """Reset state if running in full mode.

    Args:
        state: The State object to reset.
        mode: The run mode.
    """
    if mode == "full":
        state.reset()


def log_download_complete(result: dict[str, int]) -> None:
    """Log the final download result.

    Args:
        result: The result dictionary with download statistics.
    """
    logger.info("Download complete: %s", result)


def make_result(downloaded: int, skipped: int, failed: int, total: int) -> dict[str, int]:
    """Build the result dictionary.

    Args:
        downloaded: Number of files downloaded.
        skipped: Number of files skipped.
        failed: Number of files that failed.
        total: Total number of entries.

    Returns:
        Result dictionary.
    """
    return {
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
        "total": total,
    }
