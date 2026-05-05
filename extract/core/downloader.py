"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

from .catalog import Catalog
from .downloader_download import download_and_verify
from .downloader_download import handle_download_error
from .downloader_download import _fetch_content  # noqa: F401
from .downloader_ops import process_entry
from .downloader_ops import should_skip_download
from .downloader_util import backup_existing_file
from .downloader_util import cleanup_stale_tmp
from .downloader_util import handle_http_error as _handle_http_error
from .downloader_util import handle_network_error as _handle_network_error
from .downloader_util import safe_unlink
from .interrupt import InterruptibleDownload
from .known_missing import KnownMissing
from .state_manager import State

logger = logging.getLogger(__name__)


def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
    max_entries: int | None = None,
) -> dict[str, int]:
    """Download TLC parquet files according to the specified filters.

    Args:
        data_dir: Base directory for storing data. Defaults to "data".
        types: Data types to download. Defaults to all types.
        from_year: Starting year (inclusive). Defaults to 2009.
        to_year: Ending year (inclusive). Defaults to current year.
        mode: "incremental" (skip downloaded) or "full" (reset state).
        max_entries: Optional limit on entries for testing.

    Returns:
        Dict with keys: downloaded, skipped, failed, total.
    """
    data_dir = _resolve_data_dir(data_dir)
    catalog = Catalog(types=types, from_year=from_year, to_year=to_year, max_entries=max_entries)
    state = State(data_dir / ".download_state.json")
    _apply_mode(state, mode)
    known_missing = KnownMissing(data_dir / "known_missing.txt")

    entries = catalog.generate()
    if not entries:
        logger.warning("No entries to download.")
        return _make_result(0, 0, 0, 0)

    interruptible = InterruptibleDownload(data_dir)

    downloaded = 0
    skipped = 0
    failed = 0
    total = len(entries)

    try:
        for entry in entries:
            downloaded, skipped, failed = process_entry(
                entry, data_dir, state, known_missing,
                downloaded, skipped, failed,
            )
    except KeyboardInterrupt:
        interruptible.cleanup()
        logger.info("Download interrupted by user.")

    result = _make_result(downloaded, skipped, failed, total)
    logger.info("Download complete: %s", result)
    return result


def _resolve_data_dir(data_dir: str | Path | None) -> Path:
    """Resolve the data directory path.

    Args:
        data_dir: Optional path string or Path object.

    Returns:
        Resolved Path object.
    """
    return Path(data_dir) if data_dir else Path("data")


def _apply_mode(state: State, mode: str) -> None:
    """Reset state if running in full mode.

    Args:
        state: The State object to reset.
        mode: The run mode.
    """
    if mode == "full":
        state.reset()


def _make_result(downloaded: int, skipped: int, failed: int, total: int) -> dict[str, int]:
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


# Aliases para compatibilidade com testes
_backup_existing_file = backup_existing_file
_cleanup_stale_tmp = cleanup_stale_tmp
_download_entry = download_and_verify
_handle_download_error = handle_download_error
_make_result = _make_result
_process_entry = process_entry
_resolve_data_dir = _resolve_data_dir
_safe_unlink = safe_unlink
should_skip_download = should_skip_download

# Import helpers for test compatibility
from .downloader_download import _fetch_content  # noqa: F401

