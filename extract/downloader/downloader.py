"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

from extract.core.catalog import Catalog
from extract.core.interrupt import InterruptibleDownload
from extract.core.known_missing import KnownMissing
from extract.core.state_manager import State
from extract.downloader.actions import apply_mode
from extract.downloader.actions import log_download_complete
from extract.downloader.actions import make_result
from extract.downloader.actions import resolve_data_dir
from extract.downloader.download import download_and_verify
from extract.downloader.download import handle_download_error
from extract.downloader.download import _fetch_content  # noqa: F401
from extract.downloader.download import _log_http_error
from extract.downloader.ops import process_entry
from extract.downloader.ops import should_skip_download
from extract.downloader.utils import backup_existing_file
from extract.downloader.utils import cleanup_stale_tmp
from extract.downloader.utils import handle_network_error as _handle_network_error
from extract.downloader.utils import safe_unlink

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
    resolved_dir = resolve_data_dir(data_dir)
    catalog = Catalog(types=types, from_year=from_year, to_year=to_year, max_entries=max_entries)
    state = State(resolved_dir / ".download_state.json")
    apply_mode(state, mode)
    known_missing = KnownMissing(resolved_dir / "known_missing.txt")

    entries = catalog.generate()
    if not entries:
        logger.warning("No entries to download.")
        return make_result(0, 0, 0, 0)

    interruptible = InterruptibleDownload(resolved_dir)

    downloaded, skipped, failed = _execute_download_loop(
        entries, resolved_dir, state, known_missing,
    )

    result = make_result(downloaded, skipped, failed, len(entries))
    log_download_complete(result)
    return result


def _execute_download_loop(
    entries: list,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
) -> tuple[int, int, int]:
    """Execute the download loop for all catalog entries.

    Args:
        entries: List of catalog entries to process.
        data_dir: Base data directory.
        state: Download state tracker.
        known_missing: Known missing URLs tracker.

    Returns:
        Tuple of (downloaded, skipped, failed) counts.
    """
    downloaded = 0
    skipped = 0
    failed = 0

    try:
        for entry in entries:
            downloaded, skipped, failed = process_entry(
                entry, data_dir, state, known_missing,
                downloaded, skipped, failed,
            )
    except KeyboardInterrupt:
        _handle_interrupt(data_dir)

    return downloaded, skipped, failed


def _handle_interrupt(data_dir: Path) -> None:
    """Handle keyboard interrupt by cleaning up temporary files.

    Args:
        data_dir: Base data directory for cleanup.
    """
    interruptible = InterruptibleDownload(data_dir)
    interruptible.cleanup()
    logger.info("Download interrupted by user.")
