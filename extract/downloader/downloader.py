"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

from extract.core.catalog import Catalog
from extract.core.interrupt import InterruptibleDownload
from extract.core.known_missing import KnownMissing
from extract.core.push_manifest import load_push_manifest
from extract.core.state_manager import State
from extract.downloader.downloader_actions import apply_mode
from extract.downloader.downloader_actions import log_download_complete
from extract.downloader.downloader_actions import make_result
from extract.downloader.downloader_actions import resolve_data_dir
from extract.downloader.downloader_download import download_and_verify
from extract.downloader.downloader_download import handle_download_error
from extract.downloader.downloader_download import _fetch_content  # noqa: F401
from extract.downloader.downloader_ops import process_entry
from extract.downloader.downloader_ops import should_skip_download
from extract.downloader.downloader_util import backup_existing_file
from extract.downloader.downloader_util import cleanup_stale_tmp
from extract.downloader.downloader_util import handle_http_error as _handle_http_error
from extract.downloader.downloader_util import handle_network_error as _handle_network_error
from extract.downloader.downloader_util import safe_unlink

logger = logging.getLogger(__name__)


def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
    max_entries: int | None = None,
    push_manifest: dict | None = None,
) -> dict[str, int]:
    """Download TLC parquet files according to the specified filters.

    Args:
        data_dir: Base directory for storing data. Defaults to "data".
        types: Data types to download. Defaults to all types.
        from_year: Starting year (inclusive). Defaults to 2009.
        to_year: Ending year (inclusive). Defaults to current year.
        mode: "incremental" (skip downloaded) or "full" (reset state).
        max_entries: Optional limit on entries for testing.
        push_manifest: Push manifest dict for S3 skip check.

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

    if push_manifest is None:
        push_manifest = load_push_manifest(resolved_dir)

    interruptible = InterruptibleDownload(resolved_dir)

    downloaded, skipped, failed = _execute_download_loop(
        entries, resolved_dir, state, known_missing, push_manifest,
    )

    result = make_result(downloaded, skipped, failed, len(entries))
    log_download_complete(result)
    return result


def _execute_download_loop(
    entries: list,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
    push_manifest: dict | None = None,
) -> tuple[int, int, int]:
    """Execute the download loop for all catalog entries.

    Args:
        entries: List of catalog entries to process.
        data_dir: Base data directory.
        state: Download state tracker.
        known_missing: Known missing URLs tracker.
        push_manifest: Push manifest dict for S3 skip check.

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
                push_manifest,
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


# Aliases para compatibilidade com testes
_backup_existing_file = backup_existing_file
_cleanup_stale_tmp = cleanup_stale_tmp
_download_entry = download_and_verify
_handle_download_error = handle_download_error
_make_result = make_result
_process_entry = process_entry
_resolve_data_dir = resolve_data_dir
_safe_unlink = safe_unlink
should_skip_download = should_skip_download
