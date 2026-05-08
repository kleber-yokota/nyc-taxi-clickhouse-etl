"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from extract.core.catalog import Catalog
from extract.core.interrupt import InterruptibleDownload
from extract.core.state_manager import State
from extract.downloader.actions import apply_mode
from extract.downloader.actions import log_download_complete
from extract.downloader.actions import resolve_data_dir
from extract.downloader.download import download_and_verify
from extract.downloader.download import handle_download_error
from extract.downloader.download import _fetch_content  # noqa: F401
from extract.downloader.download import _log_http_error
from extract.downloader.ops import DownloadEntry
from extract.downloader.ops import DownloadResult
from extract.downloader.ops import process_entry
from extract.downloader.ops import should_skip_download


logger = logging.getLogger(__name__)

ChecksumFunc = Callable[[Path], str] | None


def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
    max_entries: int | None = None,
    push_manifest: dict | None = None,
    checksum_func: ChecksumFunc = None,
) -> DownloadResult:
    """Download TLC parquet files according to the specified filters.

    Args:
        data_dir: Base directory for storing data. Defaults to "data".
        types: Data types to download. Defaults to all types.
        from_year: Starting year (inclusive). Defaults to 2009.
        to_year: Ending year (inclusive). Defaults to current year.
        mode: "incremental" (skip downloaded) or "full" (reset state).
        max_entries: Optional limit on entries for testing.
        push_manifest: Push manifest dict for S3 skip check.
        checksum_func: Optional checksum function for verification.

    Returns:
        DownloadResult with download statistics and per-file entries.
    """
    resolved_dir = resolve_data_dir(data_dir)
    catalog = Catalog(types=types, from_year=from_year, to_year=to_year, max_entries=max_entries)
    state = State(resolved_dir / ".download_state.json")
    apply_mode(state, mode)

    catalog_entries = catalog.generate()
    if not catalog_entries:
        logger.warning("No entries to download.")
        return DownloadResult()

    interruptible = InterruptibleDownload(resolved_dir)

    downloaded, skipped, failed, entries = _execute_download_loop(
        catalog_entries, resolved_dir, state, push_manifest, checksum_func,
    )

    result = DownloadResult(
        downloaded=downloaded, skipped=skipped, failed=failed,
        total=len(catalog_entries), entries=entries,
    )
    log_download_complete(result)
    return result


def _execute_download_loop(
    entries: list,
    data_dir: Path,
    state: State,
    push_manifest: dict | None = None,
    checksum_func: ChecksumFunc = None,
) -> tuple[int, int, int, list[DownloadEntry]]:
    """Execute the download loop for all catalog entries.

    Args:
        entries: List of catalog entries to process.
        data_dir: Base data directory.
        state: Download state tracker.
        push_manifest: Push manifest dict for S3 skip check.
        checksum_func: Optional checksum function for verification.

    Returns:
        Tuple of (downloaded, skipped, failed, entries) counts.
    """
    downloaded = 0
    skipped = 0
    failed = 0
    entries_list: list[DownloadEntry] = []

    for catalog_entry in entries:
        downloaded, skipped, failed, entries_list = process_entry(
            catalog_entry, data_dir, state,
            downloaded, skipped, failed,
            push_manifest,
            checksum_func,
            entries_list,
        )

    return downloaded, skipped, failed, entries_list
