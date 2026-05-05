"""Download operations — orchestration helpers."""

from __future__ import annotations

import logging
from pathlib import Path

from .downloader_download import download_and_verify
from .downloader_download import handle_download_error
from .known_missing import KnownMissing
from .state import CatalogEntry
from .state_manager import State

logger = logging.getLogger(__name__)





def should_skip_download(
    entry: CatalogEntry,
    state: State,
    known_missing: KnownMissing,
    data_dir: Path,
) -> bool:
    """Check if a download should be skipped.

    Args:
        entry: The catalog entry to check.
        state: Download state tracker.
        known_missing: Known missing URLs tracker.
        data_dir: Base data directory.

    Returns:
        True if the download should be skipped.
    """
    if known_missing.is_missing(entry.url):
        logger.info("Skipping known missing: %s", entry.url)
        return True

    if state.is_downloaded(entry.url):
        target_path = data_dir / entry.target_dir / entry.filename
        if target_path.exists():
            return True
        state.save(entry.url, "")

    return False


def process_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
    downloaded: int,
    skipped: int,
    failed: int,
) -> tuple[int, int, int]:
    """Process a single catalog entry.

    Args:
        entry: The catalog entry to process.
        data_dir: Base data directory.
        state: Download state tracker.
        known_missing: Known missing URLs tracker.
        downloaded: Current download count.
        skipped: Current skip count.
        failed: Current failure count.

    Returns:
        Updated (downloaded, skipped, failed) counts.
    """
    if should_skip_download(entry, state, known_missing, data_dir):
        skipped += 1
        return downloaded, skipped, failed

    try:
        result = download_and_verify(entry, data_dir, state, known_missing)
        if result == "skipped":
            skipped += 1
        elif result == "downloaded":
            downloaded += 1
        elif result == "failed":
            failed += 1
    except Exception as e:
        handle_download_error(e, entry, state, known_missing)
        failed += 1

    return downloaded, skipped, failed
