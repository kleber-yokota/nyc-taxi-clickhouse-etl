"""Download operations — orchestration helpers."""

from __future__ import annotations

import logging
from pathlib import Path

from extract.core.known_missing import KnownMissing
from extract.core.push_manifest import is_pushed_in_manifest
from extract.core.state import CatalogEntry
from extract.core.state_manager import State
from extract.downloader.downloader_download import download_and_verify
from extract.downloader.downloader_download import handle_download_error

logger = logging.getLogger(__name__)





def should_skip_download(
    entry: CatalogEntry,
    state: State,
    known_missing: KnownMissing,
    data_dir: Path,
    push_manifest: dict | None = None,
) -> bool:
    """Check if a download should be skipped.

    Args:
        entry: The catalog entry to check.
        state: Download state tracker.
        known_missing: Known missing URLs tracker.
        data_dir: Base data directory.
        push_manifest: Push manifest dict for S3 skip check.

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

    if is_pushed_in_manifest(push_manifest, entry.target_dir, entry.year, entry.month):
        logger.info("Skipping (already in S3): %s", entry.url)
        return True

    return False


def process_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
    downloaded: int,
    skipped: int,
    failed: int,
    push_manifest: dict | None = None,
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
        push_manifest: Push manifest dict for S3 skip check.

    Returns:
        Updated (downloaded, skipped, failed) counts.
    """
    if should_skip_download(entry, state, known_missing, data_dir, push_manifest):
        skipped += 1
        return downloaded, skipped, failed

    try:
        download_result = download_and_verify(entry, data_dir, state, known_missing)
        if download_result == "skipped":
            skipped += 1
        elif download_result == "downloaded":
            downloaded += 1
        elif download_result == "failed":
            failed += 1
    except Exception as e:
        handle_download_error(e, entry, state, known_missing)
        failed += 1

    return downloaded, skipped, failed
