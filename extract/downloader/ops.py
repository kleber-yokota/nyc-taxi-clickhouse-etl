"""Download operations — orchestration helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from extract.downloader.download import download_and_verify
from extract.downloader.download import handle_download_error
from extract.core.state import CatalogEntry
from extract.core.state_manager import State

ChecksumFunc = Callable[[Path], str] | None


@dataclass(frozen=True)
class DownloadEntry:
    """Metadata for a single downloaded file."""
    rel_path: str
    checksum: str


@dataclass(frozen=True)
class DownloadResult:
    """Result of a download operation."""
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0
    entries: list[DownloadEntry] = field(default_factory=list)


logger = logging.getLogger(__name__)


def should_skip_download(
    entry: CatalogEntry,
    state: State,
    data_dir: Path,
    push_manifest: dict | None = None,
) -> bool:
    """Check if a download should be skipped.

    Args:
        entry: The catalog entry to check.
        state: Download state tracker.
        data_dir: Base data directory.
        push_manifest: Push manifest dict for S3 skip check.

    Returns:
        True if the download should be skipped.
    """
    if state.is_downloaded(entry.url):
        target_path = data_dir / entry.data_type / entry.filename
        if target_path.exists():
            return True
        state.save(entry.url, "")

    if push_manifest and _is_in_push_manifest(push_manifest, entry):
        logger.info("Skipping (already in manifest): %s", entry.url)
        return True

    return False


def _is_in_push_manifest(push_manifest: dict, entry: CatalogEntry) -> bool:
    """Check if entry is in push manifest."""
    rel_path = f"{entry.data_type}/{entry.filename}"
    return rel_path in push_manifest


def process_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    downloaded: int,
    skipped: int,
    failed: int,
    push_manifest: dict | None = None,
    checksum_func: ChecksumFunc = None,
    entries: list[DownloadEntry] | None = None,
) -> tuple[int, int, int, list[DownloadEntry]]:
    """Process a single catalog entry.

    Args:
        entry: The catalog entry to process.
        data_dir: Base data directory.
        state: Download state tracker.
        downloaded: Current download count.
        skipped: Current skip count.
        failed: Current failure count.
        push_manifest: Push manifest dict for S3 skip check.
        checksum_func: Optional checksum function for verification.
        entries: List to append DownloadEntry to.

    Returns:
        Updated (downloaded, skipped, failed, entries).
    """
    if should_skip_download(entry, state, data_dir, push_manifest):
        skipped += 1
        return downloaded, skipped, failed, entries or []

    try:
        download_result = download_and_verify(entry, data_dir, state, checksum_func=checksum_func)
        if download_result == "skipped":
            skipped += 1
        elif download_result == "downloaded":
            downloaded += 1
            if entries is not None:
                rel_path = f"{entry.data_type}/{entry.filename}"
                checksum = state.get_checksum(entry.url) or ""
                entries.append(DownloadEntry(rel_path=rel_path, checksum=checksum))
        elif download_result == "failed":
            failed += 1
    except Exception as e:
        handle_download_error(e, entry, state)
        failed += 1

    return downloaded, skipped, failed, entries or []
