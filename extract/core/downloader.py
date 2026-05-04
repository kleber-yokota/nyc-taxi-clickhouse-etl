"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

import requests

from .catalog import Catalog
from .interrupt import InterruptibleDownload
from .known_missing import KnownMissing
from .state import CatalogEntry, DOWNLOAD_TIMEOUT, ErrorType, compute_sha256
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
            downloaded, skipped, failed = _process_entry(
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


def _make_result(downloaded: int, skipped: int, failed: int, total: int) -> dict[str, int]:  # pragma: no mutate
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


def _process_entry(
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
    if known_missing.is_missing(entry.url):
        skipped += 1
        logger.info("Skipping known missing: %s", entry.url)
        return downloaded, skipped, failed

    if state.is_downloaded(entry.url):
        target_path = data_dir / entry.target_dir / entry.filename
        if target_path.exists():
            skipped += 1
            return downloaded, skipped, failed
        state.save(entry.url, "")

    try:
        result = _download_entry(entry, data_dir, state, known_missing)
        if result == "skipped":
            skipped += 1
        elif result == "downloaded":
            downloaded += 1
        elif result == "failed":
            failed += 1
    except Exception as e:
        logger.error("Unexpected error for %s: %s", entry.url, e)
        state.log_error(entry.url, ErrorType.UNKNOWN, str(e))
        failed += 1

    return downloaded, skipped, failed


def _download_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
) -> str:
    """Download a single parquet file.

    Args:
        entry: The catalog entry to download.
        data_dir: Base data directory.
        state: Download state tracker.

    Returns:
        "downloaded", "skipped", or "failed".
    """
    target_path = data_dir / entry.target_dir / entry.filename
    tmp_path = data_dir / entry.target_dir / (entry.filename + ".download.tmp")

    _cleanup_stale_tmp(tmp_path)

    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _fetch_content(entry.url, tmp_path)

        actual_checksum = compute_sha256(tmp_path)

        if target_path.exists():
            existing_checksum = compute_sha256(target_path)
            if existing_checksum == actual_checksum:
                tmp_path.unlink()
                return "skipped"
            _backup_existing_file(target_path)

        tmp_path.rename(target_path)
        state.save(entry.url, actual_checksum)
        return "downloaded"

    except requests.HTTPError as e:
        _handle_http_error(e, entry.url, state, known_missing)
        _safe_unlink(tmp_path)
        return "failed"

    except requests.RequestException as e:
        _handle_network_error(e, entry.url, state)
        _safe_unlink(tmp_path)
        return "failed"

    except Exception as e:
        state.log_error(entry.url, ErrorType.UNKNOWN, str(e))
        _safe_unlink(tmp_path)
        raise


def _fetch_content(url: str, tmp_path: Path) -> None:
    """Fetch content from a URL and stream-write to tmp_path.

    Args:
        url: The URL to fetch.
        tmp_path: Path to write the downloaded content.
    """
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
    response.raise_for_status()
    with open(tmp_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def _handle_http_error(
    e: requests.HTTPError,
    url: str,
    state: State,
    known_missing: KnownMissing,
) -> None:
    """Handle HTTP error and record missing files.

    Args:
        e: The HTTPError.
        url: The URL that failed.
        state: The download state tracker.
        known_missing: Known missing URLs tracker.
    """
    status_code = e.response.status_code
    if status_code == 404:
        state.log_error(url, ErrorType.MISSING_FILE, f"HTTP {status_code}")
        known_missing.add(url)
        logger.error("File not found: %s (HTTP 404) — recording as missing", url)
    else:
        state.log_error(url, ErrorType.HTTP_ERROR, f"HTTP {status_code}")
        logger.error("HTTP error: %s (HTTP %d)", url, status_code)


def _handle_network_error(e: requests.RequestException, url: str, state: State) -> None:
    """Handle network error.

    Args:
        e: The RequestException.
        url: The URL that failed.
        state: The download state tracker.
    """
    state.log_error(url, ErrorType.NETWORK_ERROR, str(type(e).__name__))
    logger.error("Network error for %s: %s", url, type(e).__name__)


def _backup_existing_file(target_path: Path) -> None:  # pragma: no mutate
    """Backup an existing file before overwriting.

    Args:
        target_path: Path to the file to backup.
    """
    backup_path = target_path.with_suffix(target_path.suffix + ".old")
    target_path.rename(backup_path)
    logger.info("Backed up old file: %s -> %s", target_path, backup_path)


def _cleanup_stale_tmp(tmp_path: Path) -> None:  # pragma: no mutate
    """Remove stale temporary download file.

    Args:
        tmp_path: Path to the temporary file.
    """
    if tmp_path.exists():
        tmp_path.unlink()


def _safe_unlink(path: Path) -> None:  # pragma: no mutate
    """Safely unlink a file, ignoring errors.

    Args:
        path: Path to the file to delete.
    """
    if path.exists():
        path.unlink()
