"""Download and verification operations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import requests

from extract.downloader.utils import backup_existing_file
from extract.downloader.utils import cleanup_stale_tmp
from extract.downloader.utils import safe_unlink
from extract.core.state import CatalogEntry, ErrorType, compute_sha256
from extract.core.state_manager import State

ChecksumFunc = Callable[[Path], str] | None

logger = logging.getLogger(__name__)


def download_and_verify(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    checksum_func: ChecksumFunc = None,
) -> str:
    """Download a file and verify its checksum.

    Args:
        entry: The catalog entry to download.
        data_dir: Base data directory.
        state: Download state tracker.
        checksum_func: Optional checksum function for verification.

    Returns:
        "downloaded", "skipped", or "failed".
    """
    target_path = data_dir / entry.data_type / entry.filename
    tmp_path = data_dir / entry.data_type / (entry.filename + ".download.tmp")

    cleanup_stale_tmp(tmp_path)

    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _fetch_content(entry.url, tmp_path)

        actual_checksum = checksum_func(tmp_path) if checksum_func else compute_sha256(tmp_path)

        if target_path.exists():
            existing_checksum = checksum_func(target_path) if checksum_func else compute_sha256(target_path)
            if existing_checksum == actual_checksum:
                tmp_path.unlink()
                return "skipped"
            backup_existing_file(target_path)

        tmp_path.rename(target_path)
        state.save(entry.url, actual_checksum)
        return "downloaded"

    except requests.HTTPError as e:
        safe_unlink(tmp_path)
        _log_http_error(e, entry.url, state)
        return "failed"

    except requests.RequestException as e:
        safe_unlink(tmp_path)
        state.log_error(entry.url, ErrorType.NETWORK_ERROR, str(type(e).__name__))
        logger.error("Network error for %s: %s", entry.url, type(e).__name__)
        return "failed"

    except Exception as e:
        safe_unlink(tmp_path)
        state.log_error(entry.url, ErrorType.UNKNOWN, f"Unexpected error: {type(e).__name__}")
        logger.exception("Unexpected error downloading %s", entry.url)
        return "failed"


def handle_download_error(
    e: Exception,
    entry: CatalogEntry,
    state: State,
) -> None:
    """Handle download errors and record them.

    Args:
        e: The exception that occurred.
        entry: The catalog entry that failed.
        state: Download state tracker.
    """
    if isinstance(e, requests.HTTPError):
        status_code = e.response.status_code
        if status_code == 404:
            state.log_error(entry.url, ErrorType.MISSING_FILE, f"HTTP {status_code}")
            logger.error("File not found: %s (HTTP 404) — recording as missing", entry.url)
        else:
            state.log_error(entry.url, ErrorType.HTTP_ERROR, f"HTTP {status_code}")
            logger.error("HTTP error: %s (HTTP %d)", entry.url, status_code)
    elif isinstance(e, requests.RequestException):
        state.log_error(entry.url, ErrorType.NETWORK_ERROR, str(type(e).__name__))
        logger.error("Network error for %s: %s", entry.url, type(e).__name__)
    else:
        state.log_error(entry.url, ErrorType.UNKNOWN, str(e))
        logger.error("Unexpected error for %s: %s", entry.url, e)


def _log_http_error(e: requests.HTTPError, url: str, state: State) -> None:
    """Log HTTP error and record missing files.

    Args:
        e: The HTTPError.
        url: The URL that failed.
        state: The download state tracker.
    """
    if not isinstance(e, requests.HTTPError):
        return
    status_code = e.response.status_code
    if status_code == 404:
        state.log_error(url, ErrorType.MISSING_FILE, f"HTTP {status_code}")
        logger.error("File not found: %s (HTTP 404) — recording as missing", url)
    else:
        state.log_error(url, ErrorType.HTTP_ERROR, f"HTTP {status_code}")
        logger.error("HTTP error: %s (HTTP %d)", url, status_code)


def _fetch_content(url: str, tmp_path: Path) -> None:
    """Fetch content from a URL and stream-write to tmp_path.

    Args:
        url: The URL to fetch.
        tmp_path: Path to write the downloaded content.
    """
    response = requests.get(url, timeout=300, stream=True)
    response.raise_for_status()
    with open(tmp_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
