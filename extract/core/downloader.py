"""Core download logic for TLC parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

import httpx

from .catalog import Catalog
from .interrupt import InterruptibleDownload
from .state import CatalogEntry, DOWNLOAD_TIMEOUT, ErrorType, compute_sha256
from .state_manager import State

logger = logging.getLogger(__name__)


def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
) -> dict[str, int]:
    data_dir = Path(data_dir) if data_dir else Path("data")
    catalog = Catalog(types=types, from_year=from_year, to_year=to_year)
    state = State(data_dir / ".download_state.json")

    if mode == "full":
        state.reset()

    entries = catalog.generate()
    if not entries:
        logger.warning("No entries to download.")
        return {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    interruptible = InterruptibleDownload(data_dir)
    interruptible._setup_handlers()

    downloaded = 0
    skipped = 0
    failed = 0
    total = len(entries)

    try:
        for entry in entries:
            if state.is_downloaded(entry.url):
                target_path = data_dir / entry.target_dir / entry.filename
                if target_path.exists():
                    skipped += 1
                    continue
                else:
                    state.save(entry.url, "")

            try:
                result = _download_entry(
                    entry, data_dir, state, interruptible
                )
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
    except KeyboardInterrupt:
        interruptible.cleanup()
        logger.info("Download interrupted by user.")
    finally:
        interruptible._cleanup_tmp()

    result = {
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
        "total": total,
    }
    logger.info("Download complete: %s", result)
    return result


def _download_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    interruptible: InterruptibleDownload,
) -> str:
    target_path = data_dir / entry.target_dir / entry.filename
    tmp_path = data_dir / entry.target_dir / (entry.filename + ".download.tmp")

    if tmp_path.exists():
        tmp_path.unlink()

    try:
        with httpx.Client(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
            response = client.get(entry.url)
            response.raise_for_status()
            content = response.content

        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_bytes(content)

        actual_checksum = compute_sha256(tmp_path)

        if target_path.exists():
            existing_checksum = compute_sha256(target_path)
            if existing_checksum == actual_checksum:
                tmp_path.unlink()
                return "skipped"
            else:
                backup_path = target_path.with_suffix(target_path.suffix + ".old")
                target_path.rename(backup_path)
                logger.info("Backed up old file: %s -> %s", target_path, backup_path)

        tmp_path.rename(target_path)
        state.save(entry.url, actual_checksum)
        return "downloaded"

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        if status_code == 404:
            state.log_error(entry.url, ErrorType.MISSING_FILE, f"HTTP {status_code}")
        else:
            state.log_error(entry.url, ErrorType.HTTP_ERROR, f"HTTP {status_code}")
        if tmp_path.exists():
            tmp_path.unlink()
        return "failed"

    except httpx.RequestError as e:
        state.log_error(entry.url, ErrorType.NETWORK_ERROR, str(type(e).__name__))
        if tmp_path.exists():
            tmp_path.unlink()
        return "failed"

    except Exception as e:
        state.log_error(entry.url, ErrorType.UNKNOWN, str(e))
        if tmp_path.exists():
            tmp_path.unlink()
        raise
