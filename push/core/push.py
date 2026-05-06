"""Upload orchestration — coordinates file collection, upload, and state tracking."""

from __future__ import annotations

import logging
from pathlib import Path

from .checksum import compute_sha256
from .client import S3Client
from .filter import collect_files
from .state import PushedEntry
from .state import PushResult
from .state import PushState
from .state import UploadConfig

logger = logging.getLogger(__name__)


def upload(
    data_dir: str | Path,
    client: S3Client,
    state: PushState,
    config: UploadConfig | None = None,
) -> PushResult:
    """Upload files from data_dir to S3.

    Walks the data/ directory tree and uploads files to S3, respecting
    include/exclude filters and push state tracking.

    Args:
        data_dir: Path to the local data directory.
        client: Configured S3Client instance.
        state: PushState instance for tracking uploads.
        config: Upload configuration (include/exclude patterns, overwrite flag).

    Returns:
        PushResult with uploaded/skipped/failed/total counts.
    """
    data_dir = Path(data_dir)
    if not data_dir.exists():
        logger.warning("Data directory does not exist: %s", data_dir)
        return PushResult()

    config = config if config is not None else UploadConfig()
    files = collect_files(data_dir, config.include, config.exclude)
    if not files:
        logger.info("No files to upload in %s", data_dir)
        return PushResult()

    total = len(files)
    uploaded = 0
    skipped = 0
    failed = 0
    uploaded_files: list[str] = []

    uploaded_entries: list[PushedEntry] = []

    for local_path in files:
        try:
            status, checksum = _upload_one(local_path, data_dir, client, state, config)
            if status == "uploaded":
                uploaded += 1
                rel_path = str(local_path.relative_to(data_dir))
                uploaded_files.append(rel_path)
                s3_key = client.build_key(rel_path)
                uploaded_entries.append(PushedEntry(
                    rel_path=rel_path,
                    s3_key=s3_key,
                    checksum=checksum,
                ))
            elif status == "skipped":
                skipped += 1
        except Exception as e:
            logger.error("Unexpected error uploading %s: %s", local_path, e)
            failed += 1

    state.save()
    return PushResult(
        uploaded=uploaded,
        skipped=skipped,
        failed=failed,
        total=total,
        uploaded_files=uploaded_files,
        uploaded_entries=uploaded_entries,
    )


def _upload_one(
    local_path: Path,
    data_dir: Path,
    client: S3Client,
    state: PushState,
    config: UploadConfig,
) -> tuple[str, str]:
    """Upload a single file to S3.

    Args:
        local_path: Path to the local file.
        data_dir: Base data directory.
        client: S3Client instance.
        state: PushState instance.
        config: Upload configuration.

    Returns:
        Tuple of (status, checksum) where status is "uploaded" or "skipped".
    """
    rel_path = str(local_path.relative_to(data_dir))
    checksum = compute_sha256(local_path)

    if _should_skip(local_path, checksum, state, config.overwrite):
        logger.debug("Skipping (already pushed): %s", rel_path)
        return "skipped", checksum

    s3_key = client.build_key(rel_path)
    _do_upload(local_path, s3_key, client)
    state.record_push(str(local_path), s3_key, checksum)
    logger.info("Uploaded: %s -> s3://%s/%s", rel_path, client.bucket, s3_key)
    if config.delete_after_push:
        local_path.unlink()
        logger.info("Deleted local file after push: %s", rel_path)
    return "uploaded", checksum


def _should_skip(
    local_path: Path,
    checksum: str,
    state: PushState,
    overwrite: bool,
) -> bool:
    """Check if a file should be skipped (already pushed with same checksum).

    Args:
        local_path: Path to the local file.
        checksum: SHA-256 checksum of the file.
        state: PushState instance.
        overwrite: Whether to overwrite existing uploads.

    Returns:
        True if the file should be skipped.
    """
    return not overwrite and state.is_pushed(str(local_path), checksum)


def _do_upload(local_path: Path, s3_key: str, client: S3Client) -> None:
    """Perform the actual file upload to S3.

    Args:
        local_path: Path to the local file.
        s3_key: S3 key where the file will be uploaded.
        client: S3Client instance.
    """
    with open(local_path, "rb") as f:
        client.upload_fileobj(s3_key, f)
