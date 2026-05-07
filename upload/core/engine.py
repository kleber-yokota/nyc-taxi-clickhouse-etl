"""Upload orchestration — coordinates file collection, upload, and state tracking."""

from __future__ import annotations

import logging
from pathlib import Path

from .checksum import compute_sha256
from .client import S3Client
from .filter import collect_files
from .state import UploadResult, UploadState, UploadConfig

logger = logging.getLogger(__name__)


def upload(
    data_dir: str | Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig | None = None,
) -> UploadResult:
    """Upload files from data_dir to S3."""
    data_dir = Path(data_dir)
    if not data_dir.exists():
        logger.warning("Data directory does not exist: %s", data_dir)
        return UploadResult()

    config = config if config is not None else UploadConfig()
    files = collect_files(data_dir, config.include, config.exclude)
    if not files:
        logger.info("No files to upload in %s", data_dir)
        return UploadResult()

    total = len(files)
    uploaded = skipped = failed = 0
    uploaded_files: list[str] = []

    for local_path in files:
        try:
            status = _upload_one(local_path, data_dir, client, state, config)
            if status == "uploaded":
                uploaded += 1
                uploaded_files.append(str(local_path.relative_to(data_dir)))
            else:
                skipped += 1
        except Exception as e:
            logger.error("Unexpected error uploading %s: %s", local_path, e)
            failed += 1

    state.save()
    return UploadResult(
        uploaded=uploaded, skipped=skipped, failed=failed,
        total=total, uploaded_files=uploaded_files,
    )


def _upload_one(
    local_path: Path,
    data_dir: Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
) -> str:
    """Upload a single file to S3."""
    rel_path = str(local_path.relative_to(data_dir))
    checksum = compute_sha256(local_path)

    if _should_skip(local_path, checksum, state, config.overwrite):
        logger.debug("Skipping (already uploaded): %s", rel_path)
        return "skipped"

    s3_key = client.build_key(rel_path)
    _do_upload(local_path, s3_key, client)
    state.record_upload(str(local_path), s3_key, checksum)
    logger.info("Uploaded: %s -> s3://%s/%s", rel_path, client.bucket, s3_key)
    if config.delete_after_upload:
        local_path.unlink()
        logger.info("Deleted local file after upload: %s", rel_path)
    return "uploaded"


def _should_skip(local_path: Path, checksum: str, state: UploadState, overwrite: bool) -> bool:
    """Check if a file should be skipped."""
    return not overwrite and state.is_uploaded(str(local_path), checksum)


def _do_upload(local_path: Path, s3_key: str, client: S3Client) -> None:
    """Perform the actual file upload to S3."""
    with open(local_path, "rb") as f:
        client.upload_fileobj(s3_key, f)
