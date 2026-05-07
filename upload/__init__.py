"""Upload module -- upload NYC TLC trip record parquet files to S3-compatible storage."""

from upload.core import (
    S3Client,
    S3ClientError,
    UploadResult,
    UploadState,
    UploadConfig,
    upload,
    upload_from_env,
)

__all__ = [
    "S3Client",
    "S3ClientError",
    "UploadResult",
    "UploadState",
    "UploadConfig",
    "upload",
    "upload_from_env",
]
