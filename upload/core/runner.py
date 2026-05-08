"""Entry point — upload files using environment variables."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

from .client import S3Client
from .engine import upload, recover_from_s3
from .state import UploadResult, UploadState, UploadConfig

ChecksumFunc = Callable[[Path], str] | None


def upload_from_env(
    data_dir: str | Path,
    config: UploadConfig | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    endpoint_url: str | None = None,
    checksum_func: ChecksumFunc = None,
) -> UploadResult:
    """Upload files using environment variables for configuration."""
    bucket = _resolve_bucket(bucket)
    prefix = prefix or os.environ.get("S3_PREFIX", "data")
    endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL")
    client = S3Client.from_env(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)
    state = UploadState(Path(data_dir) / ".upload_state.json")
    return upload(data_dir=data_dir, client=client, state=state, config=config, checksum_func=checksum_func)


def _resolve_bucket(bucket: str | None) -> str:
    """Resolve bucket name from argument or environment."""
    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET must be set via environment or as argument")
    return bucket


def get_existing_uploads(data_dir: str | Path) -> dict:
    """Return {rel_path: {s3_key, checksum}} for existing uploads.

    Generic — doesn't assume S3 or any specific storage backend.
    """
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        return {}

    client = _create_s3_client(data_dir)
    state = recover_from_s3(data_dir, client)
    return _entries_to_dict(state, data_dir)


def _create_s3_client(data_dir: str | Path) -> S3Client:
    """Create S3 client from environment variables."""
    prefix = os.environ.get("S3_PREFIX", "data")
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    bucket = os.environ.get("S3_BUCKET") or ""
    return S3Client.from_env(bucket=bucket, prefix=prefix, endpoint_url=endpoint)


def _entries_to_dict(state, data_dir: str | Path) -> dict:
    """Convert UploadState entries to relative-path dict."""
    data_dir_path = Path(data_dir)
    return {
        str(Path(local_path).relative_to(data_dir_path)): info
        for local_path, info in state.get_entries().items()
    }
