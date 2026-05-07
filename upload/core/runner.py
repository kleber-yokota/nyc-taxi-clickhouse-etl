"""Entry point — upload files using environment variables."""

from __future__ import annotations

import os
from pathlib import Path

from .client import S3Client
from .engine import upload
from .state import UploadResult, UploadState, UploadConfig


def upload_from_env(
    data_dir: str | Path,
    config: UploadConfig | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    endpoint_url: str | None = None,
) -> UploadResult:
    """Upload files using environment variables for configuration."""
    bucket = _resolve_bucket(bucket)
    prefix = prefix or os.environ.get("S3_PREFIX", "data")
    endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL")
    client = S3Client.from_env(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)
    state = UploadState(Path(data_dir) / ".upload_state.json")
    return upload(data_dir=data_dir, client=client, state=state, config=config)


def _resolve_bucket(bucket: str | None) -> str:
    """Resolve bucket name from argument or environment."""
    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET must be set via environment or as argument")
    return bucket
