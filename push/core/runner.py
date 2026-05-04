"""Entry point — upload files using environment variables for configuration."""

from __future__ import annotations

import os
from pathlib import Path

from .client import S3Client
from .state import PushResult, PushState, UploadConfig
from .push import upload



def upload_from_env(
    data_dir: str | Path,
    config: UploadConfig | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    endpoint_url: str | None = None,
) -> PushResult:
    """Upload files using environment variables for configuration.

    Reads S3_ENDPOINT_URL, S3_BUCKET, S3_PREFIX, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY from environment.

    Args:
        data_dir: Path to the local data directory.
        config: Upload configuration (include/exclude/overwrite).
        bucket: S3 bucket name. Overrides S3_BUCKET env var.
        prefix: S3 key prefix. Overrides S3_PREFIX env var.
        endpoint_url: S3 endpoint URL. Overrides S3_ENDPOINT_URL env var.

    Returns:
        PushResult with uploaded/skipped/failed/total counts.

    Raises:
        ValueError: If S3_BUCKET is not set in environment or as argument.
        RuntimeError: If AWS credentials are not available.
    """
    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET must be set via environment or as argument")

    prefix = prefix or os.environ.get("S3_PREFIX", "data")
    endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL")

    client = S3Client.from_env(
        bucket=bucket,
        prefix=prefix,
        endpoint_url=endpoint_url,
    )

    state = PushState(Path(data_dir) / ".push_state.json")

    return upload(
        data_dir=data_dir,
        client=client,
        state=state,
        config=config,
    )
