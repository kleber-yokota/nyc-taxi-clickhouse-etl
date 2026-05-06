"""List S3 objects — neutral data from push.

Push provides a raw list of S3 objects. The orchestrator adapts this
data into the formats needed by extract (dict with rel_path keys)
and by the manifest (dict with rel_path -> {s3_key, checksum}).

Push does NOT know about manifest structure.
"""

from __future__ import annotations

from dataclasses import dataclass

from .client import S3Client


@dataclass(frozen=True)
class S3Object:
    """Raw S3 object — neutral format, no manifest awareness.

    Args:
        key: Full S3 key (e.g. 'data/yellow/file.parquet').
    """
    key: str


def list_s3_objects(
    bucket: str,
    prefix: str = "data",
) -> list[S3Object]:
    """List all S3 objects under the given prefix.

    Returns a neutral list of S3Object records. The orchestrator
    adapts this into manifest format for extract and for storage.

    Args:
        bucket: S3 bucket name.
        prefix: S3 key prefix (default: "data").

    Returns:
        List of S3Object with full S3 keys.

    Raises:
        ValueError: If bucket is empty.
    """
    if not bucket:
        raise ValueError("S3_BUCKET must be set to list S3 objects")

    client = S3Client.from_env(
        bucket=bucket,
        prefix=prefix,
    )

    s3_keys = client.list_objects(prefix=prefix)
    return [S3Object(key=key) for key in s3_keys]
