"""File checksum computation for the upload module."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .state import PARQUET_EXTENSION


def compute_content_type(file_path: Path) -> str:
    """Determine the content type for a file based on its extension.

    Args:
        file_path: Path to the file.

    Returns:
        MIME type string for the file.
    """
    suffix = file_path.suffix.lower()
    if suffix == PARQUET_EXTENSION:
        return "application/x-parquet"
    return "application/octet-stream"


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file.

    Args:
        file_path: Path to the file.

    Returns:
        Hex digest string of the SHA-256 hash.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
