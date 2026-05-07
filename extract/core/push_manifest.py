"""Read-only access to the push manifest for the extract module.

The push manifest is a JSON file at data/.push_manifest.json that maps
local file paths (relative to data/) to S3 keys and checksums.
This module only reads the manifest — it never writes to it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

PUSH_MANIFEST_FILE = ".push_manifest.json"


class PushManifestError(Exception):
    """Raised when the push manifest file exists but has invalid format."""


def load_push_manifest(data_dir: Path) -> dict:
    """Load the push manifest from disk.

    Args:
        data_dir: Path to the data directory.

    Returns:
        Dict mapping relative file paths to {s3_key, checksum}.
        Returns empty dict if manifest does not exist.

    Raises:
        PushManifestError: If the manifest file exists but has invalid format.
    """
    manifest_path = data_dir / PUSH_MANIFEST_FILE
    if not manifest_path.exists():
        logger.debug("Push manifest not found: %s", manifest_path)
        return {}

    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
    except json.JSONDecodeError as e:
        raise PushManifestError(
            f"Push manifest contains invalid JSON: {e}"
        ) from e
    except OSError as e:
        raise PushManifestError(
            f"Push manifest cannot be read: {e}"
        ) from e

    if not isinstance(manifest, dict):
        raise PushManifestError(
            f"Push manifest must be a dict, got {type(manifest).__name__}"
        )

    logger.debug("Loaded push manifest with %d entries", len(manifest))
    return manifest


def is_pushed_in_manifest(
    manifest: dict | None,
    data_type: str,
    year: int,
    month: int,
) -> bool:
    """Check if a catalog entry is already in the push manifest.

    Args:
        manifest: The push manifest dict, or None.
        data_type: Data type (e.g. 'yellow').
        year: Year of the data.
        month: Month of the data.

    Returns:
        True if the entry is in the manifest.
        Returns False if manifest is None or empty.
    """
    if not manifest:
        return False
    filename = f"{data_type}_tripdata_{year}-{month:02d}.parquet"
    rel_path = f"{data_type}/{filename}"
    return rel_path in manifest
