"""Manifest management — creates and updates .push_manifest.json.

The push manifest is a JSON file at data/.push_manifest.json that maps
local file paths (relative to data/) to S3 keys and checksums.
This module is responsible for creating, reading, and updating the manifest.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from extract.core.push_manifest import PushManifestError

logger = logging.getLogger(__name__)

PUSH_MANIFEST_FILE = ".push_manifest.json"


def load(data_dir: Path) -> dict:
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


def save(data_dir: Path, manifest: dict) -> None:
    """Persist the manifest to disk.

    Args:
        data_dir: Path to the data directory.
        manifest: The manifest dict to persist.
    """
    manifest_path = data_dir / PUSH_MANIFEST_FILE
    try:
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
    except OSError as e:
        raise PushManifestError(
            f"Push manifest cannot be written: {e}"
        ) from e

    logger.info("Saved push manifest with %d entries", len(manifest))


def add_entry(
    manifest: dict,
    rel_path: str,
    s3_key: str,
    checksum: str,
) -> None:
    """Add an entry to the manifest.

    Args:
        manifest: The manifest dict to update.
        rel_path: Relative path from data_dir (e.g. 'yellow/...parquet').
        s3_key: Full S3 key (e.g. 'data/yellow/...parquet').
        checksum: SHA-256 hex digest of the file.
    """
    manifest[rel_path] = {
        "s3_key": s3_key,
        "checksum": checksum,
    }
