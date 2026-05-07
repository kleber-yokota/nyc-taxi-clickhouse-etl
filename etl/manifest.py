"""Manifest persistence — load, save, and add entries to the push manifest.

The push manifest (.push_manifest.json) tracks uploaded files with their
S3 keys and checksums. This module handles file I/O only.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

PUSH_MANIFEST_FILE = ".push_manifest.json"


def load_manifest(data_dir: Path) -> dict:
    """Load the push manifest from disk.

    Args:
        data_dir: Path to the data directory.

    Returns:
        Dict mapping relative file paths to {s3_key, checksum}.
        Returns empty dict if manifest does not exist.
    """
    manifest_path = data_dir / PUSH_MANIFEST_FILE
    if not manifest_path.exists():
        logger.debug("Push manifest not found: %s", manifest_path)
        return {}

    try:
        with open(manifest_path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load push manifest: %s", e)
        return {}
    if not isinstance(data, dict):
        logger.warning("Push manifest is not a dict: %s", type(data).__name__)
        return {}
    return data


def save_manifest(data_dir: Path, manifest: dict) -> None:
    """Persist push manifest to disk.

    Args:
        data_dir: Path to the data directory.
        manifest: Manifest dict to persist.
    """
    manifest_path = data_dir / PUSH_MANIFEST_FILE
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)


def add_entry(manifest: dict, rel_path: str, s3_key: str, checksum: str) -> None:
    """Add an entry to the manifest.

    Args:
        manifest: The manifest dict to modify.
        rel_path: Relative file path (e.g. 'yellow/tripdata_2024-01.parquet').
        s3_key: Full S3 key (e.g. 'data/yellow/tripdata_2024-01.parquet').
        checksum: Hash hex digest of the file.
    """
    manifest[rel_path] = {"s3_key": s3_key, "checksum": checksum}
