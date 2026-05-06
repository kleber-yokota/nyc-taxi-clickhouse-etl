"""Manifest management — orchestrator is the authority.

The push manifest is a JSON file at data/.push_manifest.json that maps
local file paths (relative to data/) to S3 keys and checksums.
The orchestrator creates, reads, updates, and saves this manifest.
Push provides data (PushedEntry), orchestrator writes it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from extract.core.push_manifest import PushManifestError
from push.core.state import PushedEntry

logger = logging.getLogger(__name__)
PUSH_MANIFEST_FILE = ".push_manifest.json"


def load(data_dir: Path) -> dict[str, Any]:
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

    return manifest


def save(data_dir: Path, manifest: dict[str, Any]) -> None:
    """Persist the manifest to disk.

    Args:
        data_dir: Path to the data directory.
        manifest: The manifest dict to persist.
    """
    manifest_path = data_dir / PUSH_MANIFEST_FILE
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)


def update_from_entries(manifest: dict[str, Any], entries: list[PushedEntry]) -> None:
    """Update manifest with uploaded entries from push.

    Args:
        manifest: The manifest dict to update.
        entries: List of PushedEntry objects (rel_path, s3_key, checksum).
    """
    for entry in entries:
        manifest[entry.rel_path] = {
            "s3_key": entry.s3_key,
            "checksum": entry.checksum,
        }
