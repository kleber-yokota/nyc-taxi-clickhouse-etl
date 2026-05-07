"""Manifest updater — manages push manifest entries."""

from __future__ import annotations

from pathlib import Path

from .checksum import Checksum
from .manifest import add_entry, load_manifest, save_manifest


class ManifestUpdater:
    """Updates the push manifest with uploaded file checksums.

    Separates manifest management from the orchestrator.
    """

    def __init__(self, data_dir: Path, checksum: Checksum) -> None:
        self.data_dir = data_dir
        self.checksum = checksum

    def update(self, uploaded_files: list[str], manifest: dict | None = None) -> dict:
        """Update manifest with checksums for uploaded files.

        Args:
            uploaded_files: List of relative file paths that were uploaded.
            manifest: Existing manifest dict (loads from disk if None).

        Returns:
            Updated manifest dict.
        """
        if manifest is None:
            manifest = load_manifest(self.data_dir)
        for rel_path in uploaded_files:
            self._add_entry(manifest, rel_path)
        save_manifest(self.data_dir, manifest)
        return manifest

    def _add_entry(self, manifest: dict, rel_path: str) -> None:
        """Add a single entry to the manifest."""
        file_path = self.data_dir / rel_path
        checksum = self.checksum.compute(file_path)
        s3_key = f"data/{rel_path}"
        add_entry(manifest, rel_path, s3_key, checksum)
