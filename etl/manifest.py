"""Push manifest — single source of truth for pipeline state."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

PUSH_MANIFEST_FILE = ".push_manifest.json"


def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


class Manifest:
    """Manages the push manifest: create, load, update with statuses, save.

    Single source of truth for pipeline state.
    Each entry tracks: status, s3_key, checksum, timestamps.

    Consumers only call:
    - init() — create or recover manifest, return current dict
    - apply_mode(mode) — reset if full mode
    - record_download(rel_path, checksum) — mark file as downloaded
    - record_upload(rel_path) — mark file as uploaded
    - record_download_failure(rel_path, error) — mark download failure
    - get_uploaded() — set of uploaded rel_paths
    - get_not_uploaded() — list of downloaded but not uploaded rel_paths
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    def init(self, uploaded_entries: dict | None = None) -> dict:
        """Create or recover manifest. Return current dict."""
        data = self._load()
        if not data:
            data = self._recover(uploaded_entries)
            self._save(data)
        return data

    def apply_mode(self, mode: str) -> None:
        """Reset manifest if full mode."""
        if mode == "full":
            self._save({})

    def record_download(self, rel_path: str, checksum: str) -> None:
        """Mark file as downloaded."""
        manifest = self._load()
        manifest[rel_path] = {
            "status": "downloaded",
            "s3_key": f"data/{rel_path}",
            "checksum": checksum,
            "downloaded_at": _now_iso(),
        }
        self._save(manifest)

    def record_upload(self, rel_path: str) -> None:
        """Mark file as uploaded."""
        manifest = self._load()
        entry = manifest.setdefault(rel_path, {
            "status": "pending",
            "s3_key": f"data/{rel_path}",
        })
        entry.update({
            "status": "uploaded",
            "checksum": entry.get("checksum"),
            "uploaded_at": _now_iso(),
        })
        self._save(manifest)

    def record_download_failure(self, rel_path: str, error: str) -> None:
        """Mark download failure."""
        manifest = self._load()
        entry = manifest.setdefault(rel_path, {"status": "pending"})
        entry.update({"status": "download_failed", "error": error})
        self._save(manifest)

    def get_uploaded(self) -> set[str]:
        """Return rel_paths already in garage."""
        return {k for k, v in self._load().items() if v["status"] == "uploaded"}

    def get_not_uploaded(self) -> list[str]:
        """Return rel_paths downloaded but not sent."""
        return [k for k, v in self._load().items() if v["status"] == "downloaded"]

    def _recover(self, uploaded_entries: dict | None = None) -> dict:
        """Recover manifest from upload state + local disk.

        Manifest is the ONLY module that knows how to convert
        upload state format into manifest format.
        """
        data: dict = {}
        data.update(self._convert_upload_entries(uploaded_entries))
        self._recover_parquets(data)
        return data

    def _convert_upload_entries(self, uploaded_entries: dict | None) -> dict:
        """Convert upload entries to manifest format."""
        if not uploaded_entries:
            return {}
        result: dict = {}
        for local_path, info in uploaded_entries.items():
            rel_path = self._to_rel_path(local_path)
            result[rel_path] = {
                "status": "uploaded",
                "s3_key": info.get("s3_key", f"data/{rel_path}"),
                "checksum": info.get("checksum"),
            }
        return result

    def _to_rel_path(self, local_path: str) -> str:
        """Convert absolute or relative path to relative path."""
        p = Path(local_path)
        try:
            return str(p.relative_to(self.data_dir))
        except ValueError:
            return str(p)

    def _recover_parquets(self, data: dict) -> None:
        """Add .parquet files on disk not yet in manifest (mutates data)."""
        for path in self.data_dir.rglob("*.parquet"):
            rel_path = str(path.relative_to(self.data_dir))
            if rel_path not in data:
                data[rel_path] = {
                    "status": "downloaded",
                    "s3_key": f"data/{rel_path}",
                }

    def _load(self) -> dict:
        """Load the push manifest from disk."""
        manifest_path = self.data_dir / PUSH_MANIFEST_FILE
        if not manifest_path.exists():
            logger.debug("Push manifest not found: %s", manifest_path)
            return {}
        data = self._read_manifest_file(manifest_path)
        if not isinstance(data, dict):
            logger.warning("Push manifest is not a dict: %s", type(data).__name__)
            return {}
        return data

    def _read_manifest_file(self, path: Path) -> dict:
        """Read and parse manifest JSON file."""
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load push manifest: %s", e)
            return {}

    def _save(self, manifest: dict) -> None:
        """Persist push manifest to disk."""
        manifest_path = self.data_dir / PUSH_MANIFEST_FILE
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
