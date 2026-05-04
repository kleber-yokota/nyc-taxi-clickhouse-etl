"""Constants and shared types for the push module."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

PARQUET_EXTENSION = ".parquet"


@dataclass(frozen=True)
class PushResult:
    """Result of a push operation."""

    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0


@dataclass(frozen=True)
class UploadConfig:
    """Configuration for the upload() function.

    Args:
        include: Set of glob patterns to include (e.g. {"*.parquet"}).
        exclude: Set of glob patterns to exclude (e.g. {".push_state.json"}).
        overwrite: Whether to re-upload files already recorded in state.
    """

    include: set[str] | None = None
    exclude: set[str] | None = None
    overwrite: bool = False


@dataclass
class PushState:
    """Tracks which files have been pushed to S3.

    Stores mapping of local file paths to their S3 keys and checksums.
    Persists state to a JSON file on disk.
    """

    state_path: Path
    _data: dict = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Load existing state from disk if file exists."""
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._data = {}

    def save(self) -> None:
        """Persist state to disk.

        Returns:
            None
        """
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def is_pushed(self, local_path: str, checksum: str) -> bool:
        """Check if a file has already been pushed with the same checksum.

        Args:
            local_path: Local file path.
            checksum: SHA-256 checksum of the file.

        Returns:
            True if the file is already pushed with matching checksum.
        """
        entry = self._data.get(local_path)
        if entry is None:
            return False
        return entry.get("checksum") == checksum

    def record_push(self, local_path: str, s3_key: str, checksum: str) -> None:
        """Record that a file has been pushed.

        Args:
            local_path: Local file path.
            s3_key: S3 key where the file was pushed.
            checksum: SHA-256 checksum of the file.

        Returns:
            None
        """
        self._data[local_path] = {
            "s3_key": s3_key,
            "checksum": checksum,
        }
