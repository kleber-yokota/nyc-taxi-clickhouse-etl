"""Constants and shared types for the upload module."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

PARQUET_EXTENSION = ".parquet"


@dataclass(frozen=True)
class UploadEntry:
    """Metadata for a single uploaded file."""
    rel_path: str
    s3_key: str
    checksum: str


@dataclass(frozen=True)
class UploadResult:
    """Result of an upload operation."""
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0
    entries: list[UploadEntry] = field(default_factory=list)


@dataclass(frozen=True)
class UploadConfig:
    """Configuration for the upload() function."""
    include: set[str] | None = None
    exclude: set[str] | None = None
    overwrite: bool = False
    delete_after_upload: bool = False


@dataclass
class UploadState:
    """Tracks which files have been uploaded to S3. Persists to JSON."""
    state_path: Path
    _data: dict = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._data = {}

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def is_uploaded(self, local_path: str, checksum: str) -> bool:
        entry = self._data.get(local_path)
        if entry is None:
            return False
        return entry.get("checksum") == checksum

    def record_upload(self, local_path: str, s3_key: str, checksum: str) -> None:
        self._data[local_path] = {"s3_key": s3_key, "checksum": checksum}

    def get_entries(self) -> dict:
        """Return raw upload state entries {local_path: {s3_key, checksum}}."""
        return dict(self._data)
