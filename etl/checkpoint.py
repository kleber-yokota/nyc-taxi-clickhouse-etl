"""Checkpoint persistence — saves/restores pipeline state to disk.

Checkpoint file: data/.etl_checkpoint.json
"""

from __future__ import annotations

import json
import os
import struct
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

CHECKPOINT_FILE = ".etl_checkpoint.json"

# ULID base32 alphabet (excludes I, L, O, U to avoid confusion)
_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _generate_ulid() -> str:
    """Generate a ULID (Universally Unique Lexicographically Sortable Identifier).

    ULIDs are time-sortable and lexicographically sortable, making them
    ideal for ordering pipeline runs by recency.

    Format: 26 base32 characters:
      - First 10 chars: 43-bit millisecond timestamp
      - Remaining 16 chars: 80 bits of randomness
    """
    timestamp_ms = int(time.time() * 1000)
    rand_bytes = os.urandom(10)

    parts = []

    # Encode 6-byte timestamp as 10 base32 chars
    ts_bytes = struct.pack(">Q", timestamp_ms)[-6:]
    ts_int = int.from_bytes(ts_bytes, "big")
    for _ in range(10):
        parts.append(_B32[ts_int & 31])
        ts_int >>= 5
    parts.reverse()

    # Encode 10 random bytes as 16 base32 chars
    rand_int = int.from_bytes(rand_bytes, "big")
    for _ in range(16):
        parts.append(_B32[rand_int & 31])
        rand_int >>= 5

    return "".join(parts)


@dataclass(frozen=True)
class Checkpoint:
    """Snapshot of pipeline execution state.

    Args:
        pipeline_id: ULID — time-sortable unique identifier.
        started_at: Float timestamp (time.time()) of pipeline start.
        finished_at: Float timestamp or None.
        status: Pipeline stage value.
        error: Error message if pipeline failed (or None).
        extract: Extract stage metrics dict.
        upload: Upload stage metrics dict.
        total_duration_seconds: Total pipeline execution time.
    """
    pipeline_id: str = field(default_factory=_generate_ulid)
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    status: str = "not_running"
    error: str | None = None
    extract: dict = field(default_factory=dict)
    upload: dict = field(default_factory=dict)
    total_duration_seconds: float = 0.0

    def to_dict(self) -> dict:
        """Serialize checkpoint to dict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> Checkpoint:
        """Deserialize checkpoint from dict."""
        return cls(**data)


def load_checkpoint(data_dir: Path) -> Checkpoint | None:
    """Load checkpoint from disk.

    Args:
        data_dir: Path to the data directory.

    Returns:
        Checkpoint object, or None if file doesn't exist or is invalid.
    """
    checkpoint_path = data_dir / CHECKPOINT_FILE
    if not checkpoint_path.exists():
        return None
    try:
        with open(checkpoint_path, "r") as f:
            data = json.load(f)
        return Checkpoint.from_dict(data)
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def save_checkpoint(data_dir: Path, checkpoint: Checkpoint) -> None:
    """Persist checkpoint to disk.

    Args:
        data_dir: Path to the data directory.
        checkpoint: Checkpoint to persist.
    """
    checkpoint_path = data_dir / CHECKPOINT_FILE
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_path, "w") as f:
        json.dump(checkpoint.to_dict(), f, indent=2)
