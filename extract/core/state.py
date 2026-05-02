"""Constants, shared utilities, and error types for the extract module."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple

STATE_FILE = "data/.download_state.json"
ERRORS_DIR = "data/errors"
ERRORS_LOG = "data/errors/download_errors.log"

DOWNLOAD_TIMEOUT = 300
CDN_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_TYPES = ["fhv", "fhvhv", "green", "yellow"]
AVAILABLE_YEARS = range(2009, datetime.now().year + 1)
AVAILABLE_MONTHS = range(1, 13)

# fhvhv availability: not available before 2016, and missing in some 2026 months
# Based on TLC page: 2026-02 and 2026-03 have no fhvhv
FHVHV_MISSING_MONTHS: dict[int, list[int]] = {
    2009: list(range(1, 13)),
    2010: list(range(1, 13)),
    2011: list(range(1, 13)),
    2012: list(range(1, 13)),
    2013: list(range(1, 13)),
    2014: list(range(1, 13)),
    2015: list(range(1, 13)),
    2026: [2, 3],
}


def build_url(data_type: str, year: int, month: int) -> str:
    """Build TLC CDN URL for a parquet file.

    Args:
        data_type: One of 'yellow', 'green', 'fhv', 'fhvhv'.
        year: Year of the data (e.g. 2024).
        month: Month of the data (1-12).

    Returns:
        The full CDN URL string for the parquet file.
    """
    return f"{CDN_BASE}/{data_type}_tripdata_{year}-{month:02d}.parquet"


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file.

    Args:
        file_path: Path to the file to checksum.

    Returns:
        Hex digest string of the SHA-256 hash.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


class ErrorType(Enum):
    """Types of errors that can occur during download."""

    MISSING_FILE = "missing_file"
    NETWORK_ERROR = "network_error"
    HTTP_ERROR = "http_error"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    CORRUPT_FILE = "corrupt_file"
    UNKNOWN = "unknown"


@dataclass
class DownloadSummary:
    """Summary of a download run."""

    downloaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0


@dataclass(frozen=True)
class CatalogEntry:
    """Represents a single TLC trip data parquet file."""

    data_type: str
    year: int
    month: int

    @property
    def url(self) -> str:
        """Full CDN URL for this parquet file.

        Returns:
            The complete URL string (e.g. https://.../yellow_tripdata_2024-01.parquet).
        """
        return build_url(self.data_type, self.year, self.month)

    @property
    def filename(self) -> str:
        """Standard filename for this parquet file.

        Returns:
            Filename string (e.g. yellow_tripdata_2024-01.parquet).
        """
        return f"{self.data_type}_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def target_dir(self) -> str:
        """Target subdirectory for this file type.

        Returns:
            The data_type string (e.g. "yellow").
        """
        return self.data_type
