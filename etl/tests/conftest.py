"""Shared fixtures for etl tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from push.core.state import PushedEntry


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory for ETL tests."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def fake_pushed_entries() -> list[PushedEntry]:
    """Return sample PushedEntry records for testing."""
    return [
        PushedEntry(
            rel_path="yellow/yellow_tripdata_2024-01.parquet",
            s3_key="data/yellow/yellow_tripdata_2024-01.parquet",
            checksum="abc123",
        ),
        PushedEntry(
            rel_path="green/green_tripdata_2024-01.parquet",
            s3_key="data/green/green_tripdata_2024-01.parquet",
            checksum="def456",
        ),
    ]
