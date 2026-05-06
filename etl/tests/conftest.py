"""Shared fixtures for ETL tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory for ETL tests.

    Args:
        tmp_path: Pytest temporary path fixture.

    Returns:
        Path to the temporary data directory.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir
