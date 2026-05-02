"""Shared fixtures for extract tests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for state files."""
    return tmp_path / "data"


@pytest.fixture
def fake_parquet_content() -> bytes:
    """Return fake parquet content for testing."""
    return b"FAKE_PARQUET_CONTENT_" + b"x" * 100


@pytest.fixture
def state_file(state_dir: Path) -> Path:
    """Return path to state file."""
    return state_dir / ".download_state.json"


@pytest.fixture
def existing_state(state_file: Path) -> Path:
    """Create a state file with pre-existing checksums."""
    state_data = {
        "checksums": {
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet": "abc123",
            "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet": "def456",
        }
    }
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state_data))
    return state_file


@pytest.fixture
def errors_dir(state_dir: Path) -> Path:
    """Return path to errors directory."""
    errors = state_dir / "errors"
    errors.mkdir(parents=True, exist_ok=True)
    return errors


@pytest.fixture
def errors_log(state_dir: Path) -> Path:
    """Return path to errors log file."""
    log = state_dir / "errors" / "download_errors.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    return log
