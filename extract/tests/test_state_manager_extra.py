"""Tests for State._load, log_error, reset, and get_checksum methods."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.state import ErrorType
from extract.core.state_manager import State


class TestStateLogError:
    """Tests for State.log_error method."""

    def test_log_error_all_types(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        for error_type in ErrorType:
            state.log_error("https://example.com/file.parquet", error_type, f"Detail for {error_type.value}")

        error_log = tmp_path / "errors" / "download_errors.log"
        lines = error_log.read_text().strip().split("\n")
        assert len(lines) == len(ErrorType)

    def test_log_error_with_detail(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.log_error("https://example.com/file.parquet", ErrorType.UNKNOWN, "Something went wrong")

        error_log = tmp_path / "errors" / "download_errors.log"
        entry = json.loads(error_log.read_text().strip())
        assert entry["detail"] == "Something went wrong"

    def test_log_error_timestamp_present(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.log_error("https://example.com/file.parquet", ErrorType.NETWORK_ERROR)

        error_log = tmp_path / "errors" / "download_errors.log"
        entry = json.loads(error_log.read_text().strip())
        assert "timestamp" in entry
        assert "T" in entry["timestamp"]


class TestStateReset:
    """Tests for State.reset method."""

    def test_reset_clears_checksums(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.save("https://example.com/file.parquet", "abc123")
        assert state.is_downloaded("https://example.com/file.parquet")

        state.reset()
        assert not state.is_downloaded("https://example.com/file.parquet")

    def test_reset_persists_empty(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.save("https://example.com/file.parquet", "abc123")
        state.reset()

        data = json.loads((tmp_path / "state.json").read_text())
        assert data["checksums"] == {}


class TestStateGetChecksum:
    """Tests for State.get_checksum method."""

    def test_get_existing_checksum(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.save("https://example.com/file.parquet", "abc123")
        assert state.get_checksum("https://example.com/file.parquet") == "abc123"

    def test_get_missing_checksum(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        assert state.get_checksum("https://example.com/missing.parquet") is None
