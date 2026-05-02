"""Unit tests for downloader state management and error logging."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.state_manager import State
from extract.core.state import ErrorType


class TestState:
    def test_loads_empty_state(self, state_file: Path):
        state = State(state_file)
        assert state.checksums == {}

    def test_loads_existing_state(self, existing_state: Path):
        state = State(existing_state)
        assert state.checksums["https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"] == "abc123"

    def test_save_adds_checksum(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "sha256hash")
        assert state.checksums["https://example.com/file.parquet"] == "sha256hash"

    def test_get_checksum_returns_none_when_missing(self, state_file: Path):
        state = State(state_file)
        assert state.get_checksum("https://example.com/missing.parquet") is None

    def test_get_checksum_returns_stored_value(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "abc123")
        assert state.get_checksum("https://example.com/file.parquet") == "abc123"

    def test_is_downloaded_returns_true(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "abc123")
        assert state.is_downloaded("https://example.com/file.parquet") is True

    def test_is_downloaded_returns_false_when_missing(self, state_file: Path):
        state = State(state_file)
        assert state.is_downloaded("https://example.com/missing.parquet") is False

    def test_reset_clears_all(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/a.parquet", "hash1")
        state.save("https://example.com/b.parquet", "hash2")
        state.reset()
        assert state.checksums == {}

    def test_reset_persists_empty_state(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/a.parquet", "hash1")
        state.reset()
        state2 = State(state_file)
        assert state2.checksums == {}

    def test_persists_to_file(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "abc123")
        data = json.loads(state_file.read_text())
        assert data["checksums"]["https://example.com/file.parquet"] == "abc123"

    def test_loads_corrupt_state_returns_empty(self, tmp_path: Path):
        corrupt = tmp_path / "corrupt.json"
        corrupt.write_text("{ invalid json }")
        state = State(corrupt)
        assert state.checksums == {}


class TestErrorLogging:
    def test_log_error_writes_json_line(self, errors_dir: Path, errors_log: Path):
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/file.parquet", ErrorType.MISSING_FILE, "HTTP 404")
        lines = errors_log.read_text().strip().split("\n")
        entry = json.loads(lines[-1])
        assert entry["type"] == "missing_file"
        assert entry["url"] == "https://example.com/file.parquet"
        assert entry["detail"] == "HTTP 404"
        assert "timestamp" in entry

    def test_log_error_creates_errors_dir(self, tmp_path: Path):
        errors_dir = tmp_path / "new_dir"
        log = errors_dir / "download_errors.log"
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/file.parquet", ErrorType.NETWORK_ERROR, "test")
        assert log.exists()

    def test_log_multiple_errors(self, errors_dir: Path, errors_log: Path):
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/a.parquet", ErrorType.MISSING_FILE, "404")
        state.log_error("https://example.com/b.parquet", ErrorType.NETWORK_ERROR, "timeout")
        lines = errors_log.read_text().strip().split("\n")
        assert len(lines) == 2

    @pytest.mark.parametrize(
        "error_type",
        [
            ErrorType.MISSING_FILE,
            ErrorType.NETWORK_ERROR,
            ErrorType.HTTP_ERROR,
            ErrorType.CHECKSUM_MISMATCH,
            ErrorType.CORRUPT_FILE,
            ErrorType.UNKNOWN,
        ],
    )
    def test_all_error_types_logged(self, errors_dir: Path, errors_log: Path, error_type: ErrorType):
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/f.parquet", error_type, "detail")
        lines = errors_log.read_text().strip().split("\n")
        entry = json.loads(lines[-1])
        assert entry["type"] == error_type.value
