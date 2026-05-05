"""Unit tests for downloader state management and error logging."""

from __future__ import annotations

import json
import logging
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

    def test_init_with_custom_errors_dir(self, tmp_path: Path):
        state_path = tmp_path / "data" / "state.json"
        errors = tmp_path / "custom_errors"
        state = State(state_path, errors_dir=errors)
        assert state._errors_dir == errors
        assert state.state_path == state_path

    def test_init_with_custom_state_path_only(self, tmp_path: Path):
        state_path = tmp_path / "custom" / "state.json"
        state = State(state_path)
        assert state.state_path == state_path
        expected_errors = tmp_path / "custom" / "errors"
        assert state._errors_dir == expected_errors

    def test_init_state_path_without_parent(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = State(state_path)
        assert state.state_path == state_path

    def test_persist_creates_parent_dirs(self, tmp_path: Path):
        state_path = tmp_path / "deep" / "nested" / "state.json"
        state = State(state_path)
        state.save("https://example.com/file.parquet", "hash1")
        assert state_path.exists()

    def test_load_nonexistent_file_returns_empty(self, tmp_path: Path):
        state_path = tmp_path / "nonexistent.json"
        state = State(state_path)
        assert state.checksums == {}

    def test_log_error_appends_to_log(self, errors_dir: Path, errors_log: Path):
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/a.parquet", ErrorType.MISSING_FILE, "first")
        state.log_error("https://example.com/b.parquet", ErrorType.NETWORK_ERROR, "second")
        lines = errors_log.read_text().strip().split("\n")
        assert len(lines) == 2
        first = json.loads(lines[0])
        second = json.loads(lines[1])
        assert first["type"] == "missing_file"
        assert second["type"] == "network_error"

    def test_persist_errors_creates_log(self, errors_dir: Path):
        state = State(errors_dir=errors_dir)
        state.log_error("https://example.com/file.parquet", ErrorType.MISSING_FILE, "test")
        log = errors_dir / "download_errors.log"
        assert log.exists()
        lines = log.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_ensure_dirs_creates_both(self, tmp_path: Path):
        state_path = tmp_path / "data" / "state.json"
        errors = tmp_path / "custom_errors"
        state = State(state_path, errors_dir=errors)
        assert state_path.parent.exists()
        assert errors.exists()

    def test_state_with_absolute_paths(self, tmp_path: Path):
        import os
        state_path = str(tmp_path / "abs_state.json")
        errors_dir = str(tmp_path / "abs_errors")
        state = State(state_path, errors_dir=errors_dir)
        state.save("https://example.com/file.parquet", "hash1")
        assert os.path.isabs(state_path)
        assert state.is_downloaded("https://example.com/file.parquet")

    def test_errors_dir_from_state_parent_when_has_parent(self, tmp_path: Path):
        state_path = tmp_path / "data" / "subdir" / "state.json"
        state = State(state_path)
        expected_errors = tmp_path / "data" / "subdir" / "errors"
        assert state._errors_dir == expected_errors



