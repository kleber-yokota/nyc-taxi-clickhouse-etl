"""Tests for state_manager module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.state import ErrorType
from extract.core.state_manager import State


class TestStatePersist:
    """Tests for State._persist method."""

    def test_persist_creates_state_file(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.save("https://example.com/file.parquet", "abc123")

        state_file = tmp_path / "state.json"
        assert state_file.exists()
        data = json.loads(state_file.read_text())
        assert "checksums" in data
        assert data["checksums"]["https://example.com/file.parquet"] == "abc123"

    def test_persist_creates_nested_parent_dir(self, tmp_path: Path):
        """Test that _persist creates nested parent directories (kills parents=False mutant)."""
        nested_state = tmp_path / "deep" / "nested" / "path" / "state.json"
        state = State(nested_state)
        state.save("https://example.com/file.parquet", "hash123")

        assert nested_state.exists()
        data = json.loads(nested_state.read_text())
        assert data["checksums"]["https://example.com/file.parquet"] == "hash123"

    def test_persist_overwrites_existing(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.save("https://example.com/file1.parquet", "hash1")
        state.save("https://example.com/file2.parquet", "hash2")

        state_file = tmp_path / "state.json"
        data = json.loads(state_file.read_text())
        assert len(data["checksums"]) == 2

    def test_persist_empty_checksums(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.reset()

        state_file = tmp_path / "state.json"
        data = json.loads(state_file.read_text())
        assert data["checksums"] == {}

    def test_persist_preserves_multiple_urls(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        urls = [
            "https://example.com/file1.parquet",
            "https://example.com/file2.parquet",
            "https://example.com/file3.parquet",
        ]
        for url in urls:
            state.save(url, f"hash_{url}")

        data = json.loads((tmp_path / "state.json").read_text())
        assert len(data["checksums"]) == 3
        for url in urls:
            assert url in data["checksums"]


class TestStateLoad:
    """Tests for State._load method."""

    def test_load_existing_state(self, tmp_path: Path, existing_state: Path):
        state = State(tmp_path / "state.json")
        state.checksums = {}
        # Re-load from existing_state
        state.state_path = existing_state
        state._load()
        assert "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" in state.checksums

    def test_load_corrupted_json(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        state_file.write_text("not valid json {{{")
        state = State(tmp_path / "state.json")
        assert state.checksums == {}

    def test_load_missing_file(self, tmp_path: Path):
        state = State(tmp_path / "nonexistent.json")
        assert state.checksums == {}

    def test_load_empty_file(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        state_file.write_text("")
        state = State(tmp_path / "state.json")
        assert state.checksums == {}

    def test_load_missing_checksums_key(self, tmp_path: Path):
        state_file = tmp_path / "state.json"
        state_file.write_text('{"other_key": "value"}')
        state = State(tmp_path / "state.json")
        assert state.checksums == {}


class TestStatePersistErrors:
    """Tests for State._persist_errors method."""

    def test_persist_errors_creates_log(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.log_error("https://example.com/file.parquet", ErrorType.NETWORK_ERROR, "ConnectionError")

        error_log = tmp_path / "errors" / "download_errors.log"
        assert error_log.exists()
        lines = error_log.read_text().strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["url"] == "https://example.com/file.parquet"
        assert entry["type"] == "network_error"

    def test_persist_errors_creates_nested_errors_dir(self, tmp_path: Path):
        """Test that _persist_errors creates nested error directories (kills parents=False mutant)."""
        nested_state = tmp_path / "deep" / "nested" / "path" / "state.json"
        state = State(nested_state)
        state.log_error("https://example.com/file.parquet", ErrorType.HTTP_ERROR, "HTTP 500")

        error_log = tmp_path / "deep" / "nested" / "path" / "errors" / "download_errors.log"
        assert error_log.exists()
        lines = error_log.read_text().strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["type"] == "http_error"

    def test_persist_errors_multiple(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.log_error("https://example.com/file1.parquet", ErrorType.MISSING_FILE, "HTTP 404")
        state.log_error("https://example.com/file2.parquet", ErrorType.HTTP_ERROR, "HTTP 500")

        error_log = tmp_path / "errors" / "download_errors.log"
        lines = error_log.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_persist_errors_clears_after_write(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        state.log_error("https://example.com/file.parquet", ErrorType.NETWORK_ERROR, "test")
        assert len(state._errors) == 0


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
