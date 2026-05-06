"""Tests for the push manifest module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.push_manifest import (
    PUSH_MANIFEST_FILE,
    is_pushed_in_manifest,
    load_push_manifest,
)


class TestLoadPushManifest:
    """Tests for load_push_manifest()."""

    def test_returns_empty_dict_when_file_missing(self, tmp_path: Path) -> None:
        """Should return empty dict when manifest file does not exist."""
        result = load_push_manifest(tmp_path)
        assert result == {}

    def test_returns_empty_dict_on_invalid_json(self, tmp_path: Path) -> None:
        """Should return empty dict when manifest contains invalid JSON."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text("not valid json {{{")
        result = load_push_manifest(tmp_path)
        assert result == {}

    def test_returns_empty_dict_on_io_error(self, tmp_path: Path) -> None:
        """Should return empty dict when manifest file cannot be read."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text('{"key": "value"}')
        manifest_path.chmod(0o000)
        try:
            result = load_push_manifest(tmp_path)
            assert result == {}
        finally:
            manifest_path.chmod(0o644)

    def test_loads_valid_manifest(self, tmp_path: Path) -> None:
        """Should load a valid manifest file."""
        manifest_data = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            },
        }
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text(json.dumps(manifest_data))

        result = load_push_manifest(tmp_path)
        assert result == manifest_data

    def test_loads_empty_manifest(self, tmp_path: Path) -> None:
        """Should handle empty manifest (empty dict)."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text("{}")

        result = load_push_manifest(tmp_path)
        assert result == {}


class TestIsPushedInManifest:
    """Tests for is_pushed_in_manifest()."""

    def test_returns_true_when_entry_exists(self) -> None:
        """Should return True when the file is in the manifest."""
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            },
        }
        assert is_pushed_in_manifest(manifest, "yellow", 2024, 1) is True

    def test_returns_false_when_entry_missing(self) -> None:
        """Should return False when the file is not in the manifest."""
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            },
        }
        assert is_pushed_in_manifest(manifest, "yellow", 2024, 2) is False

    def test_returns_false_for_different_type(self) -> None:
        """Should return False for a different data type."""
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            },
        }
        assert is_pushed_in_manifest(manifest, "green", 2024, 1) is False

    def test_returns_false_for_empty_manifest(self) -> None:
        """Should return False for an empty manifest."""
        assert is_pushed_in_manifest({}, "yellow", 2024, 1) is False

    def test_handles_single_digit_month(self) -> None:
        """Should correctly format single-digit months."""
        manifest = {
            "green/green_tripdata_2024-03.parquet": {
                "s3_key": "data/green/green_tripdata_2024-03.parquet",
                "checksum": "def456",
            },
        }
        assert is_pushed_in_manifest(manifest, "green", 2024, 3) is True

    def test_handles_double_digit_month(self) -> None:
        """Should correctly format double-digit months."""
        manifest = {
            "fhv/fhv_tripdata_2024-12.parquet": {
                "s3_key": "data/fhv/fhv_tripdata_2024-12.parquet",
                "checksum": "ghi789",
            },
        }
        assert is_pushed_in_manifest(manifest, "fhv", 2024, 12) is True
