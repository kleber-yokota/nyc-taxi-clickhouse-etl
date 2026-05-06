"""Tests for the push manifest module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.push_manifest import (
    PUSH_MANIFEST_FILE,
    PushManifestError,
    is_pushed_in_manifest,
    load_push_manifest,
)


class TestLoadPushManifest:
    """Tests for load_push_manifest()."""

    def test_returns_empty_dict_when_file_missing(self, tmp_path: Path) -> None:
        """Should return empty dict when manifest file does not exist."""
        result = load_push_manifest(tmp_path)
        assert result == {}

    def test_raises_on_invalid_json(self, tmp_path: Path) -> None:
        """Should raise PushManifestError when manifest contains invalid JSON."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text("not valid json {{{")
        with pytest.raises(PushManifestError, match="^Push manifest contains invalid JSON"):
            load_push_manifest(tmp_path)

    def test_raises_on_io_error(self, tmp_path: Path) -> None:
        """Should raise PushManifestError when manifest file cannot be read."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text('{"key": "value"}')
        manifest_path.chmod(0o000)
        try:
            with pytest.raises(PushManifestError):
                load_push_manifest(tmp_path)
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

    def test_raises_on_non_dict_manifest(self, tmp_path: Path) -> None:
        """Should raise PushManifestError when manifest is not a dict."""
        manifest_path = tmp_path / PUSH_MANIFEST_FILE
        manifest_path.write_text('["not", "a", "dict"]')

        with pytest.raises(PushManifestError, match="^Push manifest must be a dict"):
            load_push_manifest(tmp_path)

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

    def test_returns_false_for_none_manifest(self) -> None:
        """Should return False when manifest is None."""
        assert is_pushed_in_manifest(None, "yellow", 2024, 1) is False

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
