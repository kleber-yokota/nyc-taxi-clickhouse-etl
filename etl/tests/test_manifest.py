"""Tests for etl/manifest.py — CRUD operations on push manifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from extract.core.push_manifest import PushManifestError
from push.core.state import PushedEntry

from etl.manifest import load, save, update_from_entries


class TestLoad:
    """Tests for manifest.load()."""

    def test_load_nonexistent(self, tmp_data_dir: Path) -> None:
        """Manifest file does not exist -> returns empty dict."""
        result = load(tmp_data_dir)
        assert result == {}

    def test_load_valid_json(self, tmp_data_dir: Path) -> None:
        """Valid JSON manifest -> returns dict."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_data = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            }
        }
        manifest_path.write_text(json.dumps(manifest_data))

        result = load(tmp_data_dir)
        assert result == manifest_data

    def test_load_invalid_json(self, tmp_data_dir: Path) -> None:
        """Corrupted JSON -> raises PushManifestError."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("{invalid json}")

        with pytest.raises(PushManifestError, match="^Push manifest contains invalid JSON"):
            load(tmp_data_dir)

    def test_load_not_dict(self, tmp_data_dir: Path) -> None:
        """JSON array (not dict) -> raises PushManifestError."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text(json.dumps([1, 2, 3]))

        with pytest.raises(PushManifestError, match="^Push manifest must be a dict"):
            load(tmp_data_dir)


class TestSave:
    """Tests for manifest.save()."""

    def test_save_and_reload(self, tmp_data_dir: Path) -> None:
        """Save manifest and reload it."""
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            }
        }
        save(tmp_data_dir, manifest)

        reloaded = load(tmp_data_dir)
        assert reloaded == manifest

    def test_save_creates_directory(self, tmp_path: Path) -> None:
        """Save creates parent directories if they don't exist."""
        nested_data_dir = tmp_path / "nested" / "data"
        manifest = {"file.parquet": {"s3_key": "data/file.parquet", "checksum": "xyz"}}
        save(nested_data_dir, manifest)

        manifest_path = nested_data_dir / ".push_manifest.json"
        assert manifest_path.exists()
        assert load(nested_data_dir) == manifest


class TestUpdateFromEntries:
    """Tests for manifest.update_from_entries()."""

    def test_update_from_entries(self, fake_pushed_entries: list[PushedEntry]) -> None:
        """PushedEntry records are added to manifest."""
        manifest: dict = {}
        update_from_entries(manifest, fake_pushed_entries)

        assert len(manifest) == 2
        assert manifest["yellow/yellow_tripdata_2024-01.parquet"] == {
            "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
            "checksum": "abc123",
        }
        assert manifest["green/green_tripdata_2024-01.parquet"] == {
            "s3_key": "data/green/green_tripdata_2024-01.parquet",
            "checksum": "def456",
        }

    def test_update_from_entries_overwrites_existing(self) -> None:
        """Updating an existing entry overwrites the old value."""
        manifest = {
            "yellow/file.parquet": {
                "s3_key": "data/old/path.parquet",
                "checksum": "old",
            }
        }
        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/new/path.parquet",
                checksum="new",
            )
        ]
        update_from_entries(manifest, entries)

        assert manifest["yellow/file.parquet"] == {
            "s3_key": "data/new/path.parquet",
            "checksum": "new",
        }

    def test_update_from_entries_empty_list(self) -> None:
        """Empty entries list leaves manifest unchanged."""
        manifest = {"existing": {"s3_key": "key", "checksum": "chk"}}
        update_from_entries(manifest, [])
        assert manifest == {"existing": {"s3_key": "key", "checksum": "chk"}}
