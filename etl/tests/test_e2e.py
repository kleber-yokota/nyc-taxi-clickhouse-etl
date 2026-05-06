"""E2E tests for etl manifest module — real filesystem, no mocks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from etl.manifest import load, save, update_from_entries
from push.core.state import PushedEntry


class TestManifestPersistence:
    """Full lifecycle tests: save → load → update → load."""

    def test_save_and_reload_roundtrip(self, tmp_path: Path) -> None:
        """Save manifest, reload, verify all entries preserved."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            },
            "green/green_tripdata_2024-01.parquet": {
                "s3_key": "data/green/green_tripdata_2024-01.parquet",
                "checksum": "def456",
            },
        }

        save(data_dir, manifest)
        loaded = load(data_dir)

        assert loaded == manifest

    def test_update_persists_to_disk(self, tmp_path: Path) -> None:
        """Update manifest, save, verify new entries on disk."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        manifest: dict[str, Any] = {}
        entries = [
            PushedEntry(
                rel_path="yellow/yellow_tripdata_2024-02.parquet",
                s3_key="data/yellow/yellow_tripdata_2024-02.parquet",
                checksum="new123",
            ),
            PushedEntry(
                rel_path="green/green_tripdata_2024-02.parquet",
                s3_key="data/green/green_tripdata_2024-02.parquet",
                checksum="new456",
            ),
        ]

        update_from_entries(manifest, entries)
        save(data_dir, manifest)

        loaded = load(data_dir)
        assert "yellow/yellow_tripdata_2024-02.parquet" in loaded
        assert "green/green_tripdata_2024-02.parquet" in loaded
        assert loaded["yellow/yellow_tripdata_2024-02.parquet"]["checksum"] == "new123"

    def test_update_overwrites_existing(self, tmp_path: Path) -> None:
        """Updating an existing key replaces the old value."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        manifest = {
            "yellow/file.parquet": {
                "s3_key": "data/old/file.parquet",
                "checksum": "old",
            },
        }
        save(data_dir, manifest)

        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/new/file.parquet",
                checksum="new",
            ),
        ]

        loaded = load(data_dir)
        update_from_entries(loaded, entries)
        save(data_dir, loaded)

        reloaded = load(data_dir)
        assert reloaded["yellow/file.parquet"]["checksum"] == "new"
        assert reloaded["yellow/file.parquet"]["s3_key"] == "data/new/file.parquet"

    def test_full_pipeline_save_update_load(self, tmp_path: Path) -> None:
        """Full pipeline: save initial → load → update → save → load."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Step 1: Save initial manifest
        initial = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "init",
            },
        }
        save(data_dir, initial)

        # Step 2: Load and update
        manifest = load(data_dir)
        new_entries = [
            PushedEntry(
                rel_path="yellow/yellow_tripdata_2024-02.parquet",
                s3_key="data/yellow/yellow_tripdata_2024-02.parquet",
                checksum="updated",
            ),
        ]
        update_from_entries(manifest, new_entries)
        save(data_dir, manifest)

        # Step 3: Reload and verify both entries
        final = load(data_dir)
        assert len(final) == 2
        assert "yellow/yellow_tripdata_2024-01.parquet" in final
        assert "yellow/yellow_tripdata_2024-02.parquet" in final
        assert final["yellow/yellow_tripdata_2024-02.parquet"]["checksum"] == "updated"

    def test_manifest_file_is_valid_json(self, tmp_path: Path) -> None:
        """Saved manifest file is valid, parseable JSON."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        manifest = {"key": {"s3_key": "value", "checksum": "abc"}}
        save(data_dir, manifest)

        manifest_path = data_dir / ".push_manifest.json"
        raw = manifest_path.read_text()
        parsed = json.loads(raw)
        assert parsed == manifest

    def test_save_creates_nested_directories(self, tmp_path: Path) -> None:
        """Save creates parent directories if they don't exist."""
        data_dir = tmp_path / "data" / "nested" / "deep"
        manifest = {"key": {"s3_key": "value"}}
        save(data_dir, manifest)

        assert (data_dir / ".push_manifest.json").exists()
        loaded = load(data_dir)
        assert loaded == manifest


class TestManifestLoadEdgeCases:
    """E2E tests for manifest loading edge cases."""

    def test_load_empty_manifest(self, tmp_path: Path) -> None:
        """Loading an empty manifest dict returns empty dict."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        manifest_path = data_dir / ".push_manifest.json"
        manifest_path.write_text("{}")

        result = load(data_dir)
        assert result == {}

    def test_load_preserves_special_characters(self, tmp_path: Path) -> None:
        """Manifest keys with special characters are preserved."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        manifest = {
            "yellow/yellow_tripdata_2024-01_12-00-00.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01_12-00-00.parquet",
                "checksum": "abc",
            },
        }
        save(data_dir, manifest)
        loaded = load(data_dir)
        assert list(loaded.keys()) == list(manifest.keys())
