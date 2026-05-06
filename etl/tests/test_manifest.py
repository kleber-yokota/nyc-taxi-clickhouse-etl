"""Tests for etl.manifest — CRUD operations on .push_manifest.json."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from etl.manifest import add_entry, load, save
from extract.core.push_manifest import PushManifestError


class TestLoad:
    """Tests for manifest.load()."""

    def test_returns_empty_dict_when_no_file(self, tmp_data_dir: Path) -> None:
        result = load(tmp_data_dir)
        assert result == {}

    def test_returns_empty_dict_when_file_missing(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert not manifest_path.exists()
        result = load(tmp_data_dir)
        assert result == {}

    def test_loads_valid_manifest(self, tmp_data_dir: Path) -> None:
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            }
        }
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        result = load(tmp_data_dir)
        assert result == manifest

    def test_loads_empty_manifest(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("{}")

        result = load(tmp_data_dir)
        assert result == {}

    def test_raises_on_invalid_json(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("{invalid json}")

        with pytest.raises(PushManifestError, match="invalid JSON"):
            load(tmp_data_dir)

    def test_raises_on_non_dict(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("[1, 2, 3]")

        with pytest.raises(PushManifestError, match="must be a dict"):
            load(tmp_data_dir)

    def test_raises_on_string(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text('"not a dict"')

        with pytest.raises(PushManifestError, match="must be a dict"):
            load(tmp_data_dir)

    def test_load_opens_file_with_read_mode(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("{}")

        with patch("builtins.open", return_value=iter([])) as mock_open:
            try:
                load(tmp_data_dir)
            except Exception:
                pass

            assert mock_open.called
            call_args = mock_open.call_args
            assert call_args[0][1] == "r"

    def test_raises_on_non_dict_with_type_name(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text("[1, 2, 3]")

        with pytest.raises(PushManifestError, match="got list"):
            load(tmp_data_dir)


class TestSave:
    """Tests for manifest.save()."""

    def test_creates_file(self, tmp_data_dir: Path) -> None:
        manifest = {"key": "value"}
        save(tmp_data_dir, manifest)

        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert manifest_path.exists()

    def test_writes_valid_json(self, tmp_data_dir: Path) -> None:
        manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "abc123",
            }
        }
        save(tmp_data_dir, manifest)

        manifest_path = tmp_data_dir / ".push_manifest.json"
        loaded = json.loads(manifest_path.read_text())
        assert loaded == manifest

    def test_overwrites_existing(self, tmp_data_dir: Path) -> None:
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text('{"old": "data"}')

        save(tmp_data_dir, {"new": "data"})

        loaded = json.loads(manifest_path.read_text())
        assert loaded == {"new": "data"}

    def test_raises_on_unwritable_dir(self, tmp_path: Path) -> None:
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o000)

        with pytest.raises(PushManifestError, match="cannot be written"):
            save(readonly_dir, {"key": "value"})

        readonly_dir.chmod(0o755)


class TestAddEntry:
    """Tests for manifest.add_entry()."""

    def test_adds_single_entry(self) -> None:
        manifest: dict = {}
        add_entry(manifest, "yellow/file.parquet", "data/yellow/file.parquet", "abc")
        assert manifest == {
            "yellow/file.parquet": {
                "s3_key": "data/yellow/file.parquet",
                "checksum": "abc",
            }
        }

    def test_adds_multiple_entries(self) -> None:
        manifest: dict = {}
        add_entry(manifest, "yellow/a.parquet", "data/yellow/a.parquet", "hash1")
        add_entry(manifest, "green/b.parquet", "data/green/b.parquet", "hash2")
        assert len(manifest) == 2
        assert manifest["yellow/a.parquet"]["checksum"] == "hash1"
        assert manifest["green/b.parquet"]["checksum"] == "hash2"

    def test_overwrites_existing_key(self) -> None:
        manifest = {
            "yellow/file.parquet": {
                "s3_key": "data/yellow/file.parquet",
                "checksum": "old_hash",
            }
        }
        add_entry(manifest, "yellow/file.parquet", "data/yellow/file.parquet", "new_hash")
        assert manifest["yellow/file.parquet"]["checksum"] == "new_hash"

    def test_preserves_other_entries(self) -> None:
        manifest = {
            "yellow/a.parquet": {"s3_key": "data/yellow/a.parquet", "checksum": "hash1"}
        }
        add_entry(manifest, "green/b.parquet", "data/green/b.parquet", "hash2")
        assert "yellow/a.parquet" in manifest
        assert manifest["yellow/a.parquet"]["checksum"] == "hash1"

    def test_empty_checksum(self) -> None:
        manifest: dict = {}
        add_entry(manifest, "yellow/file.parquet", "data/yellow/file.parquet", "")
        assert manifest["yellow/file.parquet"]["checksum"] == ""

    def test_s3_key_with_slash(self) -> None:
        manifest: dict = {}
        add_entry(manifest, "fhvhv/2024/01/file.parquet", "data/fhvhv/2024/01/file.parquet", "abc")
        assert manifest["fhvhv/2024/01/file.parquet"]["s3_key"] == "data/fhvhv/2024/01/file.parquet"
