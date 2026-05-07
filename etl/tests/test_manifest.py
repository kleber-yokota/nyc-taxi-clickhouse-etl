"""Tests for etl.manifest."""

from pathlib import Path

import json

from etl.manifest import add_entry, load_manifest, save_manifest


def test_load_nonexistent(tmp_data_dir: Path):
    result = load_manifest(tmp_data_dir)
    assert result == {}


def test_load_valid(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_data = {"yellow/tripdata_2024-01.parquet": {"s3_key": "data/yellow/...", "checksum": "abc123"}}
    manifest_path.write_text(json.dumps(manifest_data))
    result = load_manifest(tmp_data_dir)
    assert result == manifest_data


def test_load_invalid_json(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text("{invalid json}")
    result = load_manifest(tmp_data_dir)
    assert result == {}


def test_save_and_load_roundtrip(tmp_data_dir: Path):
    manifest: dict = {}
    add_entry(manifest, "yellow/tripdata_2024-01.parquet", "data/yellow/tripdata_2024-01.parquet", "abc123")
    save_manifest(tmp_data_dir, manifest)
    loaded = load_manifest(tmp_data_dir)
    assert "yellow/tripdata_2024-01.parquet" in loaded
    assert loaded["yellow/tripdata_2024-01.parquet"]["s3_key"] == "data/yellow/tripdata_2024-01.parquet"
    assert loaded["yellow/tripdata_2024-01.parquet"]["checksum"] == "abc123"


def test_add_entry():
    manifest: dict = {}
    add_entry(manifest, "green/tripdata_2023-06.parquet", "data/green/tripdata_2023-06.parquet", "xyz789")
    assert "green/tripdata_2023-06.parquet" in manifest
    assert manifest["green/tripdata_2023-06.parquet"]["s3_key"] == "data/green/tripdata_2023-06.parquet"
    assert manifest["green/tripdata_2023-06.parquet"]["checksum"] == "xyz789"


def test_load_manifest_returns_empty_for_non_dict(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps([1, 2, 3]))
    result = load_manifest(tmp_data_dir)
    assert result == {}


def test_load_manifest_returns_empty_for_string(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps("just a string"))
    result = load_manifest(tmp_data_dir)
    assert result == {}


def test_load_manifest_returns_empty_for_number(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps(42))
    result = load_manifest(tmp_data_dir)
    assert result == {}


def test_save_manifest_creates_file(tmp_data_dir: Path):
    save_manifest(tmp_data_dir, {})
    path = tmp_data_dir / ".push_manifest.json"
    assert path.exists()


def test_save_manifest_uses_correct_filename(tmp_data_dir: Path):
    save_manifest(tmp_data_dir, {})
    assert (tmp_data_dir / ".push_manifest.json").exists()
    assert not (tmp_data_dir / "manifest.json").exists()


def test_add_entry_overwrites_existing(tmp_data_dir: Path):
    manifest: dict = {}
    add_entry(manifest, "yellow/tripdata_2024-01.parquet", "data/yellow/v1", "aaa")
    add_entry(manifest, "yellow/tripdata_2024-01.parquet", "data/yellow/v2", "bbb")
    assert manifest["yellow/tripdata_2024-01.parquet"]["s3_key"] == "data/yellow/v2"
    assert manifest["yellow/tripdata_2024-01.parquet"]["checksum"] == "bbb"


def test_save_and_load_roundtrip_multiple_entries(tmp_data_dir: Path):
    manifest: dict = {}
    add_entry(manifest, "yellow/tripdata_2024-01.parquet", "data/yellow/01", "aaa")
    add_entry(manifest, "green/tripdata_2024-02.parquet", "data/green/02", "bbb")
    add_entry(manifest, "yellow/tripdata_2024-03.parquet", "data/yellow/03", "ccc")
    save_manifest(tmp_data_dir, manifest)
    loaded = load_manifest(tmp_data_dir)
    assert len(loaded) == 3
    assert loaded["yellow/tripdata_2024-01.parquet"]["s3_key"] == "data/yellow/01"
    assert loaded["green/tripdata_2024-02.parquet"]["checksum"] == "bbb"
    assert loaded["yellow/tripdata_2024-03.parquet"]["s3_key"] == "data/yellow/03"
