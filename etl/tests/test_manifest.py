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
