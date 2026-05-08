"""Tests for etl.manifest.Manifest."""

from pathlib import Path

import json

from etl.manifest import Manifest


def test_init_creates_manifest(tmp_data_dir: Path):
    manifest = Manifest(tmp_data_dir)
    result = manifest.init()
    assert result == {}


def test_init_loads_existing(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_data = {"yellow/file.parquet": {
        "status": "uploaded",
        "s3_key": "data/yellow/file.parquet",
        "checksum": "abc123",
    }}
    manifest_path.write_text(json.dumps(manifest_data))
    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == manifest_data


def test_load_invalid_json(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text("{invalid json}")
    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_record_download(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/file.parquet", "abc123")
    loaded = m._load()
    assert "yellow/file.parquet" in loaded
    assert loaded["yellow/file.parquet"]["status"] == "downloaded"
    assert loaded["yellow/file.parquet"]["checksum"] == "abc123"


def test_record_upload(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_upload("yellow/file.parquet")
    loaded = m._load()
    assert "yellow/file.parquet" in loaded
    assert loaded["yellow/file.parquet"]["status"] == "uploaded"


def test_record_download_failure(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download_failure("yellow/file.parquet", "network error")
    loaded = m._load()
    assert loaded["yellow/file.parquet"]["status"] == "download_failed"
    assert loaded["yellow/file.parquet"]["error"] == "network error"


def test_get_uploaded(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_upload("yellow/a.parquet")
    m.record_upload("green/b.parquet")
    uploaded = m.get_uploaded()
    assert "yellow/a.parquet" in uploaded
    assert "green/b.parquet" in uploaded


def test_get_not_uploaded(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/a.parquet", "abc")
    m.record_upload("green/b.parquet")
    not_uploaded = m.get_not_uploaded()
    assert "yellow/a.parquet" in not_uploaded
    assert "green/b.parquet" not in not_uploaded


def test_apply_mode_full_clears(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/a.parquet", "abc")
    m.apply_mode("full")
    loaded = m._load()
    assert loaded == {}


def test_apply_mode_incremental_keeps(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/a.parquet", "abc")
    m.apply_mode("incremental")
    loaded = m._load()
    assert "yellow/a.parquet" in loaded


def test_load_returns_empty_for_non_dict(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps([1, 2, 3]))
    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_load_returns_empty_for_empty_file(tmp_data_dir: Path):
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text("")
    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_save_writes_valid_json(tmp_data_dir: Path):
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("file.parquet", "abc123")
    path = tmp_data_dir / ".push_manifest.json"
    data = json.loads(path.read_text())
    assert data["file.parquet"]["status"] == "downloaded"


def test_recover_lists_parquet_files(tmp_data_dir: Path):
    # Create some fake parquet files
    (tmp_data_dir / "yellow").mkdir()
    (tmp_data_dir / "yellow" / "file.parquet").write_text("data")
    (tmp_data_dir / "green").mkdir()
    (tmp_data_dir / "green" / "file.parquet").write_text("data")

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert "yellow/file.parquet" in result
    assert "green/file.parquet" in result
    assert result["yellow/file.parquet"]["status"] == "downloaded"


def test_recover_empty_when_no_parquet_files(tmp_data_dir: Path):
    """Recovery returns empty dict when no parquet files exist on disk."""
    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_recover_with_subdirectories(tmp_data_dir: Path):
    """Recovery finds parquet files in nested subdirectories."""
    (tmp_data_dir / "yellow" / "2024").mkdir(parents=True)
    (tmp_data_dir / "yellow" / "2024" / "file.parquet").write_text("data")
    (tmp_data_dir / "green" / "2023").mkdir(parents=True)
    (tmp_data_dir / "green" / "2023" / "file.parquet").write_text("data")

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert "yellow/2024/file.parquet" in result
    assert "green/2023/file.parquet" in result


def test_record_download_preserves_existing_entries(tmp_data_dir: Path):
    """record_download preserves other entries in manifest."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_upload("yellow/other.parquet")
    m.record_download("green/new.parquet", "xyz789")

    loaded = m._load()
    assert "yellow/other.parquet" in loaded
    assert loaded["yellow/other.parquet"]["status"] == "uploaded"
    assert "green/new.parquet" in loaded
    assert loaded["green/new.parquet"]["status"] == "downloaded"


def test_record_upload_preserves_checksum(tmp_data_dir: Path):
    """record_upload preserves checksum from download entry."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/file.parquet", "abc123")
    m.record_upload("yellow/file.parquet")

    loaded = m._load()
    assert loaded["yellow/file.parquet"]["status"] == "uploaded"
    assert loaded["yellow/file.parquet"]["checksum"] == "abc123"


def test_record_upload_without_prior_download(tmp_data_dir: Path):
    """record_upload creates entry with pending status when no prior download."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_upload("yellow/file.parquet")

    loaded = m._load()
    assert "yellow/file.parquet" in loaded
    assert loaded["yellow/file.parquet"]["status"] == "uploaded"


def test_get_uploaded_empty_when_no_uploads(tmp_data_dir: Path):
    """get_uploaded returns empty set when no files are uploaded."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/file.parquet", "abc")

    uploaded = m.get_uploaded()
    assert uploaded == set()


def test_get_not_uploaded_empty_when_all_uploaded(tmp_data_dir: Path):
    """get_not_uploaded returns empty list when all files are uploaded."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/file.parquet", "abc")
    m.record_upload("yellow/file.parquet")

    not_uploaded = m.get_not_uploaded()
    assert not_uploaded == []


def test_apply_mode_full_clears_all_entries(tmp_data_dir: Path):
    """apply_mode with 'full' clears all entries including uploaded files."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_download("yellow/file.parquet", "abc")
    m.record_upload("green/file.parquet")
    m.apply_mode("full")

    loaded = m._load()
    assert loaded == {}


def test_load_corrupt_json_returns_empty(tmp_data_dir: Path):
    """Load returns empty dict when manifest contains corrupt JSON."""
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text("{this is not valid json}}}")

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_load_string_json_returns_empty(tmp_data_dir: Path):
    """Load returns empty dict when manifest is a JSON string instead of dict."""
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text('"just a string"')

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_load_number_json_returns_empty(tmp_data_dir: Path):
    """Load returns empty dict when manifest is a JSON number instead of dict."""
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text('42')

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_load_list_json_returns_empty(tmp_data_dir: Path):
    """Load returns empty dict when manifest is a JSON list instead of dict."""
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps([1, 2, 3]))

    m = Manifest(tmp_data_dir)
    result = m.init()
    assert result == {}


def test_init_when_manifest_exists_does_not_recover(tmp_data_dir: Path):
    """init does not trigger recovery when manifest file already exists."""
    import json

    existing_data = {
        "yellow/existing.parquet": {
            "status": "uploaded",
            "s3_key": "data/yellow/existing.parquet",
            "checksum": "abc123",
        }
    }
    manifest_path = tmp_data_dir / ".push_manifest.json"
    manifest_path.write_text(json.dumps(existing_data))

    # Create parquet files on disk that should NOT be recovered
    (tmp_data_dir / "green").mkdir()
    (tmp_data_dir / "green" / "new.parquet").write_text("data")

    m = Manifest(tmp_data_dir)
    result = m.init()

    # Should only contain existing data, not recovered files
    assert "yellow/existing.parquet" in result
    assert "green/new.parquet" not in result


def test_record_download_failure_preserves_existing(tmp_data_dir: Path):
    """record_download_failure preserves existing entries."""
    m = Manifest(tmp_data_dir)
    m.init()
    m.record_upload("yellow/other.parquet")
    m.record_download_failure("green/failed.parquet", "network error")

    loaded = m._load()
    assert "yellow/other.parquet" in loaded
    assert loaded["yellow/other.parquet"]["status"] == "uploaded"
    assert "green/failed.parquet" in loaded
    assert loaded["green/failed.parquet"]["status"] == "download_failed"
    assert loaded["green/failed.parquet"]["error"] == "network error"


def test_recovery_ignores_non_parquet_files(tmp_data_dir: Path):
    """Recovery ignores files that are not .parquet."""
    (tmp_data_dir / "yellow").mkdir()
    (tmp_data_dir / "yellow" / "file.parquet").write_text("data")
    (tmp_data_dir / "yellow" / "file.csv").write_text("data")
    (tmp_data_dir / "yellow" / "file.txt").write_text("data")

    m = Manifest(tmp_data_dir)
    result = m.init()

    assert "yellow/file.parquet" in result
    assert "yellow/file.csv" not in result
    assert "yellow/file.txt" not in result


def test_recovery_with_no_data_dir_files(tmp_data_dir: Path):
    """Recovery returns empty dict when data directory has no parquet files."""
    m = Manifest(tmp_data_dir)
    result = m.init()

    assert result == {}
    assert len(result) == 0
