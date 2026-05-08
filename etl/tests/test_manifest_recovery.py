"""Tests for Manifest._recover() with uploaded_entries — recovery from upload state + disk."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl.manifest import Manifest


# ── Fixtures ─────────────────────────────────────────────────────────────


def _make_uploaded_entries(tmp_path: Path) -> dict:
    """Create uploaded_entries dict mimicking get_existing_uploads output."""
    return {
        str(tmp_path / "yellow" / "trip_2024.parquet"): {
            "s3_key": "data/yellow/trip_2024.parquet",
            "checksum": "abc123def456",
        },
        str(tmp_path / "green" / "trip_2024.parquet"): {
            "s3_key": "data/green/trip_2024.parquet",
            "checksum": "789ghi012jkl",
        },
    }


# ── Phase 3 Tests ───────────────────────────────────────────────────────


class TestRecoverWithUploadedEntries:
    """Test: recover_with_uploaded_entries — entries from upload → status='uploaded'."""

    def test_status_is_uploaded(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        m = Manifest(tmp_data_dir)
        # Remove any existing manifest so _recover triggers
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert "yellow/trip_2024.parquet" in result
        assert "green/trip_2024.parquet" in result
        assert result["yellow/trip_2024.parquet"]["status"] == "uploaded"
        assert result["green/trip_2024.parquet"]["status"] == "uploaded"


class TestRecoverConvertsAbsoluteToRelative:
    """Test: recover_converts_absolute_to_relative — local_path absolute → rel_path relative."""

    def test_relative_keys(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        # Keys should be relative to data_dir
        assert "yellow/trip_2024.parquet" in result
        assert "green/trip_2024.parquet" in result
        # Absolute paths should NOT appear as keys
        for key in result:
            assert not key.startswith(str(tmp_data_dir))


class TestRecoverPreservesChecksum:
    """Test: recover_preserves_checksum — checksum from upload → manifest has checksum."""

    def test_checksum_preserved(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert result["yellow/trip_2024.parquet"]["checksum"] == "abc123def456"
        assert result["green/trip_2024.parquet"]["checksum"] == "789ghi012jkl"


class TestRecoverDiskFallbackForMissing:
    """Test: recover_disk_fallback_for_missing — .parquet on disk but not in upload → status='downloaded'."""

    def test_disk_files_get_downloaded_status(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        # Create a parquet file on disk that's NOT in uploaded_entries
        (tmp_data_dir / "fhv").mkdir()
        (tmp_data_dir / "fhv" / "fhv_2024.parquet").write_text("fhv_data")

        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert "fhv/fhv_2024.parquet" in result
        assert result["fhv/fhv_2024.parquet"]["status"] == "downloaded"


class TestRecoverNoEntriesNoDisk:
    """Test: recover_no_entries_no_disk — no upload entries and no disk files → {}."""

    def test_empty_when_nothing(self, tmp_data_dir: Path):
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=None)

        assert result == {}


class TestRecoverEmptyDictIgnored:
    """Test: recover_empty_dict_ignored — uploaded_entries={} → only scans disk."""

    def test_empty_dict_treated_as_no_entries(self, tmp_data_dir: Path):
        # Upload empty dict
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        # No disk files → empty result
        result = m.init(uploaded_entries={})

        assert result == {}

    def test_empty_dict_still_scans_disk(self, tmp_data_dir: Path):
        """Edge: empty dict should still scan disk for parquet files."""
        (tmp_data_dir / "yellow").mkdir()
        (tmp_data_dir / "yellow" / "disk_only.parquet").write_text("data")

        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries={})

        assert "yellow/disk_only.parquet" in result
        assert result["yellow/disk_only.parquet"]["status"] == "downloaded"


class TestRecoverUploadTakesPriority:
    """Test: recover_upload_takes_priority — same file in upload and disk → status='uploaded'."""

    def test_upload_overrides_disk(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        # Also create the same file on disk
        (tmp_data_dir / "yellow").mkdir(parents=True, exist_ok=True)
        (tmp_data_dir / "yellow" / "trip_2024.parquet").write_text("disk_data")

        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert result["yellow/trip_2024.parquet"]["status"] == "uploaded"
        assert result["yellow/trip_2024.parquet"]["checksum"] == "abc123def456"


class TestInitWithUploadedEntries:
    """Test: init_with_uploaded_entries — init() passes entries to _recover."""

    def test_init_passes_entries(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert "yellow/trip_2024.parquet" in result
        assert result["yellow/trip_2024.parquet"]["status"] == "uploaded"


class TestInitNoEntriesCallsRecoverEmpty:
    """Test: init_no_entries_calls_recover_empty — init() without entries → _recover({})."""

    def test_init_without_entries(self, tmp_data_dir: Path):
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init()

        # Should return empty dict when no entries and no disk files
        assert result == {}


class TestRecoverPreservesS3Key:
    """Test: recover_preserves_s3_key — s3_key from upload → manifest has s3_key."""

    def test_s3_key_preserved(self, tmp_data_dir: Path):
        entries = _make_uploaded_entries(tmp_data_dir)
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert result["yellow/trip_2024.parquet"]["s3_key"] == "data/yellow/trip_2024.parquet"
        assert result["green/trip_2024.parquet"]["s3_key"] == "data/green/trip_2024.parquet"


# ── Additional Edge Cases ───────────────────────────────────────────────


class TestRecoverEdgeCases:
    """Edge case tests for _recover()."""

    def test_none_entries(self, tmp_data_dir: Path):
        """None entries should be treated as empty."""
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=None)
        assert result == {}

    def test_missing_s3_key_in_entry(self, tmp_data_dir: Path):
        """Entry missing s3_key should get default."""
        entries = {
            str(tmp_data_dir / "yellow" / "trip.parquet"): {
                "checksum": "abc123",
            }
        }
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        # Should fall back to default s3_key pattern
        assert result["yellow/trip.parquet"]["s3_key"] == "data/yellow/trip.parquet"

    def test_missing_checksum_in_entry(self, tmp_data_dir: Path):
        """Entry missing checksum should have None/empty checksum."""
        entries = {
            str(tmp_data_dir / "yellow" / "trip.parquet"): {
                "s3_key": "data/yellow/trip.parquet",
            }
        }
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert result["yellow/trip.parquet"]["checksum"] is None

    def test_init_preserves_existing_manifest(self, tmp_data_dir: Path):
        """If manifest already exists, init returns it without calling _recover."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        existing = {
            "yellow/existing.parquet": {
                "status": "uploaded",
                "s3_key": "data/yellow/existing.parquet",
                "checksum": "prev",
            }
        }
        manifest_path.write_text(json.dumps(existing))

        m = Manifest(tmp_data_dir)
        result = m.init(uploaded_entries={"not_used": {}})

        assert result == existing

    def test_recover_with_nested_upload_entries(self, tmp_data_dir: Path):
        """Upload entries with nested paths should produce correct relative keys."""
        entries = {
            str(tmp_data_dir / "yellow" / "2024" / "01" / "trip.parquet"): {
                "s3_key": "data/yellow/2024/01/trip.parquet",
                "checksum": "abc",
            },
        }
        m = Manifest(tmp_data_dir)
        manifest_path = tmp_data_dir / ".push_manifest.json"
        if manifest_path.exists():
            manifest_path.unlink()

        result = m.init(uploaded_entries=entries)

        assert "yellow/2024/01/trip.parquet" in result
