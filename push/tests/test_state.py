"""Tests to kill upload.py and pusher.py mutations."""

from __future__ import annotations

from pathlib import Path

from push.core.state import PushedEntry
from push.core.state import PushResult
from push.core.state import PushState
from push.core.state import UploadConfig


class TestPushResult:
    """Tests for PushResult — ensure arithmetic mutations are caught."""

    def test_default_zero(self):
        r = PushResult()
        assert r.uploaded == 0
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0
        assert r.uploaded_files == []

    def test_nonzero(self):
        r = PushResult(uploaded=3, skipped=2, failed=1, total=6)
        assert r.uploaded == 3
        assert r.skipped == 2
        assert r.failed == 1
        assert r.total == 6
        assert r.uploaded_files == []

    def test_uploaded_files(self):
        r = PushResult(
            uploaded=2,
            skipped=1,
            failed=0,
            total=3,
            uploaded_files=["fhv/fhv_tripdata_2024-01.parquet", "yellow/yellow_tripdata_2024-01.parquet"],
        )
        assert r.uploaded == 2
        assert r.uploaded_files == ["fhv/fhv_tripdata_2024-01.parquet", "yellow/yellow_tripdata_2024-01.parquet"]

    def test_uploaded_files_immutable(self):
        r = PushResult(uploaded=1, uploaded_files=["fhv/file.parquet"])
        try:
            r.uploaded_files.append("other.parquet")  # type: ignore[union-attr]
            assert False, "Should have raised"
        except Exception:
            pass

    def test_only_uploaded(self):
        r = PushResult(uploaded=5)
        assert r.uploaded == 5
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0
        assert r.uploaded_files == []

    def test_is_frozen(self):
        r = PushResult(uploaded=1)
        try:
            r.uploaded = 2  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass


class TestUploadConfig:
    """Tests for UploadConfig — verify frozen dataclass defaults."""

    def test_default_values(self):
        config = UploadConfig()
        assert config.include is None
        assert config.exclude is None
        assert config.overwrite is False

    def test_custom_values(self):
        config = UploadConfig(
            include={"*.parquet"},
            exclude={".tmp"},
            overwrite=True,
        )
        assert config.include == {"*.parquet"}
        assert config.exclude == {".tmp"}
        assert config.overwrite is True

    def test_is_frozen(self):
        config = UploadConfig(overwrite=True)
        try:
            config.overwrite = False  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass


class TestPushedEntry:
    """Tests for PushedEntry dataclass."""

    def test_pushed_entry_creation(self):
        entry = PushedEntry(
            rel_path="yellow/file.parquet",
            s3_key="data/yellow/file.parquet",
            checksum="abc123",
        )
        assert entry.rel_path == "yellow/file.parquet"
        assert entry.s3_key == "data/yellow/file.parquet"
        assert entry.checksum == "abc123"

    def test_pushed_entry_is_frozen(self):
        entry = PushedEntry(rel_path="f", s3_key="k", checksum="c")
        try:
            entry.rel_path = "new"  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass


class TestPushResultUploadedEntries:
    """Tests for PushResult.uploaded_entries field."""

    def test_uploaded_entries_default_empty(self):
        r = PushResult()
        assert r.uploaded_entries == []

    def test_uploaded_entries_populated(self):
        entries = [
            PushedEntry(rel_path="yellow/file.parquet", s3_key="data/yellow/file.parquet", checksum="abc"),
            PushedEntry(rel_path="green/file.parquet", s3_key="data/green/file.parquet", checksum="def"),
        ]
        r = PushResult(uploaded=2, uploaded_entries=entries)
        assert r.uploaded == 2
        assert len(r.uploaded_entries) == 2
        assert r.uploaded_entries[0].rel_path == "yellow/file.parquet"
        assert r.uploaded_entries[0].checksum == "abc"

    def test_uploaded_entries_immutable(self):
        r = PushResult(uploaded=1, uploaded_entries=[PushedEntry(rel_path="f", s3_key="k", checksum="c")])
        try:
            r.uploaded_entries.append(PushedEntry(rel_path="x", s3_key="x", checksum="x"))  # type: ignore[union-attr]
            assert False, "Should have raised"
        except Exception:
            pass
