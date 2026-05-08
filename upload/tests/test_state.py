"""Tests to kill upload.py and engine.py mutations."""

from __future__ import annotations

from pathlib import Path

from upload.core.state import UploadEntry, UploadResult, UploadState, UploadConfig


class TestUploadEntry:
    """Tests for UploadEntry — verify frozen dataclass."""

    def test_basic(self):
        entry = UploadEntry(
            rel_path="yellow/file.parquet",
            s3_key="data/yellow/file.parquet",
            checksum="abc123",
        )
        assert entry.rel_path == "yellow/file.parquet"
        assert entry.s3_key == "data/yellow/file.parquet"
        assert entry.checksum == "abc123"

    def test_is_frozen(self):
        entry = UploadEntry(rel_path="a", s3_key="b", checksum="c")
        try:
            entry.rel_path = "x"  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass


class TestUploadResult:
    """Tests for UploadResult — ensure arithmetic mutations are caught."""

    def test_default_zero(self):
        r = UploadResult()
        assert r.uploaded == 0
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0
        assert r.entries == []

    def test_nonzero(self):
        r = UploadResult(uploaded=3, skipped=2, failed=1, total=6)
        assert r.uploaded == 3
        assert r.skipped == 2
        assert r.failed == 1
        assert r.total == 6
        assert r.entries == []

    def test_entries(self):
        entries = [
            UploadEntry(rel_path="fhv/file.parquet", s3_key="data/fhv/file.parquet", checksum="abc"),
            UploadEntry(rel_path="yellow/file.parquet", s3_key="data/yellow/file.parquet", checksum="def"),
        ]
        r = UploadResult(
            uploaded=2,
            skipped=1,
            failed=0,
            total=3,
            entries=entries,
        )
        assert r.uploaded == 2
        assert len(r.entries) == 2
        assert r.entries[0].rel_path == "fhv/file.parquet"

    def test_entries_immutable(self):
        r = UploadResult(uploaded=1, entries=[UploadEntry(rel_path="a", s3_key="b", checksum="c")])
        try:
            r.entries.append(UploadEntry(rel_path="x", s3_key="y", checksum="z"))  # type: ignore[union-attr]
            assert False, "Should have raised"
        except Exception:
            pass

    def test_only_uploaded(self):
        r = UploadResult(uploaded=5)
        assert r.uploaded == 5
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0
        assert r.entries == []

    def test_is_frozen(self):
        r = UploadResult(uploaded=1)
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
