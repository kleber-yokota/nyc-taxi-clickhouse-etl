"""Unit tests for downloader utility functions."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.downloader.downloader import (
    _backup_existing_file,
    _cleanup_stale_tmp,
    _safe_unlink,
)
from extract.downloader.downloader_actions import apply_mode as _apply_mode
from extract.downloader.downloader_actions import make_result as _make_result
from extract.downloader.downloader_actions import resolve_data_dir as _resolve_data_dir
from extract.core.state_manager import State


class TestResolveDataDir:
    def test_none_defaults_to_data(self):
        result = _resolve_data_dir(None)
        assert result == Path("data")

    def test_string_path(self):
        result = _resolve_data_dir("/custom/path")
        assert result == Path("/custom/path")

    def test_path_object(self, tmp_path: Path):
        result = _resolve_data_dir(tmp_path)
        assert result == tmp_path

    def test_empty_string_defaults_to_data(self):
        result = _resolve_data_dir("")
        assert result == Path("data")


class TestApplyMode:
    def test_full_mode_resets_state(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "hash1")
        assert state.is_downloaded("https://example.com/file.parquet")
        _apply_mode(state, "full")
        assert not state.is_downloaded("https://example.com/file.parquet")

    def test_incremental_mode_does_not_reset(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "hash1")
        _apply_mode(state, "incremental")
        assert state.is_downloaded("https://example.com/file.parquet")


class TestMakeResult:
    def test_returns_correct_dict(self):
        result = _make_result(10, 5, 3, 18)
        assert result == {"downloaded": 10, "skipped": 5, "failed": 3, "total": 18}

    def test_zero_values(self):
        result = _make_result(0, 0, 0, 0)
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}


class TestCleanupStaleTmp:
    def test_removes_existing_tmp(self, tmp_path: Path):
        tmp_file = tmp_path / "stale.tmp"
        tmp_file.write_bytes(b"stale content")
        _cleanup_stale_tmp(tmp_file)
        assert not tmp_file.exists()

    def test_noop_when_tmp_missing(self, tmp_path: Path):
        tmp_file = tmp_path / "missing.tmp"
        _cleanup_stale_tmp(tmp_file)
        assert not tmp_file.exists()


class TestSafeUnlink:
    def test_removes_existing_file(self, tmp_path: Path):
        test_file = tmp_path / "test.tmp"
        test_file.write_bytes(b"data")
        _safe_unlink(test_file)
        assert not test_file.exists()

    def test_noop_when_missing(self, tmp_path: Path):
        test_file = tmp_path / "missing.tmp"
        _safe_unlink(test_file)

    def test_multiple_unlinks_no_error(self, tmp_path: Path):
        test_file = tmp_path / "test.tmp"
        test_file.write_bytes(b"data")
        _safe_unlink(test_file)
        _safe_unlink(test_file)


class TestBackupExistingFile:
    def test_renames_to_old(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original data")
        _backup_existing_file(target)
        assert not target.exists()
        backup = tmp_path / "data.parquet.old"
        assert backup.exists()
        assert backup.read_bytes() == b"original data"

    def test_only_renames_existing(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original data")
        _backup_existing_file(target)
        assert not target.exists()
        assert (tmp_path / "data.parquet.old").exists()

    def test_preserves_content(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        content = b"x" * 1000
        target.write_bytes(content)
        _backup_existing_file(target)
        assert (tmp_path / "data.parquet.old").read_bytes() == content
