"""Unit tests for small downloader utility functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extract.downloader.downloader import (
    _resolve_data_dir,
    _safe_unlink,
)
from extract.downloader.downloader_actions import apply_mode as _apply_mode
from extract.downloader.downloader_actions import make_result as _make_result


class TestResolveDataDir:
    def test_none_returns_data(self):
        assert _resolve_data_dir(None) == Path("data")

    def test_string_path(self):
        assert _resolve_data_dir("/custom/path") == Path("/custom/path")

    def test_path_object(self, tmp_path: Path):
        assert _resolve_data_dir(tmp_path) == tmp_path


class TestApplyMode:
    def test_full_resets_state(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, "full")
        mock_state.reset.assert_called_once()

    def test_incremental_no_reset(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, "incremental")
        mock_state.reset.assert_not_called()

    def test_none_no_reset(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, None)
        mock_state.reset.assert_not_called()


class TestMakeResult:
    def test_returns_dict_with_exact_values(self):
        result = _make_result(5, 3, 2, 10)
        assert result == {"downloaded": 5, "skipped": 3, "failed": 2, "total": 10}

    def test_all_zeros(self):
        result = _make_result(0, 0, 0, 0)
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_all_values(self):
        result = _make_result(100, 50, 25, 175)
        assert result["downloaded"] == 100
        assert result["skipped"] == 50
        assert result["failed"] == 25
        assert result["total"] == 175

    def test_returns_new_dict(self):
        r1 = _make_result(1, 2, 3, 6)
        r2 = _make_result(4, 5, 6, 15)
        assert r1 is not r2
        assert r1 != r2


class TestSafeUnlink:
    def test_removes_file(self, tmp_path: Path):
        f = tmp_path / "to_delete.txt"
        f.write_text("data")
        _safe_unlink(f)
        assert not f.exists()

    def test_no_error_on_missing(self, tmp_path: Path):
        f = tmp_path / "missing.txt"
        _safe_unlink(f)  # should not raise

    def test_removes_symlink(self, tmp_path: Path):
        target = tmp_path / "target.txt"
        target.write_text("data")
        symlink = tmp_path / "link.txt"
        symlink.symlink_to(target)
        _safe_unlink(symlink)
        assert not symlink.exists()
        assert target.exists()  # target still exists
