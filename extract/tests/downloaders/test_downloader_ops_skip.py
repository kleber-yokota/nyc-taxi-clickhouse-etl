"""Tests for should_skip_download in downloader_ops module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extract.downloader.downloader_ops import should_skip_download
from extract.core.state import CatalogEntry


class TestShouldSkipDownload:
    """Tests for should_skip_download function."""

    def test_skips_when_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True
        known_missing.is_missing.assert_called_once_with(entry.url)
        state.is_downloaded.assert_not_called()

    def test_skips_when_downloaded_and_file_exists(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry.filename).write_bytes(b"data")

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True

    def test_saves_empty_when_downloaded_but_no_file(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True)

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False
        state.save.assert_called_with(entry.url, "")

    def test_does_not_skip_when_not_downloaded(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False

    def test_does_not_skip_when_not_downloaded_no_file(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False
        state.save.assert_not_called()

    def test_checks_known_missing_first(self, tmp_path: Path):
        """Test that known_missing check happens before state check."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True
        state.is_downloaded.assert_not_called()
