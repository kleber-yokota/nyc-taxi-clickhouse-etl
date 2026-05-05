"""Tests for process_entry in downloader_ops module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader_ops import process_entry
from extract.core.state import CatalogEntry, ErrorType


class TestProcessEntry:
    """Tests for process_entry function."""

    def test_process_downloaded(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (1, 0, 0)

    def test_process_skipped(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="skipped"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)

    def test_process_failed(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="failed"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_process_exception_handles_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("download failed"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)
        state.log_error.assert_called_once()
        call_args = state.log_error.call_args
        assert call_args[0][0] == entry.url
        assert call_args[0][1] == ErrorType.UNKNOWN

    def test_process_with_non_zero_starting_counts(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 5, 10, 3)

        assert result == (6, 10, 3)

    def test_process_skips_when_known_missing_no_download(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("should not be called"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)

    def test_process_with_all_error_types(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", side_effect=Exception("test error")
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_process_incrementing_counts_correctly(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 100, 200, 50)

        assert result == (101, 200, 50)

    def test_process_skipped_incrementing_correctly(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="skipped"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 100, 200, 50)

        assert result == (100, 201, 50)

    def test_process_failed_incrementing_correctly(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="failed"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 100, 200, 50)

        assert result == (100, 200, 51)

    def test_process_exception_with_non_zero_failed(self, tmp_path: Path):
        """Test that exception path correctly increments failed count from non-zero start.

        This kills the mutmut_47 mutant that changes 'failed += 1' to 'failed = 1'.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("download failed"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 5)

        assert result == (0, 0, 6)
        state.log_error.assert_called_once()
