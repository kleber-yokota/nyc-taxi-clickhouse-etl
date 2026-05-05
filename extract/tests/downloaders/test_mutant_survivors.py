"""Tests targeting mutmut survivors in downloader_ops and downloader_util."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader_ops import process_entry, should_skip_download
from extract.core.state import CatalogEntry, ErrorType
from extract.downloader.downloader_util import (
    backup_existing_file,
    handle_http_error,
    handle_network_error,
)
from extract.core.state_manager import State


class TestShouldSkipDownloadKnownMissing:
    """Tests to kill mutmut survivors in should_skip_download known_missing path."""

    def test_known_missing_prevents_state_check(self, tmp_path: Path):
        """If known_missing check is removed, state.is_downloaded would be called
        and the function would return False instead of True.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True
        # Verify state.is_downloaded was NOT called (early return on known_missing)
        state.is_downloaded.assert_not_called()

    def test_known_missing_not_in_state(self, tmp_path: Path):
        """URL in known_missing but not in state should still skip."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True


class TestShouldSkipDownloadFileExists:
    """Tests to kill mutmut survivors in should_skip_download file existence path."""

    def test_downloaded_no_file_saves_empty(self, tmp_path: Path):
        """When state says downloaded but file is missing, should_save empty checksum
        and return False (don't skip, re-download needed).
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        # Don't create the target file
        target_dir = tmp_path / entry.target_dir
        # target_dir not created, so file doesn't exist

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False
        state.save.assert_called_once_with(entry.url, "")

    def test_downloaded_file_exists_skips(self, tmp_path: Path):
        """When state says downloaded AND file exists, should skip."""
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
        # save should NOT be called when file exists
        state.save.assert_not_called()


class TestProcessEntrySkipPath:
    """Tests to kill mutmut survivors in process_entry skip path."""

    def test_skip_path_no_download_called(self, tmp_path: Path):
        """When should_skip_download returns True, download_and_verify must NOT be called.
        This kills mutants that remove the skip check or change the skip increment.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("should not be called"),
        ) as mock_download:
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)
        mock_download.assert_not_called()

    def test_skip_path_with_nonzero_counts(self, tmp_path: Path):
        """Verify skip correctly increments from non-zero counts.
        This kills mutants that change 'skipped += 1' to 'skipped = 1'.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("should not be called"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 10, 20, 5)

        assert result == (10, 21, 5)


class TestProcessEntryResultHandling:
    """Tests to kill mutmut survivors in process_entry result handling."""

    def test_skipped_result_increments_skipped_only(self, tmp_path: Path):
        """When download_and_verify returns 'skipped', only skipped count increments.
        This kills mutants that change the result comparison.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="skipped"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 10, 20, 5)

        assert result == (10, 21, 5)

    def test_failed_result_increments_failed_only(self, tmp_path: Path):
        """When download_and_verify returns 'failed', only failed count increments."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="failed"
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 10, 20, 5)

        assert result == (10, 20, 6)

    def test_exception_increments_failed_with_nonzero(self, tmp_path: Path):
        """When download_and_verify raises, failed count increments from non-zero.
        This kills mutants that change 'failed += 1' to 'failed = 1'.
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
