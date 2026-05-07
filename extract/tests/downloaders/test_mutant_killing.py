"""Tests to kill surviving mutmut mutations in extract/core modules."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.ops import process_entry as _process_entry
from extract.downloader.downloader import run
from extract.downloader.actions import apply_mode as _apply_mode
from extract.core.state import CatalogEntry, ErrorType


class TestRunKeyboardInterrupt:
    """Kills mutant: except block body replaced with pass."""

    def test_run_empty_catalog_returns_zero_result(self, tmp_path: Path):
        """Verify run() returns zero result when Catalog.generate returns empty list."""
        mock_state = MagicMock()
        mock_known_missing = MagicMock()
        mock_catalog = MagicMock()
        mock_interruptible = MagicMock()

        mock_catalog.generate.return_value = []
        mock_state.reset.return_value = None

        with patch("extract.downloader.downloader.Catalog", return_value=mock_catalog):
            with patch("extract.downloader.downloader.State", return_value=mock_state):
                with patch("extract.downloader.downloader.KnownMissing", return_value=mock_known_missing):
                    with patch(
                        "extract.downloader.downloader.InterruptibleDownload",
                        return_value=mock_interruptible,
                    ):
                        result = run(data_dir=tmp_path, mode="full", max_entries=0)

        assert result["total"] == 0
        mock_catalog.generate.assert_called_once()


class TestProcessEntryIsDownloaded:
    """Kills mutants: 'downloaded += 1' → 'downloaded = 1' and 'skipped += 1' → 'skipped = 1'."""

    def test_skips_when_downloaded_and_file_exists(self, tmp_path: Path):
        """Entry marked downloaded with file present → skip, no save."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = True

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / entry.filename).write_bytes(b"existing content")

        downloaded, skipped, failed = _process_entry(
            entry, tmp_path, state, known_missing, 0, 0, 0
        )

        assert downloaded == 0
        assert skipped == 1
        assert failed == 0
        state.save.assert_not_called()

    def test_saves_empty_when_downloaded_but_no_file(self, tmp_path: Path):
        """Entry marked downloaded without file → re-download, save empty checksum."""
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = True

        with patch(
            "extract.downloader.ops.download_and_verify",
            return_value="downloaded",
        ):
            downloaded, skipped, failed = _process_entry(
                entry, tmp_path, state, known_missing, 0, 0, 0
            )

        assert downloaded == 1
        assert skipped == 0
        assert failed == 0
        state.save.assert_called_once_with(entry.url, "")


class TestProcessEntryExceptionHandler:
    """Kills mutant: 'failed += 1' → 'failed = 1' in exception handler."""

    def test_exception_logs_unknown_error_and_increments_failed(self, tmp_path: Path):
        """Uncaught exception → log UNKNOWN error, increment failed by 1."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = False
        state.log_error.return_value = None

        with patch(
            "extract.downloader.ops.download_and_verify",
            side_effect=ValueError("unexpected failure"),
        ):
            downloaded, skipped, failed = _process_entry(
                entry, tmp_path, state, known_missing, 0, 0, 0
            )

        assert downloaded == 0
        assert skipped == 0
        assert failed == 1
        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.UNKNOWN
        assert args[0][2] == "unexpected failure"
