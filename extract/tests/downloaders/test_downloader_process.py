"""Tests for _process_entry function in downloader_ops module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from extract.downloader.downloader import _process_entry
from extract.core.state import CatalogEntry, ErrorType


class TestProcessEntry:
    def test_skips_known_missing(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = _process_entry(
            entry, Path("."), state, known_missing, 0, 0, 0
        )

        assert result == (0, 1, 0)
        known_missing.is_missing.assert_called_once_with(entry.url)
        state.is_downloaded.assert_not_called()

    def test_skips_downloaded_existing(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing_mock = MagicMock()
        known_missing_mock.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing_mock, 0, 0, 0
            )

        assert state.is_downloaded.called

    def test_saves_empty_when_downloaded_no_file(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        target_dir = tmp_path / "yellow"
        target_dir.mkdir()

        with patch("extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"):
            _process_entry(
                entry, tmp_path, state, known_missing, 0, 0, 0
            )

        state.save.assert_called_once_with(entry.url, "")

    def test_process_entry_with_known_missing_skips_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = _process_entry(
            entry, Path("."), state, known_missing, 5, 10, 3
        )

        assert result == (5, 11, 3)
        state.is_downloaded.assert_not_called()
        state.save.assert_not_called()

    def test_counts_downloaded(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (1, 0, 0)

    def test_counts_skipped_from_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="skipped"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 1, 0)

    def test_counts_failed_from_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify", return_value="failed"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 0, 1)

    def test_exception_counts_failed(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=RuntimeError("boom"),
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 0, 1)
        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][0] == entry.url
        assert args[0][1] == ErrorType.UNKNOWN

    def test_known_missing_does_not_check_state(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

