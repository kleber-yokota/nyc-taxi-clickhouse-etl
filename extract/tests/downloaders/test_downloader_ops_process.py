"""Tests for process_entry exercising all code paths."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader_ops import process_entry
from extract.core.state import CatalogEntry
from extract.core.state_manager import State


class TestProcessEntryDownloaded:
    """Tests for process_entry when download succeeds."""

    def test_returns_incremented_downloaded(self, tmp_path: Path):
        """Verify returns incremented downloaded count."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="downloaded",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (1, 0, 0)

    def test_preserves_existing_counts(self, tmp_path: Path):
        """Verify preserves existing downloaded/skipped/failed counts."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="downloaded",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 10, 5, 3)

        assert result == (11, 5, 3)

    def test_calls_download_and_verify_with_correct_args(self, tmp_path: Path):
        """Verify download_and_verify is called with correct arguments."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="downloaded",
        ) as mock_download:
            process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        mock_download.assert_called_once_with(entry, tmp_path, state, known_missing)


class TestProcessEntrySkipped:
    """Tests for process_entry when download returns skipped."""

    def test_returns_incremented_skipped(self, tmp_path: Path):
        """Verify returns incremented skipped count."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="skipped",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)

    def test_preserves_downloaded_on_skip(self, tmp_path: Path):
        """Verify downloaded count unchanged on skip."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="skipped",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 7, 0, 0)

        assert result == (7, 1, 0)


class TestProcessEntryFailed:
    """Tests for process_entry when download returns failed."""

    def test_returns_incremented_failed(self, tmp_path: Path):
        """Verify returns incremented failed count."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="failed",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_preserves_downloaded_on_fail(self, tmp_path: Path):
        """Verify downloaded count unchanged on failure."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            return_value="failed",
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 5, 2, 0)

        assert result == (5, 2, 1)


class TestProcessEntryException:
    """Tests for process_entry when download raises an exception."""

    def test_handles_http_error(self, tmp_path: Path):
        """Verify handles HTTPError exception."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=error,
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_handles_network_error(self, tmp_path: Path):
        """Verify handles RequestException."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        import requests

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=requests.ConnectionError("timeout"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_handles_unknown_error(self, tmp_path: Path):
        """Verify handles ValueError exception."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=ValueError("unexpected"),
        ):
            result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 0, 1)

    def test_calls_handle_download_error(self, tmp_path: Path):
        """Verify handle_download_error is called on exception."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        import requests

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=requests.HTTPError("500"),
        ) as mock_download:
            with patch(
                "extract.downloader.downloader_ops.handle_download_error",
            ) as mock_handle:
                process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

                mock_download.assert_called_once()
                mock_handle.assert_called_once()

    def test_log_error_records_http_404(self, tmp_path: Path):
        """Verify HTTP 404 logs MISSING_FILE error."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=error,
        ):
            process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        calls = state.log_error.call_args_list
        assert len(calls) >= 1
        assert calls[-1][0][1].value == "missing_file"

    def test_log_error_records_network_error(self, tmp_path: Path):
        """Verify network error logs NETWORK_ERROR."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        from extract.core.known_missing import KnownMissing
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        import requests

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=requests.ConnectionError("timeout"),
        ):
            process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        calls = state.log_error.call_args_list
        assert len(calls) >= 1
        assert calls[-1][0][1].value == "network_error"


class TestProcessEntryMultiple:
    """Tests for process_entry with multiple sequential calls."""

    def test_cumulative_counts_across_calls(self, tmp_path: Path):
        """Verify counts accumulate correctly across multiple entries."""
        entry1 = CatalogEntry("yellow", 2024, 1)
        entry2 = CatalogEntry("yellow", 2024, 2)
        entry3 = CatalogEntry("yellow", 2024, 3)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=["downloaded", "skipped", "failed"],
        ):
            result1 = process_entry(entry1, tmp_path, state, known_missing, 0, 0, 0)
            result2 = process_entry(entry2, tmp_path, state, known_missing, *result1)
            result3 = process_entry(entry3, tmp_path, state, known_missing, *result2)

        assert result1 == (1, 0, 0)
        assert result2 == (1, 1, 0)
        assert result3 == (1, 1, 1)

    def test_mixed_results_from_start(self, tmp_path: Path):
        """Verify correct counts when starting from zero with mixed results."""
        entry1 = CatalogEntry("green", 2024, 1)
        entry2 = CatalogEntry("green", 2024, 2)
        entry3 = CatalogEntry("green", 2024, 3)
        entry4 = CatalogEntry("green", 2024, 4)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
            side_effect=["downloaded", "downloaded", "failed", "skipped"],
        ):
            r1 = process_entry(entry1, tmp_path, state, known_missing, 0, 0, 0)
            r2 = process_entry(entry2, tmp_path, state, known_missing, *r1)
            r3 = process_entry(entry3, tmp_path, state, known_missing, *r2)
            r4 = process_entry(entry4, tmp_path, state, known_missing, *r3)

        assert r4 == (2, 1, 1)


class TestProcessEntrySkipViaShouldSkip:
    """Tests for process_entry when should_skip_download returns True."""

    def test_skips_when_known_missing(self, tmp_path: Path):
        """Verify skips download when URL is in known_missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        known_missing.add(entry.url)

        result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)

    def test_skips_when_downloaded_and_exists(self, tmp_path: Path):
        """Verify skips when file exists and is in state."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry.filename).write_bytes(b"existing")

        state.save(entry.url, "checksum")

        result = process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        assert result == (0, 1, 0)

    def test_does_not_call_download_when_skipped(self, tmp_path: Path):
        """Verify download_and_verify is not called when skipping."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        known_missing.add(entry.url)

        with patch(
            "extract.downloader.downloader_ops.download_and_verify",
        ) as mock_download:
            process_entry(entry, tmp_path, state, known_missing, 0, 0, 0)

        mock_download.assert_not_called()
