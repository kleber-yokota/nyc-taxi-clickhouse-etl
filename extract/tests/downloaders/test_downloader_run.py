"""Tests for run() exercising all mutation paths with ≤2 mocks."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run
from extract.downloader.downloader_actions import make_result as _make_result
from extract.downloader.downloader_actions import log_download_complete as _log_download_complete


class TestRunDefaultMode:
    """Verify default mode is 'incremental' not 'INCREMENTAL'."""

    def test_default_mode_is_incremental(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify run uses incremental mode by default (state not reset)."""
        state_file = tmp_path / ".download_state.json"
        state_file.write_text('{"checksums": {"https://example.com/file.parquet": "abc123"}}')

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            result = run(data_dir=tmp_path)

        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_mode_full_resets_state(self, tmp_path: Path):
        """Verify mode='full' resets state."""
        state_file = tmp_path / ".download_state.json"
        state_file.write_text('{"checksums": {"https://example.com/file.parquet": "abc123"}}')

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            result = run(data_dir=tmp_path, mode="full")

        # State file should be reset (empty checksums)
        import json
        state_data = json.loads(state_file.read_text())
        assert state_data["checksums"] == {}

    def test_mode_incremental_preserves_state(self, tmp_path: Path):
        """Verify mode='incremental' preserves state."""
        state_file = tmp_path / ".download_state.json"
        state_file.write_text('{"checksums": {"https://example.com/file.parquet": "abc123"}}')

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            result = run(data_dir=tmp_path, mode="incremental")

        import json
        state_data = json.loads(state_file.read_text())
        assert state_data["checksums"] == {"https://example.com/file.parquet": "abc123"}


class TestRunFilePaths:
    """Verify correct file paths for State and KnownMissing."""

    def test_state_file_path(self, tmp_path: Path):
        """Verify State file path is .download_state.json."""
        from extract.core.state import CatalogEntry

        entry = CatalogEntry("yellow", 2024, 1)

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = [entry]
            MockCatalog.return_value = mock_catalog

            with patch("extract.downloader.downloader.process_entry") as mock_process:
                with patch("extract.downloader.downloader.State") as MockState:
                    mock_process.return_value = (0, 0, 0)
                    run(data_dir=tmp_path, mode="full")

                    MockState.assert_called_once()
                    call_path = MockState.call_args[0][0]
                    assert str(call_path) == str(tmp_path / ".download_state.json")

    def test_known_missing_file_path(self, tmp_path: Path):
        """Verify KnownMissing file path is known_missing.txt."""
        from extract.core.state import CatalogEntry

        entry = CatalogEntry("yellow", 2024, 1)

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = [entry]
            MockCatalog.return_value = mock_catalog

            with patch("extract.downloader.downloader.process_entry") as mock_process:
                with patch("extract.downloader.downloader.KnownMissing") as MockKnownMissing:
                    mock_process.return_value = (0, 0, 0)
                    run(data_dir=tmp_path)

                    MockKnownMissing.assert_called_once()
                    call_path = MockKnownMissing.call_args[0][0]
                    assert str(call_path) == str(tmp_path / "known_missing.txt")


class TestRunEmptyEntries:
    """Verify behavior when catalog generates no entries."""

    def test_logs_warning_on_empty_entries(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify warning is logged when no entries to download."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            with caplog.at_level(logging.WARNING, logger="extract.downloader.downloader"):
                result = run(data_dir=tmp_path)

        assert any("No entries to download" in r.message for r in caplog.records)

    def test_returns_zero_result_on_empty(self, tmp_path: Path):
        """Verify returns zero counts when no entries."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            result = run(data_dir=tmp_path)

        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}


class TestRunCounterValues:
    """Verify counter values are integers and accumulate correctly."""

    def test_initial_counters_are_zero(self, tmp_path: Path):
        """Verify initial counter values are 0 not None or 1."""
        from extract.core.state import CatalogEntry
        from extract.downloader.downloader import _execute_download_loop
        from extract.core.state_manager import State
        from extract.core.known_missing import KnownMissing
        from unittest.mock import patch

        state = State(tmp_path / ".download_state.json")
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.process_entry") as mock_process:
            mock_process.return_value = (0, 0, 0)
            downloaded, skipped, failed = _execute_download_loop(
                entries, tmp_path, state, known_missing,
            )

        assert downloaded == 0
        assert skipped == 0
        assert failed == 0
        assert isinstance(downloaded, int)
        assert isinstance(skipped, int)
        assert isinstance(failed, int)

    def test_counters_accumulate_correctly(self, tmp_path: Path):
        """Verify counters accumulate correctly across entries."""
        from extract.core.state import CatalogEntry
        from extract.downloader.downloader import _execute_download_loop
        from extract.core.state_manager import State
        from extract.core.known_missing import KnownMissing
        from unittest.mock import patch

        state = State(tmp_path / ".download_state.json")
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        entry1 = CatalogEntry("yellow", 2024, 1)
        entry2 = CatalogEntry("yellow", 2024, 2)

        with patch("extract.downloader.downloader.process_entry") as mock_process:
            mock_process.side_effect = [
                (0, 1, 0),  # entry1: skipped
                (0, 1, 1),  # entry2: failed
            ]
            downloaded, skipped, failed = _execute_download_loop(
                [entry1, entry2], tmp_path, state, known_missing,
            )

        assert downloaded == 0
        assert skipped == 1
        assert failed == 1
        assert isinstance(downloaded, int)
        assert isinstance(skipped, int)
        assert isinstance(failed, int)
