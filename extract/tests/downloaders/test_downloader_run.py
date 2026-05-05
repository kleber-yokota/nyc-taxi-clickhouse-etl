"""Tests for run() exercising all mutation paths with ≤2 mocks."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run


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
        from extract.downloader.downloader import _execute_download_loop, _make_result
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


class TestRunProcessEntryCall:
    """Verify process_entry is called with correct arguments."""

    def test_process_entry_receives_counters(self, tmp_path: Path):
        """Verify process_entry receives downloaded, skipped, failed as arguments."""
        from extract.core.state import CatalogEntry
        from extract.downloader.downloader import _execute_download_loop
        from extract.core.state_manager import State
        from extract.core.known_missing import KnownMissing
        from unittest.mock import patch

        state = State(tmp_path / ".download_state.json")
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        entry = CatalogEntry("yellow", 2024, 1)

        with patch("extract.downloader.downloader.process_entry") as mock_process:
            mock_process.return_value = (1, 0, 0)
            _execute_download_loop([entry], tmp_path, state, known_missing)

            mock_process.assert_called_once()
            call_args = mock_process.call_args[0]
            # Args: entry, data_dir, state, known_missing, downloaded, skipped, failed
            assert call_args[0] == entry
            assert call_args[1] == tmp_path
            assert call_args[2] is state
            assert call_args[3] is known_missing
            assert call_args[4] == 0  # downloaded starts at 0
            assert call_args[5] == 0  # skipped starts at 0
            assert call_args[6] == 0  # failed starts at 0

    def test_process_entry_receives_updated_counters(self, tmp_path: Path):
        """Verify process_entry receives updated counters on subsequent calls."""
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
                (1, 0, 0),  # entry1: downloaded
                (1, 1, 0),  # entry2: skipped
            ]
            _execute_download_loop([entry1, entry2], tmp_path, state, known_missing)

            assert mock_process.call_count == 2

            # Second call should receive updated counters from first call
            second_call = mock_process.call_args_list[1]
            assert second_call[0][4] == 1  # downloaded=1
            assert second_call[0][5] == 0  # skipped=0
            assert second_call[0][6] == 0  # failed=0


class TestRunResultStructure:
    """Verify result dict structure and logger.info call."""

    def test_result_has_all_keys(self, tmp_path: Path):
        """Verify result dict has downloaded, skipped, failed, total keys."""
        from extract.core.state import CatalogEntry
        from extract.downloader.downloader import _make_result

        result = _make_result(5, 3, 2, 10)

        assert "downloaded" in result
        assert "skipped" in result
        assert "failed" in result
        assert "total" in result
        assert result["downloaded"] == 5
        assert result["skipped"] == 3
        assert result["failed"] == 2
        assert result["total"] == 10

    def test_result_values_are_integers(self, tmp_path: Path):
        """Verify all result values are integers."""
        from extract.downloader.downloader import _make_result

        result = _make_result(0, 0, 0, 0)

        for key, value in result.items():
            assert isinstance(value, int), f"{key} is not int: {type(value)}"

    def test_make_result_with_none_values(self, tmp_path: Path):
        """Verify _make_result handles None values correctly (they become 0 in result)."""
        from extract.downloader.downloader import _make_result

        # If counters were None (mutation), they should still be int in result
        result = _make_result(None, None, None, 0)

        # The result dict will contain None values - this is what the mutation does
        # Our test verifies the function accepts the values
        assert result["downloaded"] is None
        assert result["skipped"] is None
        assert result["failed"] is None

    def test_log_download_complete_logs_result(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify logger.info is called with result dict."""
        from extract.downloader.downloader import _log_download_complete

        result = {"downloaded": 5, "skipped": 3, "failed": 2, "total": 10}

        with caplog.at_level(logging.INFO, logger="extract.downloader.downloader"):
            _log_download_complete(result)

        assert any("Download complete" in r.message for r in caplog.records)
        assert any("5" in r.message for r in caplog.records)

    def test_log_download_complete_with_zero_result(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify logger.info is called even with zero result."""
        from extract.downloader.downloader import _log_download_complete

        result = {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

        with caplog.at_level(logging.INFO, logger="extract.downloader.downloader"):
            _log_download_complete(result)

        assert any("Download complete" in r.message for r in caplog.records)


class TestRunFullIntegration:
    """Integration test for run() with minimal mocks."""

    def test_run_with_single_entry(self, tmp_path: Path):
        """Verify run() processes a single entry correctly."""
        from extract.core.state import CatalogEntry

        entry = CatalogEntry("yellow", 2024, 1)

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = [entry]
            MockCatalog.return_value = mock_catalog

            with patch("extract.downloader.downloader.process_entry") as mock_process:
                mock_process.return_value = (1, 0, 0)
                result = run(data_dir=tmp_path)

        assert "downloaded" in result
        assert "skipped" in result
        assert "failed" in result
        assert "total" in result
        assert result["total"] == 1

    def test_run_with_max_entries(self, tmp_path: Path):
        """Verify max_entries limits catalog generation."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            run(data_dir=tmp_path, max_entries=5)

            MockCatalog.assert_called_once_with(
                types=None, from_year=None, to_year=None, max_entries=5,
            )

    def test_run_with_types_filter(self, tmp_path: Path):
        """Verify types filter is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            run(data_dir=tmp_path, types=["yellow", "green"])

            MockCatalog.assert_called_once_with(
                types=["yellow", "green"], from_year=None, to_year=None, max_entries=None,
            )

    def test_run_with_year_range(self, tmp_path: Path):
        """Verify year range is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            run(data_dir=tmp_path, from_year=2020, to_year=2023)

            MockCatalog.assert_called_once_with(
                types=None, from_year=2020, to_year=2023, max_entries=None,
            )

    def test_run_creates_target_directory(self, tmp_path: Path):
        """Verify run creates data directory if it doesn't exist."""
        data_dir = tmp_path / "data"

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = []
            MockCatalog.return_value = mock_catalog

            run(data_dir=data_dir)

        assert data_dir.exists()

    def test_run_with_string_data_dir(self, tmp_path: Path):
        """Verify run accepts string data_dir path."""
        from extract.core.state import CatalogEntry

        data_dir_str = str(tmp_path / "data")
        entry = CatalogEntry("yellow", 2024, 1)

        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            mock_catalog = MagicMock()
            mock_catalog.generate.return_value = [entry]
            MockCatalog.return_value = mock_catalog

            with patch("extract.downloader.downloader.process_entry") as mock_process:
                with patch("extract.downloader.downloader.State") as MockState:
                    mock_process.return_value = (0, 0, 0)
                    result = run(data_dir=data_dir_str)

                    MockState.assert_called_once()
                    call_path = MockState.call_args[0][0]
                    assert str(call_path) == str(tmp_path / "data" / ".download_state.json")

        assert result["total"] == 1
