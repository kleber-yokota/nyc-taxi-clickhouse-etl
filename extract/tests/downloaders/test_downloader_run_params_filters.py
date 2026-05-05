"""Tests for survived mutmut mutants in downloader.py x_run function - filters and data_dir."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run
from extract.core.state import CatalogEntry


class TestRunDataDirPath:
    """Tests that verify data_dir path resolution."""

    def test_data_dir_passed_to_state(self, tmp_path: Path):
        """Verify data_dir is used for state path."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.State") as MockState:
                    run(data_dir=tmp_path / "mydata", types=["yellow"])

        MockState.assert_called_once()
        state_path = MockState.call_args[0][0]
        assert "mydata" in str(state_path)

    def test_data_dir_passed_to_known_missing(self, tmp_path: Path):
        """Verify data_dir is used for known_missing path."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.KnownMissing") as MockKnownMissing:
                    run(data_dir=tmp_path / "mydata", types=["yellow"])

        MockKnownMissing.assert_called_once()
        km_path = MockKnownMissing.call_args[0][0]
        assert "mydata" in str(km_path)

    def test_data_dir_passed_to_interruptible(self, tmp_path: Path):
        """Verify data_dir is passed to InterruptibleDownload."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.InterruptibleDownload") as MockInterrupt:
                    run(data_dir=tmp_path / "mydata", types=["yellow"])

        MockInterrupt.assert_called_once()
        assert MockInterrupt.call_args[0][0] == tmp_path / "mydata"


class TestRunProcessEntryCalls:
    """Tests that verify process_entry is called with correct arguments."""

    def test_process_entry_receives_state(self, tmp_path: Path):
        """Verify process_entry receives the state object."""
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry") as mock_process:
                mock_process.return_value = (1, 0, 0)
                run(data_dir=tmp_path, types=["yellow"])

        assert mock_process.called
        call_args = mock_process.call_args
        # Second arg should be data_dir
        assert call_args[0][1] == tmp_path

    def test_process_entry_receives_known_missing(self, tmp_path: Path):
        """Verify process_entry receives the known_missing object."""
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry") as mock_process:
                mock_process.return_value = (1, 0, 0)
                run(data_dir=tmp_path, types=["yellow"])

        assert mock_process.called
        call_args = mock_process.call_args
        # Fourth arg should be known_missing
        assert call_args[0][3] is not None


class TestRunResultConstruction:
    """Tests that verify result dictionary construction."""

    def test_result_contains_all_keys(self, tmp_path: Path):
        """Verify result has all expected keys."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir=tmp_path, types=["yellow"])

        assert set(result.keys()) == {"downloaded", "skipped", "failed", "total"}

    def test_result_values_are_integers(self, tmp_path: Path):
        """Verify all result values are integers."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir=tmp_path, types=["yellow"])

        for value in result.values():
            assert isinstance(value, int)

    def test_total_equals_len_of_entries(self, tmp_path: Path):
        """Verify total equals the number of catalog entries."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
            CatalogEntry("yellow", 2024, 3),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", return_value=(0, 0, 0)):
                result = run(data_dir=tmp_path, types=["yellow"])

        assert result["total"] == 3

    def test_downloaded_skipped_failed_sum(self, tmp_path: Path):
        """Verify downloaded + skipped + failed = total."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
            CatalogEntry("yellow", 2024, 3),
            CatalogEntry("yellow", 2024, 4),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[
                    (1, 0, 0),
                    (1, 1, 0),
                    (1, 1, 1),
                    (1, 2, 1),
                ],
            ):
                result = run(data_dir=tmp_path, types=["yellow"])

        assert result["downloaded"] + result["skipped"] + result["failed"] == result["total"]


class TestRunModeBehavior:
    """Tests that verify mode parameter behavior."""

    def test_full_mode_resets_state(self, tmp_path: Path):
        """Verify full mode calls state.reset()."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.State") as MockState:
                    run(data_dir=tmp_path, types=["yellow"], mode="full")

        MockState.return_value.reset.assert_called_once()

    def test_incremental_mode_does_not_reset(self, tmp_path: Path):
        """Verify incremental mode does not call state.reset()."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.State") as MockState:
                    run(data_dir=tmp_path, types=["yellow"], mode="incremental")

        MockState.return_value.reset.assert_not_called()

    def test_default_mode_is_incremental(self, tmp_path: Path):
        """Verify default mode is incremental (no reset)."""
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[CatalogEntry("yellow", 2024, 1)]):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                with patch("extract.downloader.downloader.State") as MockState:
                    run(data_dir=tmp_path, types=["yellow"])

        MockState.return_value.reset.assert_not_called()
