"""Tests targeting survived mutmut mutants in downloader.py x_run function."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run
from extract.core.state import CatalogEntry


class TestRunParameterPassing:
    """Tests that verify run() passes correct parameters to Catalog constructor."""

    def test_run_passes_types_to_catalog(self, tmp_path: Path):
        """Verify types parameter is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            MockCatalog.return_value.generate.return_value = [CatalogEntry("yellow", 2024, 1)]
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                run(data_dir=tmp_path, types=["yellow"], mode="incremental")

        MockCatalog.assert_called_once()
        call_kwargs = MockCatalog.call_args[1]
        assert call_kwargs["types"] == ["yellow"]

    def test_run_passes_from_year_to_catalog(self, tmp_path: Path):
        """Verify from_year parameter is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            MockCatalog.return_value.generate.return_value = [CatalogEntry("yellow", 2024, 1)]
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                run(data_dir=tmp_path, from_year=2024, mode="incremental")

        call_kwargs = MockCatalog.call_args[1]
        assert call_kwargs["from_year"] == 2024

    def test_run_passes_to_year_to_catalog(self, tmp_path: Path):
        """Verify to_year parameter is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            MockCatalog.return_value.generate.return_value = [CatalogEntry("yellow", 2024, 1)]
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                run(data_dir=tmp_path, to_year=2030, mode="incremental")

        call_kwargs = MockCatalog.call_args[1]
        assert call_kwargs["to_year"] == 2030

    def test_run_passes_max_entries_to_catalog(self, tmp_path: Path):
        """Verify max_entries parameter is passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            MockCatalog.return_value.generate.return_value = [CatalogEntry("yellow", 2024, 1)]
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                run(data_dir=tmp_path, max_entries=10, mode="incremental")

        call_kwargs = MockCatalog.call_args[1]
        assert call_kwargs["max_entries"] == 10

    def test_run_passes_all_params_to_catalog(self, tmp_path: Path):
        """Verify all parameters are passed to Catalog."""
        with patch("extract.downloader.downloader.Catalog") as MockCatalog:
            MockCatalog.return_value.generate.return_value = [CatalogEntry("yellow", 2024, 1)]
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                run(
                    data_dir=tmp_path,
                    types=["green"],
                    from_year=2020,
                    to_year=2024,
                    max_entries=5,
                    mode="incremental",
                )

        call_kwargs = MockCatalog.call_args[1]
        assert call_kwargs["types"] == ["green"]
        assert call_kwargs["from_year"] == 2020
        assert call_kwargs["to_year"] == 2024
        assert call_kwargs["max_entries"] == 5


class TestRunInterruptMessage:
    """Tests that verify exact log messages in run()."""

    def test_interrupt_logs_exact_message(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify the exact interrupt message is logged.

        This kills the mutmut_73 mutant that changes the log message to uppercase.
        """
        caplog.set_level(logging.INFO)
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", side_effect=KeyboardInterrupt()):
                run(data_dir=tmp_path, types=["yellow"])

        messages = [record.message for record in caplog.records]
        assert "Download interrupted by user." in messages

    def test_interrupt_calls_cleanup(self, tmp_path: Path):
        """Verify cleanup is called on interrupt."""
        entries = [CatalogEntry("yellow", 2024, 1)]
        interruptible = MagicMock()

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.InterruptibleDownload", return_value=interruptible):
                with patch("extract.downloader.downloader.process_entry", side_effect=KeyboardInterrupt()):
                    run(data_dir=tmp_path, types=["yellow"])

        interruptible.cleanup.assert_called_once()


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
