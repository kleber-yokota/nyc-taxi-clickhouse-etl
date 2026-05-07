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
    """Tests that verify KeyboardInterrupt propagates from run()."""

    def test_keyboard_interrupt_propagates(self, tmp_path: Path):
        """Verify KeyboardInterrupt propagates when process_entry raises it."""
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", side_effect=KeyboardInterrupt()):
                with pytest.raises(KeyboardInterrupt):
                    run(data_dir=tmp_path, types=["yellow"])
