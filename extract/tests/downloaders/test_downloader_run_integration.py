"""Integration test for run() with minimal mocks."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run


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
