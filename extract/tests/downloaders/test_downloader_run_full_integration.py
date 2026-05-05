"""Tests for run function in downloader module - integration tests."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run
from extract.core.state import CatalogEntry


class TestRunFunctionIntegration:
    """Integration tests for run function."""

    def test_run_with_skipped_entries(self, tmp_path: Path):
        """Test run counts skipped entries correctly."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[
                    (0, 1, 0),
                    (0, 2, 0),
                ],
            ):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    mode="incremental",
                )

        assert result["skipped"] == 2
        assert result["total"] == 2

    def test_run_mixed_results(self, tmp_path: Path):
        """Test run with mixed downloaded, skipped, and failed."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
            CatalogEntry("yellow", 2024, 3),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[
                    (1, 0, 0),
                    (1, 1, 0),
                    (1, 1, 1),
                ],
            ):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    mode="incremental",
                )

        assert result["downloaded"] == 1
        assert result["skipped"] == 1
        assert result["failed"] == 1
        assert result["total"] == 3

    def test_run_with_string_types(self, tmp_path: Path):
        """Test run accepts string types parameter."""
        entries = [CatalogEntry("green", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                result = run(
                    data_dir=tmp_path,
                    types=["green"],
                    mode="incremental",
                )

        assert result["downloaded"] == 1

    def test_run_with_none_data_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Test run with None data_dir uses default 'data' directory."""
        monkeypatch.chdir(tmp_path)
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                result = run(types=["yellow"], mode="incremental")

        assert result["downloaded"] == 1

    def test_run_logs_warning_when_no_entries(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Test run logs warning when catalog has no entries."""
        caplog.set_level(logging.WARNING)

        with patch("extract.downloader.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir=tmp_path, types=["yellow"])

        assert result["total"] == 0
        assert any("No entries to download" in record.message for record in caplog.records)
