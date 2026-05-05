"""Tests for run function in downloader module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from extract.downloader.downloader import run
from extract.core.state import CatalogEntry


class TestRun:
    def test_returns_zero_when_no_entries(self):
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir="/tmp/test_etl", types=["yellow"], max_entries=0)

        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_empty_catalog_returns_zero_result(self, tmp_path: Path):
        with patch("extract.downloader.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir=tmp_path, types=["yellow"])
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_with_max_entries_limit(self, tmp_path: Path):
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
            CatalogEntry("yellow", 2024, 3),
        ]
        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries[:2]):
            with patch(
                "extract.downloader.downloader_ops.download_and_verify",
                side_effect=["downloaded", "downloaded"],
            ):
                result = run(data_dir=tmp_path, types=["yellow"], from_year=2024, to_year=2024, max_entries=2)
        assert result["total"] == 2
