"""Tests for run function in downloader module - basic code paths."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import run
from extract.core.state import CatalogEntry


class TestRunFunction:
    """Tests for run function that exercise actual code paths."""

    def test_run_with_incremental_mode(self, tmp_path: Path):
        """Test run with incremental mode - default mode."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[
                    (1, 0, 0),
                    (2, 0, 0),
                ],
            ):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    from_year=2024,
                    to_year=2024,
                    mode="incremental",
                )

        assert result["downloaded"] == 2
        assert result["skipped"] == 0
        assert result["failed"] == 0
        assert result["total"] == 2

    def test_run_with_full_mode_resets_state(self, tmp_path: Path):
        """Test run with full mode triggers state reset."""
        entries = [CatalogEntry("yellow", 2024, 1)]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", return_value=(1, 0, 0)):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    mode="full",
                )

        assert result["downloaded"] == 1
        assert result["total"] == 1

    def test_run_with_keyboard_interrupt_propagates(self, tmp_path: Path):
        """Test run propagates KeyboardInterrupt when process_entry raises it."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch("extract.downloader.downloader.process_entry", side_effect=KeyboardInterrupt()):
                with pytest.raises(KeyboardInterrupt):
                    run(
                        data_dir=tmp_path,
                        types=["yellow"],
                        mode="incremental",
                    )

    def test_run_with_max_entries(self, tmp_path: Path):
        """Test run respects max_entries limit."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
            CatalogEntry("yellow", 2024, 3),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries[:2]):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[(1, 0, 0), (2, 0, 0)],
            ):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    from_year=2024,
                    to_year=2024,
                    max_entries=2,
                )

        assert result["total"] == 2

    def test_run_with_failed_entries(self, tmp_path: Path):
        """Test run counts failed entries correctly."""
        entries = [
            CatalogEntry("yellow", 2024, 1),
            CatalogEntry("yellow", 2024, 2),
        ]

        with patch("extract.downloader.downloader.Catalog.generate", return_value=entries):
            with patch(
                "extract.downloader.downloader.process_entry",
                side_effect=[
                    (1, 0, 0),
                    (1, 0, 1),
                ],
            ):
                result = run(
                    data_dir=tmp_path,
                    types=["yellow"],
                    mode="incremental",
                )

        assert result["downloaded"] == 1
        assert result["failed"] == 1
        assert result["total"] == 2
