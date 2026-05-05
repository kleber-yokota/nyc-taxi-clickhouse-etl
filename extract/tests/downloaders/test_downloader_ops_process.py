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
