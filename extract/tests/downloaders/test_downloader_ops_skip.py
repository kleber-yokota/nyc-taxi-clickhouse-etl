"""Tests for should_skip_download exercising all branches."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from extract.downloader.downloader_ops import should_skip_download
from extract.core.state import CatalogEntry
from extract.core.state_manager import State


class TestShouldSkipDownloadKnownMissing:
    """Tests for should_skip_download with known missing URLs."""

    def test_returns_true_when_known_missing(self, tmp_path: Path):
        """Verify returns True when URL is in known_missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        known_missing.add(entry.url)

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True

    def test_logs_skip_message(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Verify logs the skip message when known missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")
        known_missing.add(entry.url)

        with caplog.at_level(logging.INFO, logger="extract.downloader.downloader_ops"):
            should_skip_download(entry, state, known_missing, tmp_path)

        assert any("Skipping known missing" in r.message for r in caplog.records)


class TestShouldSkipDownloadState:
    """Tests for should_skip_download with state-based skipping."""

    def test_returns_true_when_downloaded_and_exists(self, tmp_path: Path):
        """Verify returns True when downloaded and file exists."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry.filename).write_bytes(b"existing")

        state.save(entry.url, "some-checksum")

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True

    def test_returns_false_when_downloaded_but_missing(self, tmp_path: Path):
        """Verify returns False when downloaded but file is missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        state.save(entry.url, "some-checksum")

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False

    def test_saves_empty_checksum_when_file_missing(self, tmp_path: Path):
        """Verify saves empty checksum when file was previously downloaded but is now missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        state.save(entry.url, "old-checksum")

        should_skip_download(entry, state, known_missing, tmp_path)

        assert state.is_downloaded(entry.url)
        assert state.get_checksum(entry.url) == ""

    def test_returns_false_when_not_downloaded(self, tmp_path: Path):
        """Verify returns False when URL not in state."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is False


class TestShouldSkipDownloadCombinations:
    """Tests for should_skip_download with various combinations."""

    def test_known_missing_takes_precedence(self, tmp_path: Path):
        """Verify known_missing check happens before state check."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry.filename).write_bytes(b"existing")

        state.save(entry.url, "checksum")
        known_missing.add(entry.url)

        result = should_skip_download(entry, state, known_missing, tmp_path)

        assert result is True

    def test_different_data_types_independent(self, tmp_path: Path):
        """Verify different data types are tracked independently."""
        entry_yellow = CatalogEntry("yellow", 2024, 1)
        entry_green = CatalogEntry("green", 2024, 1)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        target_dir = tmp_path / entry_yellow.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry_yellow.filename).write_bytes(b"data")

        state.save(entry_yellow.url, "checksum")

        result_yellow = should_skip_download(entry_yellow, state, known_missing, tmp_path)
        result_green = should_skip_download(entry_green, state, known_missing, tmp_path)

        assert result_yellow is True
        assert result_green is False

    def test_different_months_independent(self, tmp_path: Path):
        """Verify different months are tracked independently."""
        entry_jan = CatalogEntry("yellow", 2024, 1)
        entry_feb = CatalogEntry("yellow", 2024, 2)
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        target_dir = tmp_path / entry_jan.target_dir
        target_dir.mkdir(parents=True)
        (target_dir / entry_jan.filename).write_bytes(b"data")

        state.save(entry_jan.url, "checksum")

        result_jan = should_skip_download(entry_jan, state, known_missing, tmp_path)
        result_feb = should_skip_download(entry_feb, state, known_missing, tmp_path)

        assert result_jan is True
        assert result_feb is False
