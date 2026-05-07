"""Tests for download_and_verify in download module."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import responses as responses_lib

from extract.downloader.download import download_and_verify
from extract.core.state import CatalogEntry
from extract.core.state_manager import State


class TestDownloadAndVerify:
    """Tests for download_and_verify using real HTTP mocking."""

    @pytest.fixture
    def mock_state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def mock_known_missing(self) -> MagicMock:
        return MagicMock()

    def test_downloaded_successfully(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"parquet-data-here",
                status=200,
            )
            result = download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert result == "downloaded"
        assert (data_dir / entry.target_dir / entry.filename).exists()
        checksum = hashlib.sha256(b"parquet-data-here").hexdigest()
        assert mock_state.checksums[entry.url] == checksum

    def test_skipped_when_checksum_matches(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"existing-data")

        existing_checksum = hashlib.sha256(b"existing-data").hexdigest()
        mock_state.save(entry.url, existing_checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"existing-data",
                status=200,
            )
            result = download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert result == "skipped"

    def test_failed_on_404(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            result = download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert result == "failed"
        state_errors = mock_state._errors_dir / "download_errors.log"
        assert state_errors.exists()

    def test_failed_on_500(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Internal Server Error",
            )
            result = download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert result == "failed"

    def test_network_error_logs_correct_message(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock, caplog: pytest.LogCaptureFixture):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        import requests

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert any("Network error for" in record.message for record in caplog.records)

    def test_tmp_file_cleaned_on_failure(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Error",
            )
            download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_creates_nested_target_dir(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"data",
                status=200,
            )
            download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert (data_dir / entry.target_dir).exists()

    def test_backup_existing_on_checksum_mismatch(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"old-data")

        old_checksum = hashlib.sha256(b"old-data").hexdigest()
        mock_state.save(entry.url, old_checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"new-data",
                status=200,
            )
            result = download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        assert result == "downloaded"
        assert (data_dir / entry.target_dir / entry.filename).read_bytes() == b"new-data"
