"""Tests for download_and_verify and handle_download_error in downloader_download module."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses as responses_lib

from extract.downloader.download import (
    download_and_verify,
    handle_download_error,
)
from extract.downloader.download import handle_download_error as _handle_download_error
from extract.core.state import CatalogEntry, ErrorType
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


class TestHandleDownloadError:
    """Tests for handle_download_error function."""

    def test_404_records_missing_file(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)
        mock_log.assert_called_once_with(entry.url, ErrorType.MISSING_FILE, "HTTP 404")

    def test_500_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_not_called()
        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 500")

    def test_network_error_records_network_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.ConnectionError("connection refused")

        _handle_download_error(error, entry, state, known_missing)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()

    def test_network_error_calls_log_error_with_correct_args(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.ConnectionError("connection refused")

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(error, entry, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.NETWORK_ERROR, "ConnectionError")

    def test_unknown_error_records_unknown(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(ValueError("unexpected"), entry, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.UNKNOWN, "unexpected")


class TestDownloadIntegration:
    """Integration tests for download_and_verify edge cases."""

    def test_handle_error_404_adds_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)
        mock_log.assert_called_once_with(entry.url, ErrorType.MISSING_FILE, "HTTP 404")

    def test_handle_error_403_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 403

        with patch.object(state, "log_error") as mock_log:
            _handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_not_called()
        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 403")

    def test_download_skip_with_existing_same_checksum(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"same-data")

        state = State(tmp_path / "state.json")
        known_missing = MagicMock()
        existing_checksum = hashlib.sha256(b"same-data").hexdigest()
        state.save(entry.url, existing_checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"same-data",
                status=200,
            )
            result = download_and_verify(entry, data_dir, state, known_missing)

        assert result == "skipped"
        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_download_404_with_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            result = download_and_verify(entry, data_dir, state, known_missing)

        assert result == "failed"
        known_missing.add.assert_called_once_with(entry.url)

    def test_download_404_without_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        state = State(tmp_path / "state.json")

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            result = download_and_verify(entry, data_dir, state, None)

        assert result == "failed"
        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()

    def test_unexpected_error_logs_url_not_none(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        with patch("extract.downloader.download._fetch_content", side_effect=ValueError("unexpected failure")):
            with patch.object(state, "log_error") as mock_log:
                with pytest.raises(ValueError, match="unexpected failure"):
                    download_and_verify(entry, data_dir, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.UNKNOWN, "Unexpected error: ValueError")
