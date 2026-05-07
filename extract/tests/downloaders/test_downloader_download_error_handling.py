"""Tests for handle_download_error exercising all isinstance branches."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from extract.downloader.download import handle_download_error
from extract.core.state import CatalogEntry, ErrorType


class TestHandleDownloadError404:
    """Test handle_download_error with HTTP 404 responses."""

    def test_404_records_missing_file_error(self, tmp_path: Path):
        """Verify 404 logs MISSING_FILE error type."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.MISSING_FILE

    def test_404_adds_to_known_missing(self, tmp_path: Path):
        """Verify 404 adds URL to known_missing tracker."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)

    def test_404_logs_error_message(self, caplog: pytest.LogCaptureFixture):
        """Verify 404 logs the correct error message."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        assert any("File not found" in r.message for r in caplog.records)
        assert any("HTTP 404" in r.message for r in caplog.records)


class TestHandleDownloadErrorNon404:
    """Test handle_download_error with non-404 HTTP errors."""

    def test_500_records_http_error(self):
        """Verify 500 logs HTTP_ERROR error type."""
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.HTTP_ERROR

    def test_500_does_not_add_known_missing(self):
        """Verify non-404 does not add to known_missing."""
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_not_called()

    def test_500_logs_error_message(self, caplog: pytest.LogCaptureFixture):
        """Verify 500 logs the correct error message."""
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        assert any("HTTP error" in r.message for r in caplog.records)
        assert any("HTTP 500" in r.message for r in caplog.records)

    def test_502_records_http_error(self):
        """Verify 502 also records HTTP_ERROR."""
        entry = CatalogEntry("yellow", 2024, 3)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 502

        handle_download_error(error, entry, state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.HTTP_ERROR


class TestHandleDownloadErrorNetwork:
    """Test handle_download_error with network errors."""

    def test_request_exception_logs_network_error(self):
        """Verify RequestException logs NETWORK_ERROR."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.RequestException("connection refused")

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.NETWORK_ERROR

    def test_connection_error_logs_network_error(self):
        """Verify ConnectionError logs NETWORK_ERROR."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.ConnectionError("timeout")

        handle_download_error(error, entry, state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.NETWORK_ERROR

    def test_request_exception_does_not_add_known_missing(self):
        """Verify network errors do not add to known_missing."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.RequestException("timeout")

        handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_not_called()

    def test_request_exception_logs_error_message(self, caplog: pytest.LogCaptureFixture):
        """Verify network error logs the correct message."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.RequestException("connection refused")

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        assert any("Network error" in r.message for r in caplog.records)
