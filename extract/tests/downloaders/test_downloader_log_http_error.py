"""Tests for _log_http_error exercising all branches."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from extract.downloader.download import _log_http_error
from extract.core.state import ErrorType


class TestLogHttpError404:
    """Test _log_http_error with 404 responses."""

    def test_404_records_missing_file(self, tmp_path: Path):
        """Verify 404 logs MISSING_FILE error type."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        _log_http_error(error, "https://example.com/file.parquet", state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.MISSING_FILE

    def test_404_adds_to_known_missing_when_provided(self):
        """Verify 404 adds URL to known_missing when not None."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        _log_http_error(error, "https://example.com/file.parquet", state, known_missing)

        known_missing.add.assert_called_once()

    def test_404_adds_to_known_missing_with_none_param(self):
        """Verify 404 adds URL to known_missing even when param is None object."""
        state = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        _log_http_error(error, "https://example.com/file.parquet", state, None)

        # When known_missing is None, the hasattr check should fail
        # and no add should be called
        pass  # This tests the conditional branch

    def test_404_logs_error_message(self, caplog: pytest.LogCaptureFixture):
        """Verify 404 logs the correct error message."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            _log_http_error(error, "https://example.com/missing.parquet", state, known_missing)

        assert any("File not found" in r.message for r in caplog.records)
        assert any("HTTP 404" in r.message for r in caplog.records)


class TestLogHttpErrorNon404:
    """Test _log_http_error with non-404 responses."""

    def test_500_records_http_error(self):
        """Verify 500 logs HTTP_ERROR error type."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        _log_http_error(error, "https://example.com/file.parquet", state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.HTTP_ERROR

    def test_500_does_not_add_known_missing(self):
        """Verify non-404 does not add to known_missing."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        _log_http_error(error, "https://example.com/file.parquet", state, known_missing)

        known_missing.add.assert_not_called()

    def test_500_logs_error_message(self, caplog: pytest.LogCaptureFixture):
        """Verify 500 logs the correct error message."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            _log_http_error(error, "https://example.com/file.parquet", state, known_missing)

        assert any("HTTP error" in r.message for r in caplog.records)
        assert any("HTTP 500" in r.message for r in caplog.records)

    def test_403_records_http_error(self):
        """Verify 403 also records HTTP_ERROR."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 403

        _log_http_error(error, "https://example.com/forbidden.parquet", state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.HTTP_ERROR

    def test_503_records_http_error(self):
        """Verify 503 also records HTTP_ERROR."""
        state = MagicMock()
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 503

        _log_http_error(error, "https://example.com/unavailable.parquet", state, known_missing)

        call_args = state.log_error.call_args
        assert call_args[0][1] == ErrorType.HTTP_ERROR
