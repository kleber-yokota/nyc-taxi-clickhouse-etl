"""Tests for IO-related downloader functions (HTTP errors, network errors, file ops)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses

from extract.downloader.downloader import (
    _backup_existing_file,
    _fetch_content,
    _handle_http_error,
    _handle_network_error,
    _safe_unlink,
)
from extract.core.state import CatalogEntry, ErrorType


class TestHandleHttpError:
    @pytest.fixture
    def mock_404_error(self):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404
        return error

    @pytest.fixture
    def mock_500_error(self):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500
        return error

    def test_404_calls_log_error_with_url(self, mock_404_error):
        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/missing.parquet"

        _handle_http_error(mock_404_error, url, state, known_missing)

        state.log_error.assert_called_once_with(
            url, ErrorType.MISSING_FILE, "HTTP 404"
        )

    def test_404_adds_to_known_missing(self, mock_404_error):
        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/missing.parquet"

        _handle_http_error(mock_404_error, url, state, known_missing)

        known_missing.add.assert_called_once_with(url)

    def test_500_calls_log_error_with_url(self, mock_500_error):
        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/error.parquet"

        _handle_http_error(mock_500_error, url, state, known_missing)

        state.log_error.assert_called_once_with(
            url, ErrorType.HTTP_ERROR, "HTTP 500"
        )

    def test_500_no_known_missing(self, mock_500_error):
        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/error.parquet"

        _handle_http_error(mock_500_error, url, state, known_missing)

        known_missing.add.assert_not_called()

    @pytest.mark.parametrize("status_code", [400, 401, 403, 502, 503])
    def test_non_404_status(self, status_code: int):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = status_code

        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/test.parquet"

        _handle_http_error(error, url, state, known_missing)

        state.log_error.assert_called_once_with(
            url, ErrorType.HTTP_ERROR, f"HTTP {status_code}"
        )
        known_missing.add.assert_not_called()


class TestHandleNetworkError:
    def test_calls_log_error_with_url(self):
        import requests

        error = requests.RequestException("connection refused")

        state = MagicMock()
        url = "https://example.com/test.parquet"

        _handle_network_error(error, url, state)

        state.log_error.assert_called_once_with(
            url, ErrorType.NETWORK_ERROR, "RequestException"
        )

    def test_error_type_includes_class_name(self):
        import requests

        class CustomNetworkError(requests.RequestException):
            pass

        error = CustomNetworkError("custom error")

        state = MagicMock()
        url = "https://example.com/test.parquet"

        _handle_network_error(error, url, state)

        args = state.log_error.call_args
        assert "CustomNetworkError" in args[0][2]


