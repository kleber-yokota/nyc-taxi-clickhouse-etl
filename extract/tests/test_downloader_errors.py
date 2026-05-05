"""Tests for handle_download_error in downloader_download module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from extract.core.state import CatalogEntry, ErrorType


class TestHandleDownloadError:
    """Tests that handle_download_error routes errors to correct ErrorType."""

    def test_404_calls_log_error_with_missing_file(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.MISSING_FILE
        assert "HTTP 404" in args[0][2]

    def test_404_adds_url_to_known_missing(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)

    def test_500_calls_log_error_with_http_error(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR

    def test_500_does_not_add_to_known_missing(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        known_missing.add.assert_not_called()

    def test_network_error_sets_network_error_type(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.ConnectionError("connection refused")

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.NETWORK_ERROR

    def test_generic_exception_sets_unknown_type(self):
        from extract.core.downloader_download import handle_download_error

        error = ValueError("disk full")

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.UNKNOWN

    def test_400_status_sets_http_error(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 400

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR
        assert "HTTP 400" in args[0][2]

    def test_502_status_sets_http_error(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 502

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR

    def test_timeout_sets_network_error(self):
        import requests

        from extract.core.downloader_download import handle_download_error

        error = requests.Timeout("request timed out")

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        handle_download_error(error, entry, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.NETWORK_ERROR
