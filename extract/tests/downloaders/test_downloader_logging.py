"""Tests for _log_http_error in download module."""

from __future__ import annotations

from unittest.mock import MagicMock

from extract.core.state import ErrorType


class TestLogHttpError:
    """Tests that _log_http_error routes HTTP errors correctly."""

    def test_404_logs_missing_and_adds_to_known(self):
        import requests

        from extract.downloader.download import _log_http_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        state = MagicMock()
        known_missing = MagicMock()

        _log_http_error(error, "https://example.com/test.parquet", state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.MISSING_FILE
        known_missing.add.assert_called_once()

    def test_404_with_none_known_missing(self):
        import requests

        from extract.downloader.download import _log_http_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        state = MagicMock()

        _log_http_error(error, "https://example.com/test.parquet", state, None)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.MISSING_FILE

    def test_500_logs_http_error_no_known_missing_add(self):
        import requests

        from extract.downloader.download import _log_http_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        state = MagicMock()
        known_missing = MagicMock()

        _log_http_error(error, "https://example.com/test.parquet", state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR
        known_missing.add.assert_not_called()

    def test_401_status_logs_http_error(self):
        import requests

        from extract.downloader.download import _log_http_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 401

        state = MagicMock()

        _log_http_error(error, "https://example.com/test.parquet", state, None)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR

    def test_503_status_logs_http_error(self):
        import requests

        from extract.downloader.download import _log_http_error

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 503

        state = MagicMock()
        known_missing = MagicMock()

        _log_http_error(error, "https://example.com/test.parquet", state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.HTTP_ERROR
