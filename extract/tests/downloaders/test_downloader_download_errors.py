"""Tests for handle_download_error and _log_http_error in downloader_download module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses as responses_lib

from extract.downloader.download import (
    handle_download_error as _handle_download_error,
    _log_http_error,
)
from extract.core.state import CatalogEntry, ErrorType
from extract.core.state_manager import State


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


class TestLogHttpError:
    """Tests for _log_http_error function."""

    def test_404_records_missing_and_adds_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with patch.object(state, "log_error") as mock_log:
            _log_http_error(error, entry.url, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)
        mock_log.assert_called_once_with(entry.url, ErrorType.MISSING_FILE, "HTTP 404")

    def test_404_with_none_known_missing(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        _log_http_error(error, entry.url, state, None)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()

    def test_non_404_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 503

        with patch.object(state, "log_error") as mock_log:
            _log_http_error(error, entry.url, state, known_missing)

        known_missing.add.assert_not_called()
        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 503")
