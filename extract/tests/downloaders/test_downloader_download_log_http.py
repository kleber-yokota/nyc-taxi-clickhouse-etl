"""Tests for _log_http_error and _fetch_content in download module."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses as responses_lib

from extract.downloader.download import (
    _fetch_content,
    _log_http_error,
)
from extract.core.state import CatalogEntry, ErrorType
from extract.core.state_manager import State


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


class TestFetchContent:
    """Tests for _fetch_content function."""

    def test_writes_content_to_tmp_path(self, tmp_path: Path):
        url = "https://example.com/data.parquet"
        tmp_file = tmp_path / "data.parquet"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                url,
                body=b"test-content-12345",
                status=200,
            )
            _fetch_content(url, tmp_file)

        assert tmp_file.read_bytes() == b"test-content-12345"

    def test_raises_on_http_error(self):
        import requests

        url = "https://example.com/missing.parquet"
        tmp_file = Path("/tmp/nonexistent.parquet")

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                url,
                status=404,
                body=b"Not Found",
            )
            with pytest.raises(requests.HTTPError):
                _fetch_content(url, tmp_file)

    def test_writes_multiple_chunks(self, tmp_path: Path):
        url = "https://example.com/multi.parquet"
        tmp_file = tmp_path / "multi.parquet"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                url,
                body=b"chunk1" + b"x" * 8192 + b"chunk2",
                status=200,
            )
            _fetch_content(url, tmp_file)

        assert tmp_file.read_bytes() == b"chunk1" + b"x" * 8192 + b"chunk2"

    def test_503_error_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 503

        with patch.object(state, "log_error") as mock_log:
            _log_http_error(error, entry.url, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 503")
        known_missing.add.assert_not_called()

    def test_502_error_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 502

        with patch.object(state, "log_error") as mock_log:
            _log_http_error(error, entry.url, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 502")

    def test_403_error_records_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        import requests
        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 403

        with patch.object(state, "log_error") as mock_log:
            _log_http_error(error, entry.url, state, known_missing)

        mock_log.assert_called_once_with(entry.url, ErrorType.HTTP_ERROR, "HTTP 403")
