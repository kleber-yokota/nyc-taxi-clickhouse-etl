"""Unit tests for downloader helper functions - error handling and fetch."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from extract.downloader.download import _fetch_content, _log_http_error
from extract.downloader.utils import handle_network_error as _handle_network_error
from extract.core.state import ErrorType
from extract.core.state_manager import State


class TestHandleHttpError:
    def test_404_records_missing_file(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        known_missing_path = tmp_path / "known_missing.txt"
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(known_missing_path)

        class MockResponse:
            status_code = 404

        import requests
        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        _log_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
        assert known_missing.is_missing("https://example.com/file.parquet")

    def test_500_records_http_error(self, tmp_path: Path):
        state = State(state_path=tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        class MockResponse:
            status_code = 500

        import requests
        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        _log_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()


class TestHandleNetworkError:
    def test_records_network_error(self, tmp_path: Path):
        state = State(state_path=tmp_path / "state.json")
        import requests
        network_error = requests.ConnectionError("connection refused")

        _handle_network_error(network_error, "https://example.com/file.parquet", state)

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()
        assert state.is_downloaded("https://example.com/file.parquet") is False

    def test_records_timeout_error(self, tmp_path: Path):
        state = State(state_path=tmp_path / "state.json")
        import requests
        network_error = requests.Timeout("request timed out")

        _handle_network_error(network_error, "https://example.com/file.parquet", state)

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()

    def test_records_timeout_logs_correct_type(self, tmp_path: Path):
        state = State(state_path=tmp_path / "state.json")
        import requests
        network_error = requests.Timeout("request timed out")

        with patch.object(state, "log_error") as mock_log:
            _handle_network_error(network_error, "https://example.com/file.parquet", state)

        mock_log.assert_called_once_with("https://example.com/file.parquet", ErrorType.NETWORK_ERROR, "Timeout")


class TestHandleHttpErrorKnownMissing:
    def test_404_calls_known_missing_add(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        class MockResponse:
            status_code = 404

        import requests
        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        _log_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
        assert known_missing.is_missing("https://example.com/file.parquet")

    def test_non_404_does_not_call_known_missing(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        class MockResponse:
            status_code = 500

        import requests
        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        _log_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
        assert not known_missing.is_missing("https://example.com/file.parquet")

    def test_handles_non_http_error(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        _log_http_error(ValueError("unexpected"), "https://example.com/file.parquet", state, known_missing)

        # Non-HTTP errors don't trigger any logging in handle_http_error
        # because the first check is isinstance(e, requests.HTTPError)
        error_log = state._errors_dir / "download_errors.log"
        assert not error_log.exists()


class TestFetchContent:
    @responses.activate
    def test_fetches_and_writes_content(self, tmp_path: Path):
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        content = b"fake_parquet_content_for_testing"
        responses.add(
            responses.GET,
            url,
            body=content,
            status=200,
            content_type="application/octet-stream",
        )

        tmp_path = tmp_path / "test.tmp"
        _fetch_content(url, tmp_path)

        assert tmp_path.exists()
        assert tmp_path.read_bytes() == content

    @responses.activate
    def test_fetch_content_raises_on_404(self, tmp_path: Path):
        import requests
        url = "https://example.com/missing.parquet"
        responses.add(
            responses.GET,
            url,
            status=404,
            body="",
        )

        tmp_file = tmp_path / "test.tmp"
        with pytest.raises(requests.HTTPError):
            _fetch_content(url, tmp_file)
