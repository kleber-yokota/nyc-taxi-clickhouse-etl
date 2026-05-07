"""Tests targeting mutmut survivors in download module.

Kills survived mutations in:
- download_and_verify (tmp_path, state.log_error args, logger.error args)
- handle_download_error (logger.error args)
- _log_http_error (logger.error args)
- _fetch_content (requests.get args)
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses as responses_lib

from extract.downloader.download import (
    _fetch_content,
    _log_http_error,
    download_and_verify,
    handle_download_error,
)
from extract.core.state import CatalogEntry, ErrorType
from extract.core.state_manager import State


class TestDownloadAndVerifyTmpPath:
    """Tests to kill string mutations in tmp_path construction."""

    @pytest.fixture
    def mock_state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def mock_known_missing(self) -> MagicMock:
        return MagicMock()

    def test_tmp_path_has_correct_suffix(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """The tmp_path must use '.download.tmp' suffix, not a mutated variant.
        This kills mutants that change '.download.tmp' to 'XX.download.tmpXX' or '.DOWNLOAD.TMP'.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        expected_tmp = data_dir / entry.target_dir / (entry.filename + ".download.tmp")

        with responses_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(responses_lib.GET, entry.url, body=b"data", status=200)
            with patch("extract.downloader.download._fetch_content") as mock_fetch:
                with patch("extract.downloader.download.compute_sha256", return_value="abc123"):
                    with patch("extract.downloader.download.cleanup_stale_tmp") as mock_cleanup:
                        with patch("extract.downloader.download.backup_existing_file"):
                            with patch("extract.downloader.download.safe_unlink"):
                                with patch("extract.downloader.download.Path.rename"):
                                    download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        # Verify cleanup_stale_tmp was called with the correct tmp_path
        mock_cleanup.assert_called_once()
        called_path = mock_cleanup.call_args[0][0]
        assert called_path == expected_tmp

    def test_tmp_path_cleaned_on_network_error(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """When a network error occurs, the tmp file must be cleaned up.
        This kills mutants that remove the safe_unlink(tmp_path) call.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        expected_tmp = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not expected_tmp.exists()


class TestDownloadAndVerifyStateLogErrorArgs:
    """Tests to kill argument mutations in state.log_error calls."""

    @pytest.fixture
    def mock_state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def mock_known_missing(self) -> MagicMock:
        return MagicMock()

    def test_network_error_passes_correct_url_to_log_error(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """state.log_error must receive the correct URL, not None.
        This kills mutants that change entry.url to None in state.log_error.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with patch.object(mock_state, "log_error") as mock_log:
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        # Check that log_error was called with the correct URL
        mock_log.assert_called_once_with(entry.url, ErrorType.NETWORK_ERROR, "ConnectionError")

    def test_network_error_passes_correct_error_type(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """state.log_error must receive ErrorType.NETWORK_ERROR, not a mutated variant.
        This kills mutants that change ErrorType.NETWORK_ERROR.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with patch.object(mock_state, "log_error") as mock_log:
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][1] == ErrorType.NETWORK_ERROR

    def test_network_error_passes_correct_error_name(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """state.log_error must receive str(type(e).__name__), not None or a mutated variant.
        This kills mutants that change str(type(e).__name__) to None, '', str(None), or str(type(None).__name__).
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with patch.object(mock_state, "log_error") as mock_log:
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][2] == "ConnectionError"
        assert call_args[0][2] is not None
        assert call_args[0][2] != ""
        assert call_args[0][2] != "None"
        assert call_args[0][2] != "type"


class TestDownloadAndVerifyLoggerErrorArgs:
    """Tests to kill argument mutations in logger.error calls."""

    @pytest.fixture
    def mock_state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def mock_known_missing(self) -> MagicMock:
        return MagicMock()

    def test_network_error_logger_receives_correct_url(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock, caplog: pytest.LogCaptureFixture):
        """logger.error must receive the correct URL, not None.
        This kills mutants that change entry.url to None in logger.error.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        # Verify the log message contains the exact URL
        assert any(entry.url in record.message for record in caplog.records)

    def test_network_error_logger_receives_correct_error_type_name(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock, caplog: pytest.LogCaptureFixture):
        """logger.error must receive type(e).__name__, not None or type(None).__name__.
        This kills mutants that change type(e).__name__ to None or type(None).__name__.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("connection refused"),
                status=500,
            )
            with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
                download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        # Verify the log message contains "ConnectionError" not "None" or "type"
        error_records = [r for r in caplog.records if "Network error" in r.message]
        assert len(error_records) >= 1
        assert "ConnectionError" in error_records[0].message
        assert "None" not in error_records[0].message
        assert "type" not in error_records[0].message


class TestHandleDownloadErrorLoggerArgs:
    """Tests to kill argument mutations in handle_download_error logger.error calls."""

    def test_404_logger_receives_correct_url(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error in 404 path must receive entry.url, not None.
        This kills mutants that change entry.url to None.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        # Verify the log message contains the exact URL
        assert any(entry.url in record.message for record in caplog.records)

    def test_404_logger_receives_correct_message(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error message must not be mutated (XX...XX or lowercase).
        This kills string mutations in the log message.
        """
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        error_records = [r for r in caplog.records if "not found" in r.message.lower() and "recording" in r.message.lower()]
        assert len(error_records) >= 1
        # Message should contain "File not found" not "XXFile not foundXX" or "unexpected error"
        assert "XX" not in error_records[0].message
        assert "unexpected" not in error_records[0].message

    def test_500_logger_receives_correct_url(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error in 500 path must receive entry.url, not None."""
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        assert any(entry.url in record.message for record in caplog.records)

    def test_network_error_logger_receives_correct_args(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error in network error path must receive correct args."""
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.ConnectionError("connection refused")

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(error, entry, state, known_missing)

        error_records = [r for r in caplog.records if "Network error" in r.message]
        assert len(error_records) >= 1
        assert entry.url in error_records[0].message
        assert "ConnectionError" in error_records[0].message

    def test_unknown_error_logger_receives_correct_url(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error in unknown error path must receive entry.url, not None."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(ValueError("unexpected"), entry, state, known_missing)

        assert any(entry.url in record.message for record in caplog.records)

    def test_unknown_error_logger_message_not_mutated(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """logger.error message must not be mutated."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            handle_download_error(ValueError("unexpected"), entry, state, known_missing)

        error_records = [r for r in caplog.records if "unexpected" in r.message]
        assert len(error_records) >= 1
        assert "XX" not in error_records[0].message


class TestLogHttpErrorLoggerArgs:
    """Tests to kill argument mutations in _log_http_error logger.error calls."""

    def test_404_logger_receives_correct_url(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        import requests

        url = "https://example.com/file.parquet"
        state = State(tmp_path / "state.json")

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            _log_http_error(error, url, state, None)

        assert any(url in record.message for record in caplog.records)

    def test_404_logger_message_not_mutated(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        import requests

        url = "https://example.com/file.parquet"
        state = State(tmp_path / "state.json")

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            _log_http_error(error, url, state, None)

        error_records = [r for r in caplog.records if "not found" in r.message.lower()]
        assert len(error_records) >= 1
        assert "XX" not in error_records[0].message

    def test_non_404_logger_receives_correct_url(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        import requests

        url = "https://example.com/file.parquet"
        state = State(tmp_path / "state.json")

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        with caplog.at_level(logging.ERROR, logger="extract.downloader.download"):
            _log_http_error(error, url, state, None)

        assert any(url in record.message for record in caplog.records)


class TestFetchContentRequestsArgs:
    """Tests to kill argument mutations in _fetch_content requests.get calls."""

    def test_requests_get_receives_timeout_300(self, tmp_path: Path):
        """requests.get must receive timeout=300, not None or a different value.
        This kills mutants that change timeout=300 to timeout=None, timeout=301, or remove timeout.
        """
        tmp_file = tmp_path / "output.parquet"

        with patch("extract.downloader.download.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.iter_content.return_value = [b"data"]
            mock_get.return_value = mock_response

            _fetch_content("https://example.com/file.parquet", tmp_file)

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs.get("timeout") == 300

    def test_requests_get_receives_stream_true(self, tmp_path: Path):
        """requests.get must receive stream=True, not stream=False or stream=None.
        This kills mutants that change stream=True to stream=False or stream=None.
        """
        tmp_file = tmp_path / "output.parquet"

        with patch("extract.downloader.download.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.iter_content.return_value = [b"data"]
            mock_get.return_value = mock_response

            _fetch_content("https://example.com/file.parquet", tmp_file)

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs.get("stream") is True

    def test_requests_get_receives_correct_chunk_size(self, tmp_path: Path):
        """iter_content must receive chunk_size=8192, not None or 8193.
        This kills mutants that change chunk_size=8192 to chunk_size=None or chunk_size=8193.
        """
        tmp_file = tmp_path / "output.parquet"

        with patch("extract.downloader.download.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.iter_content.return_value = [b"data"]
            mock_get.return_value = mock_response

            _fetch_content("https://example.com/file.parquet", tmp_file)

        mock_response = mock_get.return_value
        mock_response.iter_content.assert_called_once()
        call_kwargs = mock_response.iter_content.call_args[1]
        assert call_kwargs.get("chunk_size") == 8192


class TestDownloadAndVerifyMkdirArgs:
    """Tests to kill parameter mutations in mkdir calls."""

    @pytest.fixture
    def mock_state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def mock_known_missing(self) -> MagicMock:
        return MagicMock()

    def test_mkdir_receives_parents_true(self, tmp_path: Path, mock_state: State, mock_known_missing: MagicMock):
        """target_path.parent.mkdir must receive parents=True, not parents=False or parents=None.
        This kills mutants that change parents=True to parents=False or parents=None.
        """
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"data",
                status=200,
            )
            with patch("extract.downloader.download.cleanup_stale_tmp"):
                with patch("extract.downloader.download.compute_sha256", return_value="abc123"):
                    with patch("extract.downloader.download.backup_existing_file"):
                        with patch("extract.downloader.download.safe_unlink"):
                            with patch("extract.downloader.download._fetch_content"):
                                with patch("extract.downloader.download.Path.mkdir") as mock_mkdir:
                                    with patch("extract.downloader.download.Path.rename"):
                                        download_and_verify(entry, data_dir, mock_state, mock_known_missing)

        # Verify mkdir was called with parents=True
        mock_mkdir.assert_called()
        # Find the call with parents kwarg
        for call in mock_mkdir.call_args_list:
            if "parents" in call[1]:
                assert call[1]["parents"] is True
                break
        else:
            pytest.fail("mkdir was not called with parents=True")
