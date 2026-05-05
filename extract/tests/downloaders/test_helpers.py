"""Unit tests for downloader helper functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from extract.downloader.downloader import (
    _apply_mode,
    _backup_existing_file,
    _cleanup_stale_tmp,
    _fetch_content,
    _handle_http_error,
    _handle_network_error,
    _make_result,
    _resolve_data_dir,
    _safe_unlink,
)
from extract.core.state import ErrorType
from extract.core.state_manager import State


class TestResolveDataDir:
    def test_none_defaults_to_data(self):
        result = _resolve_data_dir(None)
        assert result == Path("data")

    def test_string_path(self):
        result = _resolve_data_dir("/custom/path")
        assert result == Path("/custom/path")

    def test_path_object(self, tmp_path: Path):
        result = _resolve_data_dir(tmp_path)
        assert result == tmp_path

    def test_empty_string_defaults_to_data(self):
        result = _resolve_data_dir("")
        assert result == Path("data")


class TestApplyMode:
    def test_full_mode_resets_state(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "hash1")
        assert state.is_downloaded("https://example.com/file.parquet")
        _apply_mode(state, "full")
        assert not state.is_downloaded("https://example.com/file.parquet")

    def test_incremental_mode_does_not_reset(self, state_file: Path):
        state = State(state_file)
        state.save("https://example.com/file.parquet", "hash1")
        _apply_mode(state, "incremental")
        assert state.is_downloaded("https://example.com/file.parquet")


class TestMakeResult:
    def test_returns_correct_dict(self):
        result = _make_result(10, 5, 3, 18)
        assert result == {"downloaded": 10, "skipped": 5, "failed": 3, "total": 18}

    def test_zero_values(self):
        result = _make_result(0, 0, 0, 0)
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}


class TestCleanupStaleTmp:
    def test_removes_existing_tmp(self, tmp_path: Path):
        tmp_file = tmp_path / "stale.tmp"
        tmp_file.write_bytes(b"stale content")
        _cleanup_stale_tmp(tmp_file)
        assert not tmp_file.exists()

    def test_noop_when_tmp_missing(self, tmp_path: Path):
        tmp_file = tmp_path / "missing.tmp"
        _cleanup_stale_tmp(tmp_file)
        assert not tmp_file.exists()


class TestSafeUnlink:
    def test_removes_existing_file(self, tmp_path: Path):
        test_file = tmp_path / "test.tmp"
        test_file.write_bytes(b"data")
        _safe_unlink(test_file)
        assert not test_file.exists()

    def test_noop_when_missing(self, tmp_path: Path):
        test_file = tmp_path / "missing.tmp"
        _safe_unlink(test_file)

    def test_multiple_unlinks_no_error(self, tmp_path: Path):
        test_file = tmp_path / "test.tmp"
        test_file.write_bytes(b"data")
        _safe_unlink(test_file)
        _safe_unlink(test_file)


class TestBackupExistingFile:
    def test_renames_to_old(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original data")
        _backup_existing_file(target)
        assert not target.exists()
        backup = tmp_path / "data.parquet.old"
        assert backup.exists()
        assert backup.read_bytes() == b"original data"

    def test_only_renames_existing(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original data")
        _backup_existing_file(target)
        assert not target.exists()
        assert (tmp_path / "data.parquet.old").exists()

    def test_preserves_content(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        content = b"x" * 1000
        target.write_bytes(content)
        _backup_existing_file(target)
        assert (tmp_path / "data.parquet.old").read_bytes() == content


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

        _handle_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
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

        _handle_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
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

        _handle_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
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

        _handle_http_error(http_error, "https://example.com/file.parquet", state, known_missing)
        assert not known_missing.is_missing("https://example.com/file.parquet")

    def test_handles_non_http_error(self, tmp_path: Path):
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing
        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        _handle_http_error(ValueError("unexpected"), "https://example.com/file.parquet", state, known_missing)

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
