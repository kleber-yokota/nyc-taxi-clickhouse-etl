"""Tests for IO-related downloader functions (HTTP errors, network errors, file ops)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses

from extract.core.downloader import (
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


class TestBackupExistingFile:
    def test_creates_backup_with_old_suffix(self, tmp_path: Path):
        target = tmp_path / "test.parquet"
        target.write_bytes(b"content")

        _backup_existing_file(target)

        backup = tmp_path / "test.parquet.old"
        assert backup.exists()
        assert backup.read_bytes() == b"content"
        assert not target.exists()

    def test_backup_path_correct(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"data")

        _backup_existing_file(target)

        assert (tmp_path / "data.parquet.old").exists()
        assert not (tmp_path / "data.parquet").exists()


class TestFetchContent:
    @responses.activate
    def test_fetches_content_to_file(self, tmp_path: Path):
        url = "https://example.com/test.parquet"
        content = b"test content"
        responses.add(responses.GET, url, body=content, status=200)

        tmp_file = tmp_path / "download.tmp"
        _fetch_content(url, tmp_file)

        assert tmp_file.read_bytes() == content

    @responses.activate
    def test_fetch_uses_timeout(self, tmp_path: Path):
        from extract.core.state import DOWNLOAD_TIMEOUT

        url = "https://example.com/test.parquet"
        responses.add(responses.GET, url, body=b"data", status=200)

        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.iter_content.return_value = [b"data"]
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            _fetch_content(url, tmp_path / "tmp")

            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["timeout"] == DOWNLOAD_TIMEOUT

    @responses.activate
    def test_fetch_raises_on_error(self, tmp_path: Path):
        url = "https://example.com/missing.parquet"
        responses.add(responses.GET, url, body="", status=404)

        with pytest.raises(Exception):
            _fetch_content(url, tmp_path / "tmp")


class TestSafeUnlink:
    def test_removes_file(self, tmp_path: Path):
        f = tmp_path / "to_delete.txt"
        f.write_text("data")
        _safe_unlink(f)
        assert not f.exists()

    def test_no_error_on_missing(self, tmp_path: Path):
        f = tmp_path / "missing.txt"
        _safe_unlink(f)  # should not raise

    def test_removes_symlink(self, tmp_path: Path):
        target = tmp_path / "target.txt"
        target.write_text("data")
        symlink = tmp_path / "link.txt"
        symlink.symlink_to(target)
        _safe_unlink(symlink)
        assert not symlink.exists()
        assert target.exists()  # target still exists


