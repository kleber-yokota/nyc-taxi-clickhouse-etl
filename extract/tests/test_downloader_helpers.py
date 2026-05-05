"""Unit tests for downloader helpers that kill literal-value mutations."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses

from extract.core.downloader import (
    _apply_mode,
    _backup_existing_file,
    _cleanup_stale_tmp,
    _download_entry,
    _fetch_content,
    _handle_http_error,
    _handle_network_error,
    _make_result,
    _process_entry,
    _resolve_data_dir,
    _safe_unlink,
    run,
)
from extract.core.state import CatalogEntry, ErrorType


class TestResolveDataDir:
    def test_none_returns_data(self):
        assert _resolve_data_dir(None) == Path("data")

    def test_string_path(self):
        assert _resolve_data_dir("/custom/path") == Path("/custom/path")

    def test_path_object(self, tmp_path: Path):
        assert _resolve_data_dir(tmp_path) == tmp_path


class TestApplyMode:
    def test_full_resets_state(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, "full")
        mock_state.reset.assert_called_once()

    def test_incremental_no_reset(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, "incremental")
        mock_state.reset.assert_not_called()

    def test_none_no_reset(self):
        mock_state = MagicMock()
        _apply_mode(mock_state, None)
        mock_state.reset.assert_not_called()


class TestMakeResult:
    def test_returns_dict_with_exact_values(self):
        result = _make_result(5, 3, 2, 10)
        assert result == {"downloaded": 5, "skipped": 3, "failed": 2, "total": 10}

    def test_all_zeros(self):
        result = _make_result(0, 0, 0, 0)
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_all_values(self):
        result = _make_result(100, 50, 25, 175)
        assert result["downloaded"] == 100
        assert result["skipped"] == 50
        assert result["failed"] == 25
        assert result["total"] == 175

    def test_returns_new_dict(self):
        r1 = _make_result(1, 2, 3, 6)
        r2 = _make_result(4, 5, 6, 15)
        assert r1 is not r2
        assert r1 != r2


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


class TestProcessEntry:
    def test_skips_known_missing(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = _process_entry(
            entry, Path("."), state, known_missing, 0, 0, 0
        )

        assert result == (0, 1, 0)
        known_missing.is_missing.assert_called_once_with(entry.url)
        state.is_downloaded.assert_not_called()

    def test_skips_downloaded_existing(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing_mock = MagicMock()
        known_missing_mock.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing_mock, 0, 0, 0
            )

        assert state.is_downloaded.called

    def test_saves_empty_when_downloaded_no_file(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = True
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        target_dir = tmp_path / "yellow"
        target_dir.mkdir()

        with patch("extract.core.downloader_ops.download_and_verify", return_value="downloaded"):
            _process_entry(
                entry, tmp_path, state, known_missing, 0, 0, 0
            )

        state.save.assert_called_once_with(entry.url, "")

    def test_process_entry_with_known_missing_skips_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        result = _process_entry(
            entry, Path("."), state, known_missing, 5, 10, 3
        )

        assert result == (5, 11, 3)
        state.is_downloaded.assert_not_called()
        state.save.assert_not_called()

    def test_counts_downloaded(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (1, 0, 0)

    def test_counts_skipped_from_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify", return_value="skipped"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 1, 0)

    def test_counts_failed_from_download(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify", return_value="failed"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 0, 1)

    def test_exception_counts_failed(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify",
            side_effect=RuntimeError("boom"),
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 0, 0, 0
            )

        assert result == (0, 0, 1)
        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][0] == entry.url
        assert args[0][1] == ErrorType.UNKNOWN

    def test_known_missing_does_not_check_state(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = True

        _process_entry(
            entry, Path("."), state, known_missing, 5, 10, 3
        )

        state.is_downloaded.assert_not_called()
        state.save.assert_not_called()

    def test_process_entry_preserves_counts(self):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.core.downloader_ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 10, 20, 5
            )

        assert result == (11, 20, 5)

    def test_download_entry_returns_failed_on_http_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()

        import requests
        http_error = requests.HTTPError()
        http_error.response = MagicMock()
        http_error.response.status_code = 500

        with patch("extract.core.downloader_download._fetch_content", side_effect=http_error):
            result = _download_entry(entry, tmp_path, state)

        assert result == "failed"

    def test_download_entry_returns_failed_on_network_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()

        import requests
        with patch("extract.core.downloader_download._fetch_content", side_effect=requests.RequestException("conn refused")):
            result = _download_entry(entry, tmp_path, state)

        assert result == "failed"

    def test_run_returns_zero_when_no_entries(self):
        with patch("extract.core.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir="/tmp/test_etl", types=["yellow"], max_entries=0)

        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_backup_existing_file_logs(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        import logging

        target = tmp_path / "test.parquet"
        target.write_bytes(b"content")

        with caplog.at_level(logging.INFO, logger="extract.core.downloader_util"):
            _backup_existing_file(target)

        assert any("Backed up old file" in record.message for record in caplog.records)

    def test_handle_http_error_404_logs_message(self, caplog: pytest.LogCaptureFixture):
        import logging
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/missing.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.core.downloader"):
            _handle_http_error(error, url, state, known_missing)

        assert any("File not found" in record.message for record in caplog.records)
        assert any("HTTP 404" in record.message for record in caplog.records)

    def test_handle_http_error_500_logs_message(self, caplog: pytest.LogCaptureFixture):
        import logging
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/error.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.core.downloader"):
            _handle_http_error(error, url, state, known_missing)

        assert any("HTTP error" in record.message for record in caplog.records)
        assert any("HTTP 500" in record.message for record in caplog.records)

    def test_handle_network_error_logs_message(self, caplog: pytest.LogCaptureFixture):
        import logging
        import requests

        error = requests.RequestException("connection refused")

        state = MagicMock()
        url = "https://example.com/test.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.core.downloader"):
            _handle_network_error(error, url, state)

        assert any("Network error" in record.message for record in caplog.records)

    def test_run_empty_catalog_returns_zero_result(self, tmp_path: Path):
        with patch("extract.core.downloader.Catalog.generate", return_value=[]):
            result = run(data_dir=tmp_path, types=["yellow"])
        assert result == {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    def test_run_with_max_entries_limit(self, tmp_path: Path):
        result = run(data_dir=tmp_path, types=["yellow"], from_year=2024, to_year=2024, max_entries=2)
        assert result["total"] == 2
