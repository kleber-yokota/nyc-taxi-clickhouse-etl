"""Tests for downloader_util functions (backup, cleanup, safe_unlink, error handlers)."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extract.downloader.downloader_util import (
    backup_existing_file,
    cleanup_stale_tmp,
    handle_http_error,
    handle_network_error,
    safe_unlink,
)
from extract.core.state import ErrorType
from extract.core.state_manager import State


class TestBackupExistingFile:
    """Tests for backup_existing_file function."""

    def test_creates_old_backup(self, tmp_path: Path):
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original-content")

        backup_existing_file(target)

        backup = tmp_path / "data.parquet.old"
        assert backup.exists()
        assert backup.read_bytes() == b"original-content"
        assert not target.exists()

    def test_logs_backup_action(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        target = tmp_path / "test.parquet"
        target.write_bytes(b"content")

        with caplog.at_level(logging.INFO, logger="extract.downloader.downloader_util"):
            backup_existing_file(target)

        assert any("Backed up old file" in record.message for record in caplog.records)


class TestCleanupStaleTmp:
    """Tests for cleanup_stale_tmp function."""

    def test_removes_existing_tmp(self, tmp_path: Path):
        tmp_file = tmp_path / "stale.tmp"
        tmp_file.write_bytes(b"stale")

        cleanup_stale_tmp(tmp_file)

        assert not tmp_file.exists()

    def test_does_nothing_when_missing(self, tmp_path: Path):
        tmp_file = tmp_path / "nonexistent.tmp"

        cleanup_stale_tmp(tmp_file)

        assert not tmp_file.exists()


class TestSafeUnlink:
    """Tests for safe_unlink function."""

    def test_removes_existing_file(self, tmp_path: Path):
        test_file = tmp_path / "test.tmp"
        test_file.write_bytes(b"data")

        safe_unlink(test_file)

        assert not test_file.exists()

    def test_does_nothing_when_missing(self, tmp_path: Path):
        test_file = tmp_path / "nonexistent.tmp"

        safe_unlink(test_file)

        assert not test_file.exists()


class TestHandleHttpError:
    """Tests for handle_http_error function (test compatibility alias)."""

    def test_404_records_missing_file(self, tmp_path: Path):
        import requests

        entry_url = "https://example.com/missing.parquet"
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        handle_http_error(error, entry_url, state, known_missing)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()
        known_missing.add.assert_called_once_with(entry_url)

    def test_500_records_http_error(self, tmp_path: Path):
        import requests

        entry_url = "https://example.com/error.parquet"
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        handle_http_error(error, entry_url, state, known_missing)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()
        known_missing.add.assert_not_called()

    def test_non_http_error_ignored(self, tmp_path: Path):
        entry_url = "https://example.com/test.parquet"
        state = State(tmp_path / "state.json")
        known_missing = MagicMock()

        handle_http_error(ValueError("not http"), entry_url, state, known_missing)

        state_errors = state._errors_dir / "download_errors.log"
        assert not state_errors.exists()

    def test_404_with_none_known_missing(self, tmp_path: Path):
        import requests

        entry_url = "https://example.com/missing.parquet"
        state = State(tmp_path / "state.json")

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        handle_http_error(error, entry_url, state, None)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()


class TestHandleNetworkError:
    """Tests for handle_network_error function (test compatibility alias)."""

    def test_records_network_error(self, tmp_path: Path):
        import requests

        entry_url = "https://example.com/test.parquet"
        state = State(tmp_path / "state.json")

        error = requests.ConnectionError("connection refused")

        handle_network_error(error, entry_url, state)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()

    def test_logs_error_message(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        import requests

        entry_url = "https://example.com/test.parquet"
        state = State(tmp_path / "state.json")

        error = requests.Timeout("timeout")

        with caplog.at_level(logging.ERROR, logger="extract.downloader.downloader_util"):
            handle_network_error(error, entry_url, state)

        assert any("Network error" in record.message for record in caplog.records)

    def test_passes_exception_class_name(self, tmp_path: Path):
        import requests

        entry_url = "https://example.com/test.parquet"
        state = State(tmp_path / "state.json")

        error = requests.ConnectionError("connection refused")

        handle_network_error(error, entry_url, state)

        state_errors = state._errors_dir / "download_errors.log"
        assert state_errors.exists()
        log_content = state_errors.read_text()
        assert "ConnectionError" in log_content
