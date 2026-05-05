"""Tests for downloader ops functions (backup, error logging)."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extract.downloader.downloader import _backup_existing_file, _handle_http_error, _handle_network_error


class TestBackupExistingFile:
    def test_logs_backup_action(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        target = tmp_path / "test.parquet"
        target.write_bytes(b"content")

        with caplog.at_level(logging.INFO, logger="extract.downloader.downloader_util"):
            _backup_existing_file(target)

        assert any("Backed up old file" in record.message for record in caplog.records)


class TestHandleHttpErrorLogging:
    def test_404_logs_message(self, caplog: pytest.LogCaptureFixture):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/missing.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.downloader.downloader"):
            _handle_http_error(error, url, state, known_missing)

        assert any("File not found" in record.message for record in caplog.records)
        assert any("HTTP 404" in record.message for record in caplog.records)

    def test_500_logs_message(self, caplog: pytest.LogCaptureFixture):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 500

        state = MagicMock()
        known_missing = MagicMock()
        url = "https://example.com/error.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.downloader.downloader"):
            _handle_http_error(error, url, state, known_missing)

        assert any("HTTP error" in record.message for record in caplog.records)
        assert any("HTTP 500" in record.message for record in caplog.records)


class TestHandleNetworkErrorLogging:
    def test_logs_message(self, caplog: pytest.LogCaptureFixture):
        import requests

        error = requests.RequestException("connection refused")

        state = MagicMock()
        url = "https://example.com/test.parquet"

        with caplog.at_level(logging.ERROR, logger="extract.downloader.downloader"):
            _handle_network_error(error, url, state)

        assert any("Network error" in record.message for record in caplog.records)
