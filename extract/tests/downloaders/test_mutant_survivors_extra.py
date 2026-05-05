"""Tests targeting mutmut survivors in downloader_ops and downloader_util - extra tests."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader_ops import process_entry, should_skip_download
from extract.core.state import CatalogEntry, ErrorType
from extract.downloader.downloader_util import (
    backup_existing_file,
    handle_http_error,
    handle_network_error,
)
from extract.core.state_manager import State


class TestHandleNetworkErrorState:
    """Tests to kill mutmut survivors in handle_network_error."""

    def test_logs_error_to_state(self, tmp_path: Path):
        """handle_network_error must call state.log_error.
        This kills the mutant that removes the state.log_error call.
        """
        state = State(tmp_path / "state.json")
        import requests

        error = requests.ConnectionError("connection refused")

        handle_network_error(error, "https://example.com/file.parquet", state)

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()
        content = error_log.read_text()
        assert "https://example.com/file.parquet" in content
        assert "network_error" in content

    def test_creates_error_log_file(self, tmp_path: Path):
        """handle_network_error must create the error log file."""
        state = State(tmp_path / "state.json")
        import requests

        error = requests.ConnectionError("connection refused")

        handle_network_error(error, "https://example.com/file.parquet", state)

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()


class TestHandleHttpErrorState:
    """Tests to kill mutmut survivors in handle_http_error."""

    def test_404_calls_state_log_error(self, tmp_path: Path):
        """handle_http_error 404 must call state.log_error."""
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing

        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        class MockResponse:
            status_code = 404

        import requests

        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        handle_http_error(
            http_error, "https://example.com/file.parquet", state, known_missing
        )

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()
        content = error_log.read_text()
        assert "https://example.com/file.parquet" in content
        assert "missing_file" in content

    def test_500_calls_state_log_error(self, tmp_path: Path):
        """handle_http_error 500 must call state.log_error."""
        state = State(tmp_path / "state.json")
        from extract.core.known_missing import KnownMissing

        known_missing = KnownMissing(tmp_path / "known_missing.txt")

        class MockResponse:
            status_code = 500

        import requests

        http_error = requests.HTTPError()
        http_error.response = MockResponse()

        handle_http_error(
            http_error, "https://example.com/file.parquet", state, known_missing
        )

        error_log = state._errors_dir / "download_errors.log"
        assert error_log.exists()
        content = error_log.read_text()
        assert "https://example.com/file.parquet" in content
        assert "http_error" in content


class TestBackupExistingFileRename:
    """Tests to kill mutmut survivors in backup_existing_file."""

    def test_file_is_actually_renamed(self, tmp_path: Path):
        """The file must be renamed to .old. This kills the mutant that removes rename."""
        target = tmp_path / "data.parquet"
        target.write_bytes(b"original data")

        backup_existing_file(target)

        assert not target.exists()
        backup = tmp_path / "data.parquet.old"
        assert backup.exists()
        assert backup.read_bytes() == b"original data"

    def test_backup_preserves_content(self, tmp_path: Path):
        """Backup must preserve the original file content."""
        content = b"x" * 500
        target = tmp_path / "data.parquet"
        target.write_bytes(content)

        backup_existing_file(target)

        assert (tmp_path / "data.parquet.old").read_bytes() == content
