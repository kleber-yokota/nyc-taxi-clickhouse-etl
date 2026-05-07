"""Tests for _download_entry return values in download module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.download import download_and_verify as _download_entry
from extract.core.state import CatalogEntry


class TestDownloadEntryReturns:
    def test_process_entry_preserves_counts(self):
        from extract.downloader.ops import process_entry as _process_entry

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        state.is_downloaded.return_value = False
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False

        with patch(
            "extract.downloader.ops.download_and_verify", return_value="downloaded"
        ):
            result = _process_entry(
                entry, Path("."), state, known_missing, 10, 20, 5
            )

        assert result == (11, 20, 5)

    def test_download_entry_returns_failed_on_http_error(self, tmp_path: Path):
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        http_error = requests.HTTPError()
        http_error.response = MagicMock()
        http_error.response.status_code = 500

        with patch("extract.downloader.download._fetch_content", side_effect=http_error):
            result = _download_entry(entry, tmp_path, state)

        assert result == "failed"

    def test_download_entry_returns_failed_on_network_error(self, tmp_path: Path):
        import requests

        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()

        with patch("extract.downloader.download._fetch_content", side_effect=requests.RequestException("conn refused")):
            result = _download_entry(entry, tmp_path, state)

        assert result == "failed"
