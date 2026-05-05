"""Tests for download_and_verify exercising all code paths."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import responses as responses_lib

from extract.downloader.downloader_download import download_and_verify
from extract.core.state import CatalogEntry
from extract.core.state_manager import State


class TestDownloadAndVerifyBranches:
    """Tests for download_and_verify exercising all code paths."""

    @pytest.fixture
    def state(self, tmp_path: Path) -> State:
        return State(tmp_path / "state.json")

    @pytest.fixture
    def data_dir(self, tmp_path: Path) -> Path:
        return tmp_path / "data"

    def test_download_saves_checksum(self, data_dir: Path, state: State):
        """Verify successful download saves checksum to state."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"test-data-for-checksum",
                status=200,
            )
            download_and_verify(entry, data_dir, state)

        assert entry.url in state.checksums
        assert state.checksums[entry.url] == hashlib.sha256(b"test-data-for-checksum").hexdigest()

    def test_download_creates_target_dir(self, tmp_path: Path, state: State):
        """Verify download creates nested target directory."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"data",
                status=200,
            )
            download_and_verify(entry, data_dir, state)

        assert (data_dir / entry.target_dir).exists()

    def test_download_renames_tmp_to_target(self, tmp_path: Path, state: State):
        """Verify download renames tmp file to final target."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"data",
                status=200,
            )
            download_and_verify(entry, data_dir, state)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        target_file = data_dir / entry.target_dir / entry.filename
        assert not tmp_file.exists()
        assert target_file.exists()

    def test_download_backup_on_mismatch(self, tmp_path: Path, state: State):
        """Verify download backs up existing file on checksum mismatch."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"old-content")

        old_checksum = hashlib.sha256(b"old-content").hexdigest()
        state.save(entry.url, old_checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"new-content",
                status=200,
            )
            result = download_and_verify(entry, data_dir, state)

        assert result == "downloaded"
        assert (target_dir / (entry.filename + ".old")).exists()
        assert target_file.read_bytes() == b"new-content"

    def test_download_skips_on_matching_checksum(self, tmp_path: Path, state: State):
        """Verify download skips when checksum matches."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"same-content")

        checksum = hashlib.sha256(b"same-content").hexdigest()
        state.save(entry.url, checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"same-content",
                status=200,
            )
            result = download_and_verify(entry, data_dir, state)

        assert result == "skipped"

    def test_download_deletes_tmp_on_skip(self, tmp_path: Path, state: State):
        """Verify download deletes tmp file when skipping."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        target_dir = data_dir / entry.target_dir
        target_dir.mkdir(parents=True)
        target_file = target_dir / entry.filename
        target_file.write_bytes(b"same")

        checksum = hashlib.sha256(b"same").hexdigest()
        state.save(entry.url, checksum)

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=b"same",
                status=200,
            )
            download_and_verify(entry, data_dir, state)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_download_404_returns_failed(self, tmp_path: Path, state: State):
        """Verify 404 returns 'failed'."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            result = download_and_verify(entry, data_dir, state)

        assert result == "failed"

    def test_download_500_returns_failed(self, tmp_path: Path, state: State):
        """Verify 500 returns 'failed'."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Error",
            )
            result = download_and_verify(entry, data_dir, state)

        assert result == "failed"

    def test_download_network_error_returns_failed(self, tmp_path: Path, state: State):
        """Verify network error returns 'failed'."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        import requests

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("refused"),
                status=500,
            )
            result = download_and_verify(entry, data_dir, state)

        assert result == "failed"

    def test_download_deletes_tmp_on_404(self, tmp_path: Path, state: State):
        """Verify tmp file is deleted after 404 failure."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            download_and_verify(entry, data_dir, state)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_download_deletes_tmp_on_500(self, tmp_path: Path, state: State):
        """Verify tmp file is deleted after 500 failure."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Error",
            )
            download_and_verify(entry, data_dir, state)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_download_deletes_tmp_on_network_error(self, tmp_path: Path, state: State):
        """Verify tmp file is deleted after network error."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        import requests

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                body=requests.exceptions.ConnectionError("refused"),
                status=500,
            )
            download_and_verify(entry, data_dir, state)

        tmp_file = data_dir / entry.target_dir / (entry.filename + ".download.tmp")
        assert not tmp_file.exists()

    def test_download_with_known_missing_records_404(self, tmp_path: Path, state: State):
        """Verify 404 with known_missing records the URL."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        known_missing = MagicMock()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            download_and_verify(entry, data_dir, state, known_missing)

        known_missing.add.assert_called_once_with(entry.url)

    def test_download_with_known_missing_does_not_add_on_500(self, tmp_path: Path, state: State):
        """Verify 500 with known_missing does NOT record the URL."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        known_missing = MagicMock()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Error",
            )
            download_and_verify(entry, data_dir, state, known_missing)

        known_missing.add.assert_not_called()

    def test_download_with_known_missing_none_404(self, tmp_path: Path, state: State):
        """Verify 404 without known_missing (None) still works."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=404,
                body=b"Not Found",
            )
            result = download_and_verify(entry, data_dir, state, None)

        assert result == "failed"

    def test_download_with_known_missing_none_500(self, tmp_path: Path, state: State):
        """Verify 500 without known_missing (None) still works."""
        entry = CatalogEntry("yellow", 2024, 1)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                entry.url,
                status=500,
                body=b"Error",
            )
            result = download_and_verify(entry, data_dir, state, None)

        assert result == "failed"
