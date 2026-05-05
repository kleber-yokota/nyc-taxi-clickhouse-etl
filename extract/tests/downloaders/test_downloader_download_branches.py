"""Tests for _fetch_content and download_and_verify exercising all branches."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import responses as responses_lib

from extract.downloader.downloader_download import _fetch_content, download_and_verify
from extract.core.state import CatalogEntry
from extract.core.state_manager import State


class TestFetchContent:
    """Tests for _fetch_content function."""

    def test_fetch_writes_bytes_to_file(self, tmp_path: Path):
        """Verify _fetch_content writes downloaded bytes to tmp_path."""
        data = b"parquet-content-data-12345"
        tmp_path = tmp_path / "test.parquet.tmp"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://example.com/test.parquet",
                body=data,
                status=200,
            )
            _fetch_content("https://example.com/test.parquet", tmp_path)

        assert tmp_path.exists()
        assert tmp_path.read_bytes() == data

    def test_fetch_writes_large_content(self, tmp_path: Path):
        """Verify _fetch_content handles larger content streams."""
        data = b"x" * 100_000
        tmp_path = tmp_path / "large.parquet.tmp"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://example.com/large.parquet",
                body=data,
                status=200,
            )
            _fetch_content("https://example.com/large.parquet", tmp_path)

        assert tmp_path.read_bytes() == data

    def test_fetch_raises_on_404(self, tmp_path: Path):
        """Verify _fetch_content raises HTTPError on 404."""
        tmp_path = tmp_path / "test.parquet.tmp"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://example.com/missing.parquet",
                status=404,
                body=b"Not Found",
            )
            with pytest.raises(Exception):  # requests.HTTPError
                _fetch_content("https://example.com/missing.parquet", tmp_path)

    def test_fetch_raises_on_500(self, tmp_path: Path):
        """Verify _fetch_content raises HTTPError on 500."""
        tmp_path = tmp_path / "test.parquet.tmp"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://example.com/error.parquet",
                status=500,
                body=b"Internal Server Error",
            )
            with pytest.raises(Exception):  # requests.HTTPError
                _fetch_content("https://example.com/error.parquet", tmp_path)

    def test_fetch_writes_correct_sha256(self, tmp_path: Path):
        """Verify _fetch_content writes data that produces correct SHA-256."""
        data = b"verify-sha256-content"
        tmp_path = tmp_path / "verify.parquet.tmp"

        with responses_lib.RequestsMock() as rsps:
            rsps.add(
                responses_lib.GET,
                "https://example.com/verify.parquet",
                body=data,
                status=200,
            )
            _fetch_content("https://example.com/verify.parquet", tmp_path)

        actual_checksum = hashlib.sha256(tmp_path.read_bytes()).hexdigest()
        expected_checksum = hashlib.sha256(data).hexdigest()
        assert actual_checksum == expected_checksum


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
        from unittest.mock import MagicMock
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
        from unittest.mock import MagicMock
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
