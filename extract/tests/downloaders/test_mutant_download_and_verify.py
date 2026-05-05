"""Tests to kill surviving mutmut mutations in download_and_verify."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.downloader.downloader import download_and_verify
from extract.core.state import CatalogEntry, ErrorType, compute_sha256


class TestDownloadAndVerifySkipsOnChecksumMatch:
    """Kills mutant: 'if checksum == existing' condition removed."""

    def test_skips_download_when_checksum_matches(self, tmp_path: Path):
        """Existing file with matching checksum → skip, no re-download."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / entry.filename
        content = b"test parquet data"
        target_path.write_bytes(content)

        existing_checksum = compute_sha256(target_path)

        def write_tmp_to_correct_path(url, tmp_file):
            tmp_file.write_bytes(content)

        with patch(
            "extract.downloader.downloader_download._fetch_content",
            side_effect=write_tmp_to_correct_path,
        ) as mock_fetch:
            with patch(
                "extract.core.state.compute_sha256",
                return_value=existing_checksum,
            ):
                result = download_and_verify(entry, tmp_path, state)

        assert result == "skipped"
        mock_fetch.assert_called_once()


class TestDownloadAndVerifyBacksUpOnMismatch:
    """Kills mutant: 'if checksum != existing' condition removed."""

    def test_backs_up_old_file_when_checksum_mismatches(self, tmp_path: Path):
        """New download with different checksum → backup old file, save new."""
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / entry.filename
        old_content = b"old data"
        target_path.write_bytes(old_content)
        old_checksum = compute_sha256(target_path)
        state.get_checksum.return_value = old_checksum

        new_content = b"new data"
        new_checksum = hashlib.sha256(new_content).hexdigest()
        different_checksum = hashlib.sha256(b"different").hexdigest()

        def capture_fetch(url, tmp_file):
            tmp_file.write_bytes(new_content)

        with patch(
            "extract.downloader.downloader_download._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.state.compute_sha256",
                side_effect=[new_checksum, different_checksum],
            ):
                result = download_and_verify(entry, tmp_path, state)

        assert result == "downloaded"
        backup_path = tmp_path / entry.target_dir / f"{entry.filename}.old"
        assert backup_path.exists()
        assert backup_path.read_bytes() == old_content


class TestDownloadAndVerifyRenamesTmp:
    """Kills mutant: 'os.rename' replaced with pass."""

    def test_renames_tmp_file_to_target(self, tmp_path: Path):
        """Successful download → tmp renamed to final target path."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        content = b"fresh download"

        def capture_fetch(url, tmp_file):
            tmp_file.write_bytes(content)

        with patch(
            "extract.downloader.downloader_download._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.state.compute_sha256",
                return_value=compute_sha256.__globals__["hashlib"].sha256(content).hexdigest(),
            ):
                result = download_and_verify(entry, tmp_path, state)

        assert result == "downloaded"
        target_path = target_dir / entry.filename
        assert target_path.exists()
        assert target_path.read_bytes() == content


class TestDownloadAndVerifySavesChecksum:
    """Kills mutant: 'state.save' replaced with pass."""

    def test_saves_checksum_on_success(self, tmp_path: Path):
        """Successful download → checksum persisted to state."""
        entry = CatalogEntry("fhv", 2024, 3)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        content = b"fhv data"

        def capture_fetch(url, tmp_file):
            tmp_file.write_bytes(content)

        computed = compute_sha256.__globals__["hashlib"].sha256(content).hexdigest()

        with patch(
            "extract.downloader.downloader_download._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.state.compute_sha256",
                return_value=computed,
            ):
                download_and_verify(entry, tmp_path, state)

        state.save.assert_called_once_with(entry.url, computed)


class TestDownloadAndVerifyRethrowsException:
    """Kills mutant: exception handler body replaced with pass."""

    def test_rethrows_unexpected_exception(self, tmp_path: Path):
        """Unexpected error → log and rethrow."""
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        with patch(
            "extract.downloader.downloader_download._fetch_content",
            side_effect=RuntimeError("disk full"),
        ):
            with pytest.raises(RuntimeError):
                download_and_verify(entry, tmp_path, state)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.UNKNOWN
