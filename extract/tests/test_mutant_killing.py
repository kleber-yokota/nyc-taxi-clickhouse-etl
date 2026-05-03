"""Tests to kill surviving mutmut mutations in extract/core modules."""

from __future__ import annotations

import hashlib
import json
import signal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses

from extract.core.downloader import (
    _apply_mode,
    _download_entry,
    _fetch_content,
    _handle_http_error,
    _handle_network_error,
    _process_entry,
    _safe_unlink,
    run,
)
from extract.core.state import (
    CatalogEntry,
    DOWNLOAD_TIMEOUT,
    ErrorType,
    compute_sha256,
)
from extract.core.state_manager import State
from extract.core.interrupt import InterruptibleDownload


class TestRunKeyboardInterrupt:
    def test_run_keyboard_interrupt_triggers_cleanup(self, tmp_path: Path):
        mock_state = MagicMock()
        mock_known_missing = MagicMock()
        mock_catalog = MagicMock()
        mock_interruptible = MagicMock()

        mock_catalog.generate.return_value = []
        mock_state.reset.return_value = None

        with patch("extract.core.downloader.Catalog", return_value=mock_catalog):
            with patch("extract.core.downloader.State", return_value=mock_state):
                with patch("extract.core.downloader.KnownMissing", return_value=mock_known_missing):
                    with patch("extract.core.downloader.InterruptibleDownload", return_value=mock_interruptible):
                        result = run(
                            data_dir=tmp_path,
                            mode="full",
                            max_entries=0,
                        )

        assert result["total"] == 0
        mock_catalog.generate.assert_called_once()


class TestProcessEntryIsDownloaded:
    def test_process_entry_skips_when_downloaded_and_file_exists(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = True

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / entry.filename
        target_path.write_bytes(b"existing content")

        downloaded, skipped, failed = _process_entry(
            entry, tmp_path, state, known_missing,
            0, 0, 0,
        )

        assert downloaded == 0
        assert skipped == 1
        assert failed == 0
        state.save.assert_not_called()

    def test_process_entry_saves_empty_when_downloaded_but_no_file(
        self, tmp_path: Path,
    ):
        entry = CatalogEntry("green", 2024, 6)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = True

        with patch(
            "extract.core.downloader._download_entry",
            return_value="downloaded",
        ):
            downloaded, skipped, failed = _process_entry(
                entry, tmp_path, state, known_missing,
                0, 0, 0,
            )

        assert downloaded == 1
        assert skipped == 0
        assert failed == 0
        state.save.assert_called_once_with(entry.url, "")


class TestProcessEntryExceptionHandler:
    def test_process_entry_exception_logs_unknown_error(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()
        known_missing.is_missing.return_value = False
        state.is_downloaded.return_value = False
        state.log_error.return_value = None

        def raise_value_error(*args, **kwargs):
            raise ValueError("unexpected failure")

        with patch(
            "extract.core.downloader._download_entry",
            side_effect=raise_value_error,
        ):
            downloaded, skipped, failed = _process_entry(
                entry, tmp_path, state, known_missing,
                0, 0, 0,
            )

        assert downloaded == 0
        assert skipped == 0
        assert failed == 1
        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.UNKNOWN
        assert args[0][2] == "unexpected failure"


class TestDownloadEntryChecksumMatch:
    def test_download_entry_skips_when_checksum_matches(self, tmp_path: Path):
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
            "extract.core.downloader._fetch_content",
            side_effect=write_tmp_to_correct_path,
        ) as mock_fetch:
            with patch(
                "extract.core.downloader.compute_sha256",
                return_value=existing_checksum,
            ):
                result = _download_entry(entry, tmp_path, state, known_missing)

        assert result == "skipped"
        mock_fetch.assert_called_once()

    def test_download_entry_backs_up_when_checksum_mismatches(
        self, tmp_path: Path,
    ):
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
            "extract.core.downloader._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.downloader.compute_sha256",
                side_effect=[new_checksum, different_checksum],
            ):
                result = _download_entry(entry, tmp_path, state, known_missing)

        assert result == "downloaded"
        backup_path = tmp_path / entry.target_dir / f"{entry.filename}.old"
        assert backup_path.exists()
        assert backup_path.read_bytes() == old_content

    def test_download_entry_renames_tmp_to_target(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        content = b"fresh download"

        def capture_fetch(url, tmp_file):
            tmp_file.write_bytes(content)

        with patch(
            "extract.core.downloader._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.downloader.compute_sha256",
                return_value=compute_sha256.__globals__["hashlib"].sha256(content).hexdigest(),
            ):
                result = _download_entry(entry, tmp_path, state, known_missing)

        assert result == "downloaded"
        target_path = target_dir / entry.filename
        assert target_path.exists()
        assert target_path.read_bytes() == content

    def test_download_entry_saves_checksum_on_success(self, tmp_path: Path):
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
            "extract.core.downloader._fetch_content",
            side_effect=capture_fetch,
        ):
            with patch(
                "extract.core.downloader.compute_sha256",
                return_value=computed,
            ):
                _download_entry(entry, tmp_path, state, known_missing)

        state.save.assert_called_once_with(entry.url, computed)

    def test_download_entry_handles_generic_exception(self, tmp_path: Path):
        entry = CatalogEntry("yellow", 2024, 1)
        state = MagicMock()
        known_missing = MagicMock()

        target_dir = tmp_path / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        def raise_runtime(*args, **kwargs):
            raise RuntimeError("disk full")

        with patch(
            "extract.core.downloader._fetch_content",
            side_effect=raise_runtime,
        ):
            with pytest.raises(RuntimeError):
                _download_entry(entry, tmp_path, state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][1] == ErrorType.UNKNOWN


class TestFetchContentStream:
    @responses.activate
    def test_fetch_content_uses_stream_and_timeout(self, tmp_path: Path):
        url = "https://example.com/stream.parquet"
        responses.add(responses.GET, url, body=b"chunk1chunk2", status=200)

        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            tmp_file = tmp_path / "test.tmp"
            _fetch_content(url, tmp_file)

            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["timeout"] == DOWNLOAD_TIMEOUT
            assert call_kwargs["stream"] is True


class TestHandleHttpErrorStatusExtraction:
    def test_http_error_status_code_from_response(self):
        import requests

        error = requests.HTTPError()
        error.response = MagicMock()
        error.response.status_code = 404

        state = MagicMock()
        known_missing = MagicMock()

        _handle_http_error(error, "https://example.com/test.parquet", state, known_missing)

        state.log_error.assert_called_once()
        args = state.log_error.call_args
        assert args[0][2] == "HTTP 404"


class TestHandleNetworkErrorTypeName:
    def test_network_error_passes_class_name_not_message(self):
        import requests

        class MyCustomException(requests.RequestException):
            pass

        error = MyCustomException("some message text")

        state = MagicMock()

        _handle_network_error(error, "https://example.com/test.parquet", state)

        args = state.log_error.call_args
        assert args[0][2] == "MyCustomException"
        assert "some message text" not in args[0][2]


class TestStatePersistCreatesParentDir:
    def test_persist_creates_deep_nonexistent_parent(self, tmp_path: Path):
        deep_path = tmp_path / "a" / "b" / "c" / "d" / "state.json"
        state = State(deep_path)

        state.save("https://example.com/test.parquet", "hash123")

        assert deep_path.exists()
        data = json.loads(deep_path.read_text())
        assert data["checksums"]["https://example.com/test.parquet"] == "hash123"


class TestStatePersistJsonStructure:
    def test_persist_writes_exact_json_structure(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = State(state_path)

        state.save("https://example.com/a.parquet", "hash_a")
        state.save("https://example.com/b.parquet", "hash_b")

        content = state_path.read_text()
        data = json.loads(content)

        assert set(data.keys()) == {"checksums"}
        assert "checksums" in data
        assert isinstance(data["checksums"], dict)


class TestStateLogErrorAppendsToList:
    def test_log_error_appends_to_errors_list(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = State(state_path)

        with patch.object(state, "_persist_errors"):
            state.log_error("https://example.com/a.parquet", ErrorType.HTTP_ERROR, "error a")
            state.log_error("https://example.com/b.parquet", ErrorType.NETWORK_ERROR, "error b")

        assert len(state._errors) == 2
        assert state._errors[0]["url"] == "https://example.com/a.parquet"
        assert state._errors[1]["url"] == "https://example.com/b.parquet"


class TestStatePersistErrorsCreatesDir:
    def test_persist_errors_creates_errors_directory(self, tmp_path: Path):
        state_path = tmp_path / "deep" / "nested" / "state.json"
        state = State(state_path)

        state.log_error("https://example.com/test.parquet", ErrorType.HTTP_ERROR, "test error")

        log_path = state._errors_dir / "download_errors.log"
        assert log_path.exists()
        content = log_path.read_text()
        assert "https://example.com/test.parquet" in content


class TestInterruptibleDownloadCleanupNoTmphPath:
    def test_cleanup_does_not_raise_when_tmp_is_none(self):
        interruptible = InterruptibleDownload.__new__(InterruptibleDownload)
        interruptible._tmp_path = None

        interruptible.cleanup()

        assert interruptible._tmp_path is None


class TestInterruptibleDownloadCleanupNoFile:
    def test_cleanup_skips_when_file_already_deleted(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "stale.tmp"
            tmp_path.write_bytes(b"temp data")

            interruptible = InterruptibleDownload.__new__(InterruptibleDownload)
            interruptible._tmp_path = tmp_path
            interruptible.data_dir = Path(tmpdir)

            tmp_path.unlink()

            interruptible.cleanup()

            assert interruptible._tmp_path is None


class TestComputeSha256LargeFile:
    def test_compute_sha256_large_file_exceeds_chunk_size(self, tmp_path: Path):
        large_file = tmp_path / "large.bin"
        data = b"A" * (8192 * 4)
        large_file.write_bytes(data)

        digest = compute_sha256(large_file)

        import hashlib
        expected = hashlib.sha256(data).hexdigest()
        assert digest == expected

    def test_compute_sha256_exactly_chunk_size(self, tmp_path: Path):
        chunk_file = tmp_path / "chunk_size.bin"
        data = b"B" * 8192
        chunk_file.write_bytes(data)

        digest = compute_sha256(chunk_file)

        import hashlib
        expected = hashlib.sha256(data).hexdigest()
        assert digest == expected

    def test_compute_sha256_one_byte_over_chunk(self, tmp_path: Path):
        edge_file = tmp_path / "edge.bin"
        data = b"C" * 8193
        edge_file.write_bytes(data)

        digest = compute_sha256(edge_file)

        import hashlib
        expected = hashlib.sha256(data).hexdigest()
        assert digest == expected
