"""Unit tests for the interrupt module."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from extract.core.interrupt import InterruptibleDownload


class TestInterruptibleDownload:
    def test_initializes_with_data_dir(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        assert interruptible.data_dir == download_dir
        assert interruptible._tmp_path is None

    def test_cleanup_when_no_tmp(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible.cleanup()

    def test_cleanup_removes_tmp_file(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file

        interruptible.cleanup()

        assert not tmp_file.exists()
        assert interruptible._tmp_path is None

    def test_cleanup_skips_nonexistent(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "nonexistent.tmp"
        interruptible._tmp_path = tmp_file

        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_cleanup_tmp_alias(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file

        interruptible._cleanup_tmp()

        assert not tmp_file.exists()

    def test_context_manager_no_exception(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        with interruptible:
            pass

    def test_context_manager_with_exception(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file

        with pytest.raises(RuntimeError):
            with interruptible:
                raise RuntimeError("test")

        assert not tmp_file.exists()

    def test_setup_handlers_no_error(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._setup_handlers()

    def test_cleanup_with_none_tmp_path(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._tmp_path = None
        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_cleanup_sets_tmp_to_none(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file
        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_handle_signal_logs(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._handle_signal(2, None)

    def test_cleanup_removes_tmp_and_sets_none(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test2.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file
        interruptible.cleanup()
        assert not tmp_file.exists()
        assert interruptible._tmp_path is None

    def test_cleanup_logs_cleanup_message(self, download_dir: Path, caplog: pytest.LogCaptureFixture):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test3.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file

        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            interruptible.cleanup()

        assert any("Cleaning up interrupted download" in record.message for record in caplog.records)

    def test_cleanup_sets_tmp_to_none_after_deletion(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test4.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file
        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_cleanup_when_file_already_deleted(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test5.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file
        tmp_file.unlink()

        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_cleanup_multiple_times(self, download_dir: Path):
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "test6.tmp"
        tmp_file.write_bytes(b"temp data")
        interruptible._tmp_path = tmp_file
        interruptible.cleanup()
        assert interruptible._tmp_path is None
        interruptible.cleanup()
        assert interruptible._tmp_path is None
