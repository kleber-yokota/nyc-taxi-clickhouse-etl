"""Unit tests for the interrupt module."""

from __future__ import annotations

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
