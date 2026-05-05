"""Unit tests for the interrupt module."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from extract.core.interrupt import InterruptibleDownload
from extract.core.interrupt import _handle_signal


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

    def test_cleanup_when_tmp_is_falsey(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._tmp_path = False
        interruptible.cleanup()

    def test_cleanup_when_tmp_is_zero(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._tmp_path = 0
        interruptible.cleanup()

    def test_cleanup_when_tmp_is_empty_string(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._tmp_path = ""
        interruptible.cleanup()

    def test_cleanup_when_tmp_is_empty_list(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        interruptible._tmp_path = []
        interruptible.cleanup()

    def test_context_manager_exception_type_none(self, download_dir: Path):
        interruptible = InterruptibleDownload(download_dir)
        with interruptible:
            pass
        # __exit__ with exc_type=None should not call cleanup


class TestHandleSignal:
    """Tests for _handle_signal function."""

    def test_handle_signal_logs_with_signal_number(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(2, None)

        assert any("Interrupt signal received (signal 2)" in record.message for record in caplog.records)

    def test_handle_signal_with_different_signal(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(15, None)

        assert any("Interrupt signal received (signal 15)" in record.message for record in caplog.records)

    def test_handle_signal_with_frame(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(2, object())

        assert any("Interrupt signal received (signal 2)" in record.message for record in caplog.records)


class TestCleanupEdgeCases:
    """Additional edge case tests for cleanup to kill remaining mutants."""

    def test_cleanup_path_exists_but_is_falsey(self, download_dir: Path):
        """Test that cleanup handles falsy Path-like objects."""
        interruptible = InterruptibleDownload(download_dir)
        # A Path object that doesn't exist should still be truthy
        fake_path = Path("/nonexistent/path/that/does/not/exist")
        interruptible._tmp_path = fake_path
        interruptible.cleanup()
        assert interruptible._tmp_path is None

    def test_cleanup_sets_none_after_existing_file_deletion(self, download_dir: Path):
        """Verify _tmp_path is set to None after deleting existing file."""
        download_dir.mkdir(parents=True, exist_ok=True)
        interruptible = InterruptibleDownload(download_dir)
        tmp_file = download_dir / "verify_deletion.tmp"
        tmp_file.write_bytes(b"data")
        interruptible._tmp_path = tmp_file

        interruptible.cleanup()

        assert not tmp_file.exists()
        assert interruptible._tmp_path is None

    def test_cleanup_nonexistent_file_sets_none(self, download_dir: Path):
        """Verify _tmp_path is set to None even when file doesn't exist."""
        interruptible = InterruptibleDownload(download_dir)
        nonexistent = download_dir / "does_not_exist.tmp"
        interruptible._tmp_path = nonexistent

        interruptible.cleanup()

        assert interruptible._tmp_path is None
