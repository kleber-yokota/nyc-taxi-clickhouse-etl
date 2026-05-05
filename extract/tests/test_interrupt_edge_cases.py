"""Additional edge case tests for InterruptibleDownload.cleanup."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.core.interrupt import InterruptibleDownload


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
