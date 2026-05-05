"""Unit tests for compute_sha256 function."""

from __future__ import annotations

from pathlib import Path

from extract.core.state import compute_sha256


class TestComputeSha256:
    def test_compute_sha256_known_content(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(b"hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert compute_sha256(test_file) == expected

    def test_compute_sha256_empty_file(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "empty.bin"
        test_file.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_sha256(test_file) == expected

    def test_compute_sha256_large_file(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "large.bin"
        content = b"x" * 100000
        test_file.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert compute_sha256(test_file) == expected
