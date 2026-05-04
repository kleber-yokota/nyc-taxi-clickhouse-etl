"""Unit tests for checksum and content type — kill mutation targets."""

from __future__ import annotations

from pathlib import Path

from push.core.checksum import compute_content_type, compute_sha256


class TestComputeContentType:
    """Tests for compute_content_type — kill extension mutations."""

    def test_parquet(self, tmp_path: Path):
        f = tmp_path / "test.parquet"
        f.write_bytes(b"x")
        assert compute_content_type(f) == "application/x-parquet"

    def test_other(self, tmp_path: Path):
        f = tmp_path / "test.csv"
        f.write_bytes(b"x")
        assert compute_content_type(f) == "application/octet-stream"

    def test_uppercase(self, tmp_path: Path):
        f = tmp_path / "test.PARQUET"
        f.write_bytes(b"x")
        assert compute_content_type(f) == "application/x-parquet"

    def test_no_extension(self, tmp_path: Path):
        f = tmp_path / "README"
        f.write_bytes(b"x")
        assert compute_content_type(f) == "application/octet-stream"

    def test_json(self, tmp_path: Path):
        f = tmp_path / "test.json"
        f.write_bytes(b"x")
        assert compute_content_type(f) == "application/octet-stream"


class TestComputeSha256:
    """Tests for compute_sha256 — kill hash mutations."""

    def test_deterministic(self, tmp_path: Path):
        f = tmp_path / "data.parquet"
        f.write_bytes(b"hello")
        h1 = compute_sha256(f)
        h2 = compute_sha256(f)
        assert h1 == h2
        assert len(h1) == 64

    def test_different_content(self, tmp_path: Path):
        f1 = tmp_path / "a.parquet"
        f2 = tmp_path / "b.parquet"
        f1.write_bytes(b"x")
        f2.write_bytes(b"y")
        assert compute_sha256(f1) != compute_sha256(f2)

    def test_known_hash(self, tmp_path: Path):
        import hashlib
        f = tmp_path / "data.parquet"
        content = b"hello"
        f.write_bytes(content)
        computed = compute_sha256(f)
        expected = hashlib.sha256(content).hexdigest()
        assert computed == expected

    def test_large_content(self, tmp_path: Path):
        import hashlib as _hashlib
        f = tmp_path / "large.parquet"
        data = b"chunk_" * 1000
        f.write_bytes(data)
        h = compute_sha256(f)
        assert len(h) == 64
        assert _hashlib.sha256(data).hexdigest() == h
