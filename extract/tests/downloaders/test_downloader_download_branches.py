"""Tests for _fetch_content function in download module."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import responses as responses_lib

from extract.downloader.download import _fetch_content


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
