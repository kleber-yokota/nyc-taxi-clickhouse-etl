"""Tests for state module."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from extract.core.state import CatalogEntry, ErrorType, build_url, compute_sha256


class TestComputeSha256:
    """Tests for compute_sha256 function."""

    def test_computes_correct_hash(self, tmp_path: Path):
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test content for hashing")

        result = compute_sha256(test_file)
        expected = hashlib.sha256(b"test content for hashing").hexdigest()
        assert result == expected

    def test_computes_hash_empty_file(self, tmp_path: Path):
        test_file = tmp_path / "empty.parquet"
        test_file.write_bytes(b"")

        result = compute_sha256(test_file)
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected

    def test_computes_hash_large_file(self, tmp_path: Path):
        test_file = tmp_path / "large.parquet"
        large_content = b"x" * 100000
        test_file.write_bytes(large_content)

        result = compute_sha256(test_file)
        expected = hashlib.sha256(large_content).hexdigest()
        assert result == expected

    def test_computes_hash_binary_content(self, tmp_path: Path):
        test_file = tmp_path / "binary.parquet"
        binary_content = bytes(range(256))
        test_file.write_bytes(binary_content)

        result = compute_sha256(test_file)
        expected = hashlib.sha256(binary_content).hexdigest()
        assert result == expected

    def test_computes_hash_special_characters(self, tmp_path: Path):
        test_file = tmp_path / "special.parquet"
        special_content = "Hello \n World \t !@#$%^&*()".encode("utf-8")
        test_file.write_bytes(special_content)

        result = compute_sha256(test_file)
        expected = hashlib.sha256(special_content).hexdigest()
        assert result == expected

    def test_hash_is_deterministic(self, tmp_path: Path):
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"deterministic content")

        result1 = compute_sha256(test_file)
        result2 = compute_sha256(test_file)
        assert result1 == result2

    def test_different_content_different_hash(self, tmp_path: Path):
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"
        file1.write_bytes(b"content one")
        file2.write_bytes(b"content two")

        hash1 = compute_sha256(file1)
        hash2 = compute_sha256(file2)
        assert hash1 != hash2

    def test_compute_sha256_known_content(self, tmp_path: Path):
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(b"hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert compute_sha256(test_file) == expected


class TestBuildUrl:
    """Tests for build_url function."""

    def test_builds_yellow_url(self):
        url = build_url("yellow", 2024, 1)
        assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    def test_builds_green_url(self):
        url = build_url("green", 2023, 12)
        assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-12.parquet"

    def test_builds_fhv_url(self):
        url = build_url("fhv", 2022, 6)
        assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-06.parquet"

    def test_builds_fhvhv_url(self):
        url = build_url("fhvhv", 2021, 3)
        assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-03.parquet"

    def test_pads_month_with_zero(self):
        url = build_url("yellow", 2024, 1)
        assert "2024-01" in url

    def test_no_pad_month_double_digit(self):
        url = build_url("yellow", 2024, 10)
        assert "2024-10" in url


class TestCatalogEntry:
    """Tests for CatalogEntry dataclass."""

    def test_url_property(self):
        entry = CatalogEntry("yellow", 2024, 1)
        assert entry.url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    def test_filename_property(self):
        entry = CatalogEntry("yellow", 2024, 1)
        assert entry.filename == "yellow_tripdata_2024-01.parquet"

    def test_target_dir_property(self):
        entry = CatalogEntry("green", 2023, 6)
        assert entry.target_dir == "green"

    def test_frozen_dataclass(self):
        entry = CatalogEntry("yellow", 2024, 1)
        with pytest.raises(Exception):
            entry.data_type = "green"

    def test_equality(self):
        entry1 = CatalogEntry("yellow", 2024, 1)
        entry2 = CatalogEntry("yellow", 2024, 1)
        entry3 = CatalogEntry("yellow", 2024, 2)

        assert entry1 == entry2
        assert entry1 != entry3

    def test_hashable(self):
        entry = CatalogEntry("yellow", 2024, 1)
        assert hash(entry) is not None
        assert {entry} == {entry}


class TestErrorType:
    """Tests for ErrorType enum."""

    def test_all_values(self):
        assert ErrorType.MISSING_FILE.value == "missing_file"
        assert ErrorType.NETWORK_ERROR.value == "network_error"
        assert ErrorType.HTTP_ERROR.value == "http_error"
        assert ErrorType.CHECKSUM_MISMATCH.value == "checksum_mismatch"
        assert ErrorType.CORRUPT_FILE.value == "corrupt_file"
        assert ErrorType.UNKNOWN.value == "unknown"

    def test_iteration(self):
        assert len(list(ErrorType)) == 6

    def test_membership(self):
        assert ErrorType.MISSING_FILE in ErrorType
        assert ErrorType.UNKNOWN in ErrorType
