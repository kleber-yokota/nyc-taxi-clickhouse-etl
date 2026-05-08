"""Tests for recover_from_s3 — reconstruct UploadState from S3 objects."""

from __future__ import annotations

from pathlib import Path

import pytest

from upload.core.engine import recover_from_s3
from upload.core.state import UploadState
from upload.tests.fake_s3 import FakeS3Client


class TestRecoverFromS3PopulatesState:
    """Test: recover_from_s3_populates_state — S3 list → UploadState entries."""

    def test_recover_populates_state(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        # Pre-populate S3 with objects
        client.objects["data/yellow/trip.parquet"] = b"fake"

        state = recover_from_s3(tmp_path, client)

        assert isinstance(state, UploadState)
        assert len(state.get_entries()) == 1
        key = str(tmp_path / "yellow" / "trip.parquet")
        assert key in state._data


class TestRecoverFromS3StripsPrefix:
    """Test: recover_from_s3_strips_prefix — s3_key with prefix → rel_path without prefix."""

    def test_prefix_stripped_correctly(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        client.objects["data/yellow/trip.parquet"] = b"fake"
        client.objects["data/green/trip.parquet"] = b"fake"

        state = recover_from_s3(tmp_path, client)

        entries = state.get_entries()
        assert str(tmp_path / "yellow" / "trip.parquet") in entries
        assert str(tmp_path / "green" / "trip.parquet") in entries
        # s3_key should retain the full key with prefix
        assert entries[str(tmp_path / "yellow" / "trip.parquet")]["s3_key"] == "data/yellow/trip.parquet"


class TestRecoverFromS3ReadsChecksumMetadata:
    """Test: recover_from_s3_reads_checksum_metadata — head_object returns checksum in metadata."""

    def test_checksum_from_metadata(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        client.objects["data/yellow/trip.parquet"] = b"fake"
        client.set_metadata("data/yellow/trip.parquet", {"checksum": "abc123def456"})

        state = recover_from_s3(tmp_path, client)

        key = str(tmp_path / "yellow" / "trip.parquet")
        assert state._data[key]["checksum"] == "abc123def456"


class TestRecoverFromS3EmptyBucket:
    """Test: recover_from_s3_empty_bucket — empty bucket → empty state."""

    def test_empty_bucket(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")

        state = recover_from_s3(tmp_path, client)

        assert isinstance(state, UploadState)
        assert len(state.get_entries()) == 0


class TestRecoverFromS3NoChecksumInMetadata:
    """Test: recover_from_s3_no_checksum_in_metadata — head_object without checksum → checksum='''."""

    def test_no_checksum_field(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        client.objects["data/yellow/trip.parquet"] = b"fake"
        # head_object returns no Metadata key

        state = recover_from_s3(tmp_path, client)

        key = str(tmp_path / "yellow" / "trip.parquet")
        assert state._data[key]["checksum"] == ""


class TestRecoverFromS3MultipleFiles:
    """Test: recover_from_s3_multiple_files — multiple files → multiple entries."""

    def test_multiple_files(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        files = [
            "data/yellow/a.parquet",
            "data/yellow/b.parquet",
            "data/green/c.parquet",
            "data/fhv/d.parquet",
        ]
        for f in files:
            client.objects[f] = b"fake"

        state = recover_from_s3(tmp_path, client)

        assert len(state.get_entries()) == 4
        for f in files:
            rel = f.replace("data/", "", 1)
            key = str(tmp_path / rel)
            assert key in state._data


class TestRecoverFromS3NestedPaths:
    """Test: recover_from_s3_nested_paths — nested paths (yellow/trip.parquet) → correct rel_path."""

    def test_nested_paths(self, tmp_path: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        client.objects["data/yellow/2024/trip.parquet"] = b"fake"
        client.objects["data/green/2023/jan/trip.parquet"] = b"fake"

        state = recover_from_s3(tmp_path, client)

        entries = state.get_entries()
        assert str(tmp_path / "yellow" / "2024" / "trip.parquet") in entries
        assert str(tmp_path / "green" / "2023" / "jan" / "trip.parquet") in entries

    def test_prefix_with_trailing_slash(self, tmp_path: Path):
        """Edge: prefix with trailing slash should still strip correctly."""
        client = FakeS3Client(bucket="b", prefix="data/")
        client.objects["data//yellow/trip.parquet"] = b"fake"

        state = recover_from_s3(tmp_path, client)

        # The replace should handle the double slash from prefix + empty prefix arg
        entries = state.get_entries()
        assert len(entries) == 1
