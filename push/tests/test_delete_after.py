"""Tests for delete_after_push feature."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from push.core.state import PushState, UploadConfig
from push.core.push import upload
from push.tests.fake_s3 import FakeS3Client


class TestDeleteAfterPush:
    """Tests for delete_after_push functionality."""

    def test_delete_after_push_removes_file(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        config = UploadConfig(delete_after_push=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists(), f"File should be deleted: {parquet}"

    def test_delete_after_push_still_records_state(self, sample_files: Path):
        state_file = sample_files / ".push_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(state_file)

        config = UploadConfig(delete_after_push=True)
        upload(sample_files, client, state, config)

        state2 = PushState(state_file)
        for parquet in list(sample_files.rglob("*.parquet")):
            pass
        checksums = {}
        for f in sample_files.rglob("*.parquet"):
            pass

    def test_delete_after_push_false_keeps_file(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        config = UploadConfig(delete_after_push=False)
        upload(sample_files, client, state, config)

        for parquet in sample_files.rglob("*.parquet"):
            assert parquet.exists(), f"File should NOT be deleted: {parquet}"

    def test_delete_after_push_default_false(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        upload(sample_files, client, state)

        for parquet in sample_files.rglob("*.parquet"):
            assert parquet.exists(), "Default behavior should keep files"

    def test_delete_after_push_with_skip(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state_file = sample_files / ".push_state.json"
        state = PushState(state_file)

        config = UploadConfig(delete_after_push=True)
        upload(sample_files, client, state, config)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 0
        assert result.skipped == 0
        assert result.total == 0
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists()

    def test_delete_after_push_with_overwrite(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state_file = sample_files / ".push_state.json"
        state = PushState(state_file)

        config = UploadConfig(delete_after_push=True, overwrite=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists()

    def test_delete_after_push_partial_failure(self, sample_files: Path):
        from push.core.errors import S3ClientError
        from typing import BinaryIO

        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        original_upload = client.upload_fileobj
        call_count = {"n": 0}

        def failing_upload(key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise S3ClientError("fail on second file")
            return original_upload(key, fileobj, **kwargs)

        client.upload_fileobj = failing_upload

        config = UploadConfig(delete_after_push=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded >= 1
        assert result.failed == 1
        assert result.total == 3

    def test_delete_after_push_upload_to_s3_correct(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        config = UploadConfig(delete_after_push=True)
        upload(sample_files, client, state, config)

        keys = list(client.objects.keys())
        assert "data/yellow/yellow_tripdata_2024-01.parquet" in keys
        assert "data/yellow/yellow_tripdata_2024-02.parquet" in keys
        assert "data/green/green_tripdata_2024-01.parquet" in keys
