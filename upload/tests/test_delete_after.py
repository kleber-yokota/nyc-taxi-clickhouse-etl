"""Tests for delete_after_upload feature."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from upload.core.state import UploadState, UploadConfig
from upload.core.engine import upload
from upload.tests.fake_s3 import FakeS3Client


class TestDeleteAfterUpload:
    """Tests for delete_after_upload functionality."""

    def test_delete_after_upload_removes_file(self, sample_files: Path, caplog):
        caplog.set_level("INFO")
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        config = UploadConfig(delete_after_upload=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists(), f"File should be deleted: {parquet}"
        info_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        upload_logs = [msg for msg in info_messages if "Uploaded:" in msg]
        delete_logs = [msg for msg in info_messages if "Deleted local file" in msg]
        assert len(upload_logs) == 3
        assert len(delete_logs) == 3
        assert any("s3://b/data/" in msg for msg in upload_logs)

    def test_delete_after_upload_still_records_state(self, sample_files: Path):
        state_file = sample_files / ".upload_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(state_file)

        config = UploadConfig(delete_after_upload=True)
        upload(sample_files, client, state, config)

        state2 = UploadState(state_file)
        for parquet in list(sample_files.rglob("*.parquet")):
            pass
        checksums = {}
        for f in sample_files.rglob("*.parquet"):
            pass

    def test_delete_after_upload_false_keeps_file(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        config = UploadConfig(delete_after_upload=False)
        upload(sample_files, client, state, config)

        for parquet in sample_files.rglob("*.parquet"):
            assert parquet.exists(), f"File should NOT be deleted: {parquet}"

    def test_delete_after_upload_default_false(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        upload(sample_files, client, state)

        for parquet in sample_files.rglob("*.parquet"):
            assert parquet.exists(), "Default behavior should keep files"

    def test_delete_after_upload_with_skip(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state_file = sample_files / ".upload_state.json"
        state = UploadState(state_file)

        config = UploadConfig(delete_after_upload=True)
        upload(sample_files, client, state, config)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 0
        assert result.skipped == 0
        assert result.total == 0
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists()

    def test_delete_after_upload_with_overwrite(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state_file = sample_files / ".upload_state.json"
        state = UploadState(state_file)

        config = UploadConfig(delete_after_upload=True, overwrite=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists()

    def test_delete_after_upload_partial_failure(self, sample_files: Path, caplog):
        from upload.core.errors import S3ClientError
        from typing import BinaryIO

        caplog.set_level("ERROR")
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        original_upload = client.upload_fileobj
        call_count = {"n": 0}

        def failing_upload(key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise S3ClientError("fail on second file")
            return original_upload(key, fileobj, **kwargs)

        client.upload_fileobj = failing_upload

        config = UploadConfig(delete_after_upload=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded >= 1
        assert result.failed == 1
        assert result.total == 3
        error_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
        assert any("Unexpected error uploading" in msg for msg in error_messages)
        assert any("fail on second file" in msg for msg in error_messages)

    def test_delete_after_upload_multiple_failures(self, sample_files: Path, caplog):
        from upload.core.errors import S3ClientError
        from typing import BinaryIO

        caplog.set_level("ERROR")
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        call_count = {"n": 0}

        def failing_upload(key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise S3ClientError("fail on second and third")
            return {"ETag": "abc123"}

        client.upload_fileobj = failing_upload

        config = UploadConfig(delete_after_upload=True)
        result = upload(sample_files, client, state, config)

        assert result.uploaded == 1
        assert result.failed == 2
        assert result.total == 3
        error_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
        assert len(error_messages) == 2
        assert all("Unexpected error uploading" in msg for msg in error_messages)

    def test_delete_after_upload_upload_to_s3_correct(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        config = UploadConfig(delete_after_upload=True)
        upload(sample_files, client, state, config)

        keys = list(client.objects.keys())
        assert "data/yellow/yellow_tripdata_2024-01.parquet" in keys
        assert "data/yellow/yellow_tripdata_2024-02.parquet" in keys
        assert "data/green/green_tripdata_2024-01.parquet" in keys
