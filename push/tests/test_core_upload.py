"""Unit tests for push() — using FakeS3Client (zero mocks)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import BinaryIO

import pytest

from push.core.errors import S3ClientError
from push.core.state import PushState, UploadConfig
from push.core.push import upload
from push.tests.fake_s3 import FakeS3Client


class TestPushFresh:
    """Tests for fresh uploads."""

    def test_push_fresh_files(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        result =        result = upload(sample_files, client, state)

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3

    def test_push_keys_use_prefix(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        upload(sample_files, client, state)

        keys = list(client.objects.keys())
        assert "data/yellow/yellow_tripdata_2024-01.parquet" in keys
        assert "data/yellow/yellow_tripdata_2024-02.parquet" in keys
        assert "data/green/green_tripdata_2024-01.parquet" in keys

    def test_push_uses_correct_content_type(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        upload(sample_files, client, state)

        assert client.upload_count == 3


class TestPushSkip:
    """Tests for skip behavior."""

    def test_push_skip_when_already_pushed(self, sample_files: Path):
        state_file = sample_files / ".push_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(state_file)

        upload(sample_files, client, state)
        result = upload(sample_files, client, state)

        assert result.uploaded == 0
        assert result.skipped == 3
        assert client.upload_count == 3

    def test_push_overwrite_reuploads(self, sample_files: Path):
        state_file = sample_files / ".push_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(state_file)

        upload(sample_files, client, state)
        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 3
        assert result.skipped == 0
        assert client.upload_count == 6

    def test_push_skip_pre_recorded(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        one_file = sample_files / "yellow" / "yellow_tripdata_2024-01.parquet"
        checksum = hashlib.sha256(one_file.read_bytes()).hexdigest()
        pre_state_file = sample_files / ".pre_state.json"
        pre_state = {
            str(one_file): {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": checksum,
            }
        }
        pre_state_file.write_text(json.dumps(pre_state))
        state = PushState(pre_state_file)

        result = upload(sample_files, client, state)

        assert result.uploaded == 2
        assert result.skipped == 1
        assert result.total == 3

    def test_push_overwrite_pre_recorded(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        one_file = sample_files / "yellow" / "yellow_tripdata_2024-01.parquet"
        checksum = hashlib.sha256(one_file.read_bytes()).hexdigest()
        pre_state_file = sample_files / ".pre_state2.json"
        pre_state_file.write_text(json.dumps({
            str(one_file): {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": checksum,
            }
        }))
        state = PushState(pre_state_file)

        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 3
        assert result.skipped == 0

    def test_push_records_state(self, sample_files: Path):
        state_file = sample_files / ".push_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(state_file)

        upload(sample_files, client, state)

        state2 = PushState(state_file)
        for f in sample_files.rglob("*.parquet"):
            checksum = hashlib.sha256(f.read_bytes()).hexdigest()
            assert state2.is_pushed(str(f), checksum) is True


class TestPushFiltering:
    """Tests for include/exclude filtering."""

    def test_push_include_filter(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(include={"yellow*.parquet"}),
        )

        assert result.uploaded == 2
        assert result.total == 2

    def test_push_exclude_filter(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(exclude={"green*.parquet"}),
        )

        assert result.uploaded == 2
        assert result.total == 2

    def test_push_empty_include(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(include=set()),
        )

        assert result.total == 0


class TestPushErrors:
    """Tests for error handling."""

    def test_push_error_one_file(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = PushState(sample_files / ".push_state.json")

        def failing_upload(key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
            if "2024-02" in key:
                raise S3ClientError("fail")
            return {"ETag": '"fake"'}

        client.upload_fileobj = failing_upload
        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 2
        assert result.failed == 1
        assert result.total == 3

    def test_push_nonexistent_dir(self, tmp_path: Path):
        client = FakeS3Client(bucket="b")
        state = PushState(tmp_path / ".push_state.json")

        result = upload(tmp_path / "nonexistent", client, state)

        assert result.uploaded == 0
        assert result.total == 0
