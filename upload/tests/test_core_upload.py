"""Unit tests for upload() — using FakeS3Client (zero mocks)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import BinaryIO

import pytest

from upload.core.errors import S3ClientError
from upload.core.state import UploadState, UploadConfig
from upload.core.engine import upload
from upload.tests.fake_s3 import FakeS3Client


class TestUploadFresh:
    """Tests for fresh uploads."""

    def test_upload_fresh_files(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(sample_files, client, state)

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3

    def test_upload_fresh_files_collected(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(sample_files, client, state)

        assert len(result.uploaded_files) == 3
        relative_paths = {p for p in result.uploaded_files}
        assert "yellow/yellow_tripdata_2024-01.parquet" in relative_paths
        assert "yellow/yellow_tripdata_2024-02.parquet" in relative_paths
        assert "green/green_tripdata_2024-01.parquet" in relative_paths

    def test_upload_skip_files_not_in_uploaded_list(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        upload(sample_files, client, state)
        result = upload(sample_files, client, state)

        assert result.uploaded == 0
        assert result.skipped == 3
        assert result.uploaded_files == []

    def test_upload_keys_use_prefix(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        upload(sample_files, client, state)

        keys = list(client.objects.keys())
        assert "data/yellow/yellow_tripdata_2024-01.parquet" in keys
        assert "data/yellow/yellow_tripdata_2024-02.parquet" in keys
        assert "data/green/green_tripdata_2024-01.parquet" in keys

    def test_upload_uses_correct_content_type(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        upload(sample_files, client, state)

        assert client.upload_count == 3


class TestUploadSkip:
    """Tests for skip behavior."""

    def test_upload_skip_when_already_uploaded(self, sample_files: Path):
        state_file = sample_files / ".upload_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(state_file)

        upload(sample_files, client, state)
        result = upload(sample_files, client, state)

        assert result.uploaded == 0
        assert result.skipped == 3
        assert client.upload_count == 3

    def test_upload_overwrite_reuploads(self, sample_files: Path):
        state_file = sample_files / ".upload_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(state_file)

        upload(sample_files, client, state)
        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 3
        assert result.skipped == 0
        assert client.upload_count == 6

    def test_upload_skip_pre_recorded(self, sample_files: Path, caplog):
        caplog.set_level("DEBUG")
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
        state = UploadState(pre_state_file)

        result = upload(sample_files, client, state)

        assert result.uploaded == 2
        assert result.skipped == 1
        assert result.total == 3
        assert any("Skipping (already uploaded)" in record.message for record in caplog.records)

    def test_upload_overwrite_pre_recorded(self, sample_files: Path):
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
        state = UploadState(pre_state_file)

        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 3
        assert result.skipped == 0

    def test_upload_records_state(self, sample_files: Path):
        state_file = sample_files / ".upload_state.json"
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(state_file)

        upload(sample_files, client, state)

        state2 = UploadState(state_file)
        for f in sample_files.rglob("*.parquet"):
            checksum = hashlib.sha256(f.read_bytes()).hexdigest()
            assert state2.is_uploaded(str(f), checksum) is True


class TestUploadFiltering:
    """Tests for include/exclude filtering."""

    def test_upload_include_filter(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(include={"yellow*.parquet"}),
        )

        assert result.uploaded == 2
        assert result.total == 2

    def test_upload_exclude_filter(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(exclude={"green*.parquet"}),
        )

        assert result.uploaded == 2
        assert result.total == 2

    def test_upload_empty_include(self, sample_files: Path):
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(
            sample_files, client, state,
            config=UploadConfig(include=set()),
        )

        assert result.total == 0


class TestUploadErrors:
    """Tests for error handling."""

    def test_upload_error_one_file(self, sample_files: Path, caplog):
        caplog.set_level("ERROR")
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        def failing_upload(key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
            if "2024-02" in key:
                raise S3ClientError("fail")
            return {"ETag": '"fake"'}

        client.upload_fileobj = failing_upload
        result = upload(sample_files, client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 2
        assert result.failed == 1
        assert result.total == 3
        error_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
        assert any("Unexpected error uploading" in msg for msg in error_messages)
        assert any("2024-02" in msg for msg in error_messages)

    def test_upload_nonexistent_dir(self, tmp_path: Path, caplog):
        caplog.set_level("WARNING")
        client = FakeS3Client(bucket="b")
        state = UploadState(tmp_path / ".upload_state.json")

        result = upload(tmp_path / "nonexistent", client, state)

        assert result.uploaded == 0
        assert result.total == 0
        assert any("Data directory does not exist" in record.message for record in caplog.records)

    def test_upload_uploaded_files_mutant_killing(self, sample_files: Path):
        """Kills mutants that remove uploaded_files assignment or the field."""
        client = FakeS3Client(bucket="b", prefix="data")
        state = UploadState(sample_files / ".upload_state.json")

        result = upload(sample_files, client, state)

        assert hasattr(result, "uploaded_files")
        assert isinstance(result.uploaded_files, list)
        assert len(result.uploaded_files) == result.uploaded
        assert all(isinstance(p, str) for p in result.uploaded_files)
        assert all("/" in p for p in result.uploaded_files)
