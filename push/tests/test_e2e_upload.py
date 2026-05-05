"""E2E tests for push pipeline using testcontainers MinIO."""

from __future__ import annotations

from pathlib import Path

import pytest

from push.core.state import PushState, UploadConfig


class TestPushPipeline:
    """Full push pipeline tests using real MinIO."""

    def test_push_fresh_files(self, sample_files: Path, s3_client, caplog):
        import logging
        caplog.set_level(logging.INFO)
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(sample_files, s3_client, state)

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3

    def test_push_skip_already_pushed(self, sample_files: Path, s3_client, caplog):
        import logging
        caplog.set_level(logging.INFO)
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        upload(sample_files, s3_client, state)
        result = upload(sample_files, s3_client, state)

        assert result.uploaded == 0
        assert result.skipped == 3
        assert result.total == 3

    def test_push_overwrite(self, sample_files: Path, s3_client):
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        upload(sample_files, s3_client, state)
        result = upload(sample_files, s3_client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 3
        assert result.skipped == 0

    def test_push_include_filter(self, sample_files: Path, s3_client):
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(
            sample_files, s3_client, state,
            config=UploadConfig(include={"yellow*.parquet"}),
        )

        assert result.uploaded == 2
        assert result.total == 2

    def test_push_nonexistent_dir(self, sample_files: Path, s3_client):
        s3_client.create_bucket()
        state = PushState(sample_files / ".push_state.json")

        from push.core.push import upload
        result = upload(sample_files / "nonexistent", s3_client, state)

        assert result.uploaded == 0
        assert result.total == 0


class TestPushSkip:
    """Skip behavior tests using real MinIO."""

    def test_skip_pre_recorded(self, sample_files_with_state: Path, s3_client, caplog):
        import logging
        caplog.set_level(logging.DEBUG)
        state_file = sample_files_with_state / ".push_state.json"
        assert state_file.exists()

        state = PushState(state_file)
        pre_recorded_key = state._data[
            str(sample_files_with_state / "yellow" / "yellow_tripdata_2024-01.parquet")
        ].get("s3_key")
        assert pre_recorded_key == "data/yellow/yellow_tripdata_2024-01.parquet"

        s3_client.create_bucket()

        from push.core.push import upload
        result = upload(sample_files_with_state, s3_client, state)

        assert result.uploaded == 0
        assert result.skipped == 1
        assert result.total == 1

        skip_records = [r for r in caplog.records if "Skipping" in r.message]
        assert len(skip_records) >= 1
        assert "yellow_tripdata_2024-01.parquet" in skip_records[0].message

    def test_overwrite_pre_recorded(self, sample_files_with_state: Path, s3_client):
        state_file = sample_files_with_state / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(sample_files_with_state, s3_client, state, config=UploadConfig(overwrite=True))

        assert result.uploaded == 1
        assert result.skipped == 0

    def test_push_delete_after_push(self, sample_files: Path, s3_client, caplog):
        import logging
        caplog.set_level(logging.INFO)
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(
            sample_files,
            s3_client,
            state,
            config=UploadConfig(delete_after_push=True),
        )

        assert result.uploaded == 3
        assert result.skipped == 0
        assert result.failed == 0
        assert result.total == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists(), f"File should be deleted: {parquet}"
        objects = s3_client.list_objects()
        assert len(objects) == 3

    def test_push_delete_after_push_with_overwrite(self, sample_files: Path, s3_client):
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(
            sample_files,
            s3_client,
            state,
            config=UploadConfig(delete_after_push=True, overwrite=True),
        )

        assert result.uploaded == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert not parquet.exists()

    def test_push_delete_after_push_false_keeps_files(self, sample_files: Path, s3_client):
        state_file = sample_files / ".push_state.json"
        s3_client.create_bucket()
        state = PushState(state_file)

        from push.core.push import upload
        result = upload(
            sample_files,
            s3_client,
            state,
            config=UploadConfig(delete_after_push=False),
        )

        assert result.uploaded == 3
        for parquet in sample_files.rglob("*.parquet"):
            assert parquet.exists()
