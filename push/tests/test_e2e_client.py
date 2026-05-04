"""E2E tests for S3Client methods using testcontainers MinIO."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from push.core.client import S3Client


class TestS3ClientMethods:
    """Individual tests for each S3Client method using real MinIO."""

    def test_put_and_head_object(self, s3_client):
        s3_client.create_bucket()
        test_key = "data/test.parquet"
        test_data = b"integration_test_data_1234567890"

        s3_client.put_object(test_key, test_data)
        head = s3_client.head_object(test_key)

        assert head is not None
        assert head["ContentLength"] == len(test_data)

    def test_head_object_not_found(self, s3_client):
        s3_client.create_bucket()
        result = s3_client.head_object("data/nonexistent.parquet")
        assert result is None

    def test_list_objects_empty(self, s3_client):
        s3_client.create_bucket()
        objects = s3_client.list_objects("data")
        assert objects == []

    def test_list_objects_with_files(self, s3_client):
        s3_client.create_bucket()
        s3_client.put_object("data/unique_f1.parquet", b"data1")
        s3_client.put_object("data/unique_f2.parquet", b"data2")
        s3_client.put_object("data/sub/unique_f3.parquet", b"data3")

        objects = s3_client.list_objects("")
        assert len(objects) == 4
        keys = set(objects)
        assert "data/unique_f1.parquet" in keys
        assert "data/unique_f2.parquet" in keys
        assert "data/sub/unique_f3.parquet" in keys

    def test_delete_object(self, s3_client):
        s3_client.create_bucket()
        s3_client.put_object("data/to_delete.parquet", b"data")

        assert s3_client.head_object("data/to_delete.parquet") is not None
        s3_client.delete_object("data/to_delete.parquet")
        assert s3_client.head_object("data/to_delete.parquet") is None

    def test_create_bucket_exists(self, s3_client):
        s3_client.create_bucket()
        s3_client.create_bucket()

    def test_build_key_with_prefix(self, s3_client):
        key = s3_client.build_key("file.parquet")
        assert key == "data/file.parquet"

    def test_build_key_without_prefix(self):
        client = S3Client.from_env(bucket="test", prefix="")
        key = client.build_key("file.parquet")
        assert key == "file.parquet"
