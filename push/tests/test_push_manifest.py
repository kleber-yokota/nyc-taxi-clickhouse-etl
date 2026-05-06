"""Tests for push/core/push_manifest.py — list_s3_objects and S3Object."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from push.core.push_manifest import S3Object, list_s3_objects


class TestS3Object:
    """Tests for S3Object dataclass."""

    def test_s3_object_frozen(self) -> None:
        """S3Object is immutable (frozen)."""
        obj = S3Object(key="data/file.parquet")
        with pytest.raises(Exception):
            obj.key = "new_key"  # type: ignore[misc]

    def test_s3_object_repr(self) -> None:
        """S3Object has meaningful repr."""
        obj = S3Object(key="data/yellow/file.parquet")
        assert "S3Object" in repr(obj)
        assert "data/yellow/file.parquet" in repr(obj)


class TestListS3Objects:
    """Tests for list_s3_objects function."""

    def test_rejects_empty_bucket(self) -> None:
        """Empty bucket raises ValueError."""
        with pytest.raises(ValueError, match="^S3_BUCKET must be set"):
            list_s3_objects(bucket="")

    @patch("push.core.push_manifest.S3Client.from_env")
    def test_returns_empty_list_when_no_objects(self, mock_from_env: MagicMock) -> None:
        """No objects in bucket -> empty list."""
        mock_client = MagicMock()
        mock_client.list_objects.return_value = []
        mock_from_env.return_value = mock_client

        result = list_s3_objects(bucket="test-bucket", prefix="data")
        assert result == []
        mock_client.list_objects.assert_called_once_with(prefix="data")

    @patch("push.core.push_manifest.S3Client.from_env")
    def test_returns_s3_objects_list(self, mock_from_env: MagicMock) -> None:
        """Objects in bucket -> populated S3Object list."""
        mock_client = MagicMock()
        mock_client.list_objects.return_value = [
            "data/yellow/file1.parquet",
            "data/green/file2.parquet",
        ]
        mock_from_env.return_value = mock_client

        result = list_s3_objects(bucket="test-bucket", prefix="data")
        assert len(result) == 2
        assert isinstance(result[0], S3Object)
        assert result[0].key == "data/yellow/file1.parquet"
        assert result[1].key == "data/green/file2.parquet"

    @patch("push.core.push_manifest.S3Client.from_env")
    def test_uses_custom_prefix(self, mock_from_env: MagicMock) -> None:
        """Custom prefix is passed to list_objects."""
        mock_client = MagicMock()
        mock_client.list_objects.return_value = []
        mock_from_env.return_value = mock_client

        list_s3_objects(bucket="test-bucket", prefix="custom-prefix")
        mock_client.list_objects.assert_called_once_with(prefix="custom-prefix")

    @patch("push.core.push_manifest.S3Client.from_env")
    def test_creates_client_with_bucket_and_prefix(self, mock_from_env: MagicMock) -> None:
        """S3Client.from_env is called with bucket and prefix."""
        mock_client = MagicMock()
        mock_client.list_objects.return_value = []
        mock_from_env.return_value = mock_client

        list_s3_objects(bucket="my-bucket", prefix="my-prefix")
        mock_from_env.assert_called_once_with(bucket="my-bucket", prefix="my-prefix")
