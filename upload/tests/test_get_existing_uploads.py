"""Tests for get_existing_uploads — {rel_path: {s3_key, checksum}} dict."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from upload.core.runner import get_existing_uploads
from upload.tests.fake_s3 import FakeS3Client


def _make_mock_state(tmp_path: Path) -> object:
    """Create a mock state object with get_entries method."""
    class MockState:
        def get_entries(self):
            return {
                str(tmp_path / "yellow" / "trip.parquet"): {
                    "s3_key": "data/yellow/trip.parquet",
                    "checksum": "abc123",
                }
            }
    return MockState()


class TestGetExistingUploadsReturnsDict:
    """Test: get_existing_uploads_returns_dict — returns dict."""

    def test_returns_dict_type(self, tmp_path: Path):
        with patch.dict(os.environ, {}, clear=True):
            result = get_existing_uploads(tmp_path)

        assert isinstance(result, dict)


class TestGetExistingUploadsNoBucket:
    """Test: get_existing_uploads_no_bucket — no S3_BUCKET → {}."""

    def test_no_bucket_env_var(self, tmp_path: Path):
        # Ensure S3_BUCKET is not set
        env = os.environ.copy()
        env.pop("S3_BUCKET", None)
        with patch.dict(os.environ, env, clear=True):
            result = get_existing_uploads(tmp_path)

        assert result == {}


class TestGetExistingUploadsWithBucket:
    """Test: get_existing_uploads_with_bucket — with S3_BUCKET → calls recover_from_s3."""

    def test_calls_recover_with_bucket_set(self, tmp_path: Path):
        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}):
            with patch("upload.core.runner.recover_from_s3") as mock_recover:
                mock_recover.return_value = _make_mock_state(tmp_path)

                result = get_existing_uploads(tmp_path)

                assert isinstance(result, dict)
                assert "yellow/trip.parquet" in result
                assert result["yellow/trip.parquet"]["checksum"] == "abc123"


class TestGetExistingUploadsConvertsPaths:
    """Test: get_existing_uploads_converts_paths — local_path absolute → rel_path relative."""

    def test_absolute_to_relative(self, tmp_path: Path):
        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}):
            with patch("upload.core.runner.recover_from_s3") as mock_recover:
                abs_path = str(tmp_path / "yellow" / "trip.parquet")

                class MockState:
                    def get_entries(self):
                        return {
                            abs_path: {
                                "s3_key": "data/yellow/trip.parquet",
                                "checksum": "abc123",
                            }
                        }

                mock_recover.return_value = MockState()

                result = get_existing_uploads(tmp_path)

                # Key should be relative path
                assert "yellow/trip.parquet" in result
                assert result["yellow/trip.parquet"]["s3_key"] == "data/yellow/trip.parquet"
                assert result["yellow/trip.parquet"]["checksum"] == "abc123"

    def test_multiple_files_converted(self, tmp_path: Path):
        with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}):
            with patch("upload.core.runner.recover_from_s3") as mock_recover:
                abs_path1 = str(tmp_path / "yellow" / "a.parquet")
                abs_path2 = str(tmp_path / "green" / "b.parquet")

                class MockState:
                    def get_entries(self):
                        return {
                            abs_path1: {"s3_key": "data/yellow/a.parquet", "checksum": "aaa"},
                            abs_path2: {"s3_key": "data/green/b.parquet", "checksum": "bbb"},
                        }

                mock_recover.return_value = MockState()

                result = get_existing_uploads(tmp_path)

                assert "yellow/a.parquet" in result
                assert "green/b.parquet" in result
                assert len(result) == 2

    def test_no_bucket_env_returns_empty(self, tmp_path: Path):
        """Edge: S3_BUCKET not set → returns empty dict without calling recover."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("upload.core.runner.recover_from_s3") as mock_recover:
                result = get_existing_uploads(tmp_path)

                assert result == {}
                mock_recover.assert_not_called()
