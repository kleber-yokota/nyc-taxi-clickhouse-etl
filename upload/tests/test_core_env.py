"""Unit tests for upload_from_env() — using monkeypatch + minimal mocks."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from upload.core.state import UploadResult, UploadConfig


class TestUploadFromEnv:
    """Tests for upload_from_env() using minimal mocking."""

    def test_raises_without_bucket(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("S3_BUCKET", raising=False)

        with pytest.raises(ValueError, match="S3_BUCKET must be set"):
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path))

    def test_uses_env_defaults(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        monkeypatch.delenv("S3_PREFIX", raising=False)
        monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path))

            client = mock_upload.call_args[1]["client"]
            assert client.bucket == "env-bucket"
            assert client.prefix == "data"
            assert client.prefix == "data"

    def test_uses_all_env_vars(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "custom/prefix")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path))

            client = mock_upload.call_args[1]["client"]
            assert client.bucket == "my-bucket"
            assert client.prefix == "custom/prefix"

    def test_arg_overrides_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        monkeypatch.setenv("S3_PREFIX", "env-prefix")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(
                str(tmp_path),
                bucket="arg-bucket",
                prefix="arg-prefix",
                config=UploadConfig(overwrite=True),
            )

            call_kwargs = mock_upload.call_args[1]
            assert call_kwargs["client"].bucket == "arg-bucket"
            assert call_kwargs["client"].prefix == "arg-prefix"
            assert call_kwargs["config"].overwrite is True

    def test_none_arg_falls_back_to_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "env-prefix")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path), bucket=None, prefix=None)

            assert mock_upload.call_args[1]["client"].bucket == "my-bucket"
            assert mock_upload.call_args[1]["client"].prefix == "env-prefix"

    def test_empty_string_arg_falls_back_to_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        monkeypatch.setenv("S3_PREFIX", "data")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path), bucket="")

            assert mock_upload.call_args[1]["client"].bucket == "env-bucket"

    def test_config_passed_to_upload(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "data")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(
                str(tmp_path),
                config=UploadConfig(
                    include={"yellow*.parquet"},
                    exclude={".upload_state.json"},
                ),
            )

            assert mock_upload.call_args[1]["config"].include == {"yellow*.parquet"}
            assert mock_upload.call_args[1]["config"].exclude == {".upload_state.json"}

    def test_state_file_path_correct(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "data")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path))

            state = mock_upload.call_args[1]["state"]
            assert state.state_path.name == ".upload_state.json"
            assert state.state_path == tmp_path / ".upload_state.json"

    def test_data_dir_passed_to_upload(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "data")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with patch("upload.core.runner.upload") as mock_upload:
            mock_upload.return_value = UploadResult()
            from upload.core.runner import upload_from_env

            upload_from_env(str(tmp_path))

            assert mock_upload.call_args[1]["data_dir"] == str(tmp_path)
