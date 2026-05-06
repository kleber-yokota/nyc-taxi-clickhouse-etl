"""Tests for etl.orchestrator — ETLConfig and Orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.orchestrator import ETLConfig, Orchestrator


class TestETLConfig:
    """Tests for ETLConfig dataclass."""

    def test_default_values(self) -> None:
        config = ETLConfig()
        assert config.types == ["yellow", "green", "fhv", "fhvhv"]
        assert config.from_year == 2024
        assert config.to_year == 2024
        assert config.mode == "incremental"
        assert config.delete_after_push is True
        assert config.s3_prefix == "data"

    def test_custom_values(self) -> None:
        config = ETLConfig(
            types=["yellow"],
            from_year=2023,
            to_year=2024,
            mode="full",
            delete_after_push=False,
            s3_prefix="custom-prefix",
        )
        assert config.types == ["yellow"]
        assert config.from_year == 2023
        assert config.to_year == 2024
        assert config.mode == "full"
        assert config.delete_after_push is False
        assert config.s3_prefix == "custom-prefix"

    def test_is_frozen(self) -> None:
        config = ETLConfig()
        with pytest.raises(AttributeError):
            config.mode = "full"


class TestOrchestrator:
    """Tests for Orchestrator class."""

    def test_default_config(self) -> None:
        orchestrator = Orchestrator()
        assert orchestrator.config.mode == "incremental"

    def test_custom_config(self) -> None:
        config = ETLConfig(mode="full")
        orchestrator = Orchestrator(config)
        assert orchestrator.config.mode == "full"

    def test_config_none_uses_default(self) -> None:
        orchestrator = Orchestrator(None)
        assert isinstance(orchestrator.config, ETLConfig)

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_returns_result_dict(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_extract.return_value = {"downloaded": 5, "skipped": 2, "failed": 0, "total": 7}
        mock_upload.return_value = MagicMock(
            uploaded=3,
            skipped=2,
            failed=0,
            total=5,
            uploaded_files=["yellow/file.parquet", "green/file.parquet"],
            uploaded_checksums={
                "yellow/file.parquet": "abc123",
                "green/file.parquet": "def456",
            },
        )
        mock_load.return_value = {}

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="incremental"))
            result = orchestrator.run()

        assert "extract" in result
        assert "push" in result
        assert result["extract"]["downloaded"] == 5
        assert result["push"]["uploaded"] == 3

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_calls_extract_with_manifest(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        existing_manifest = {
            "yellow/old.parquet": {
                "s3_key": "data/yellow/old.parquet",
                "checksum": "old",
            }
        }
        mock_load.return_value = existing_manifest
        mock_upload.return_value = MagicMock(
            uploaded=1,
            skipped=0,
            failed=0,
            total=1,
            uploaded_files=["yellow/new.parquet"],
            uploaded_checksums={"yellow/new.parquet": "new123"},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        call_args = mock_extract.call_args
        assert call_args.kwargs["push_manifest"] is existing_manifest

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_updates_manifest_with_uploaded_files(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=2,
            skipped=0,
            failed=0,
            total=2,
            uploaded_files=["yellow/a.parquet", "green/b.parquet"],
            uploaded_checksums={
                "yellow/a.parquet": "hash_a",
                "green/b.parquet": "hash_b",
            },
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket", "S3_PREFIX": "data"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        manifest_arg = mock_save.call_args[0][1]
        assert "yellow/a.parquet" in manifest_arg
        assert "green/b.parquet" in manifest_arg
        assert manifest_arg["yellow/a.parquet"]["s3_key"] == "data/yellow/a.parquet"
        assert manifest_arg["yellow/a.parquet"]["checksum"] == "hash_a"
        assert manifest_arg["green/b.parquet"]["s3_key"] == "data/green/b.parquet"
        assert manifest_arg["green/b.parquet"]["checksum"] == "hash_b"

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_uses_env_s3_prefix(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=1,
            skipped=0,
            failed=0,
            total=1,
            uploaded_files=["yellow/file.parquet"],
            uploaded_checksums={"yellow/file.parquet": "abc"},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket", "S3_PREFIX": "custom-prefix"}):
            orchestrator = Orchestrator(ETLConfig(s3_prefix="data"))
            orchestrator.run()

        manifest_arg = mock_save.call_args[0][1]
        assert manifest_arg["yellow/file.parquet"]["s3_key"] == "custom-prefix/yellow/file.parquet"

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_full_mode_sets_overwrite(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0,
            skipped=0,
            failed=0,
            total=0,
            uploaded_files=[],
            uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="full"))
            orchestrator.run()

        upload_call = mock_upload.call_args
        assert upload_call.kwargs["config"].overwrite is True

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_incremental_mode_no_overwrite(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0,
            skipped=0,
            failed=0,
            total=0,
            uploaded_files=[],
            uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="incremental"))
            orchestrator.run()

        upload_call = mock_upload.call_args
        assert upload_call.kwargs["config"].overwrite is False

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_passes_delete_after_push(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0,
            skipped=0,
            failed=0,
            total=0,
            uploaded_files=[],
            uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(delete_after_push=False))
            orchestrator.run()

        upload_call = mock_upload.call_args
        assert upload_call.kwargs["config"].delete_after_push is False

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_empty_push_does_not_crash(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0,
            skipped=0,
            failed=0,
            total=0,
            uploaded_files=[],
            uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            result = orchestrator.run()

        assert result["push"]["uploaded"] == 0
        mock_save.assert_called_once()
