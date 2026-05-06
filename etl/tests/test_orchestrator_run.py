"""Tests for Orchestrator.run() argument passing and return value structure.

Each test asserts on exact argument values passed to extract_run, upload_from_env,
and the exact keys returned in the result dict. These assertions catch mutations
that change argument values, string literals, or dict keys.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.manifest import add_entry
from etl.orchestrator import ETLConfig, Orchestrator


class TestOrchestratorRunExtractArguments:
    """Tests that Orchestrator.run() passes correct arguments to extract_run."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_passes_types_to_extract(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig())
            orchestrator.run()

        assert mock_extract.call_args.kwargs["types"] == ["yellow", "green", "fhv", "fhvhv"]

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_passes_from_year_to_extract(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(from_year=2023))
            orchestrator.run()

        assert mock_extract.call_args.kwargs["from_year"] == 2023

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_passes_to_year_to_extract(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(to_year=2025))
            orchestrator.run()

        assert mock_extract.call_args.kwargs["to_year"] == 2025

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_passes_mode_to_extract(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="full"))
            orchestrator.run()

        assert mock_extract.call_args.kwargs["mode"] == "full"


class TestOrchestratorRunLoggerMessages:
    """Tests that Orchestrator.run() logs messages with correct format strings."""

    @patch("etl.orchestrator.logger")
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_logs_extract_starting_with_format_and_mode(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        mock_logger: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="incremental"))
            orchestrator.run()

        first_call = mock_logger.info.call_args_list[0]
        assert first_call[0][0] == "=== Extract starting (mode=%s) ==="

    @patch("etl.orchestrator.logger")
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_logs_extract_starting_with_correct_mode_value(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        mock_logger: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(mode="full"))
            orchestrator.run()

        first_call = mock_logger.info.call_args_list[0]
        assert first_call[0][1] == "full"

    @patch("etl.orchestrator.logger")
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_logs_extract_complete_with_format_string(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        mock_logger: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_extract.return_value = {"downloaded": 1, "skipped": 0, "failed": 0, "total": 1}
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        second_call = mock_logger.info.call_args_list[1]
        assert second_call[0][0] == "Extract complete: %s"

    @patch("etl.orchestrator.logger")
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_logs_push_starting_with_format_string(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        mock_logger: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        third_call = mock_logger.info.call_args_list[2]
        assert third_call[0][0] == "=== Push starting ==="

    @patch("etl.orchestrator.logger")
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_logs_push_complete_with_format_string(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        mock_logger: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=0, total=1,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        fourth_call = mock_logger.info.call_args_list[3]
        assert fourth_call[0][0] == "Push complete: %s"


class TestOrchestratorRunLoadManifest:
    """Tests that Orchestrator.run() loads manifest with correct data_dir path."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_loads_manifest_from_data_path(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            orchestrator.run()

        mock_load.assert_called_once()
        assert mock_load.call_args[0][0] == Path("data")


class TestOrchestratorRunPushConfig:
    """Tests that Orchestrator.run() creates UploadConfig with correct attributes."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_push_config_has_delete_after_push_false(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=0, skipped=0, failed=0, total=0,
            uploaded_files=[], uploaded_checksums={},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(delete_after_push=False))
            orchestrator.run()

        config = mock_upload.call_args.kwargs["config"]
        assert hasattr(config, "delete_after_push")
        assert config.delete_after_push is False


class TestOrchestratorRunReturnDictKeys:
    """Tests that Orchestrator.run() returns dict with correct push result keys."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_push_result_contains_skipped_key(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_extract.return_value = {"downloaded": 1, "skipped": 2, "failed": 0, "total": 3}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=2, failed=0, total=3,
            uploaded_files=[], uploaded_checksums={},
        )
        mock_load.return_value = {}

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            result = orchestrator.run()

        assert "skipped" in result["push"]
        assert result["push"]["skipped"] == 2

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_push_result_contains_failed_key(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_extract.return_value = {"downloaded": 1, "skipped": 0, "failed": 1, "total": 2}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=1, total=2,
            uploaded_files=[], uploaded_checksums={},
        )
        mock_load.return_value = {}

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            result = orchestrator.run()

        assert "failed" in result["push"]
        assert result["push"]["failed"] == 1

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_push_result_contains_total_key(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_extract.return_value = {"downloaded": 1, "skipped": 0, "failed": 0, "total": 1}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=0, total=1,
            uploaded_files=[], uploaded_checksums={},
        )
        mock_load.return_value = {}

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            result = orchestrator.run()

        assert "total" in result["push"]
        assert result["push"]["total"] == 1

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_push_result_contains_uploaded_files_key(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=0, total=1,
            uploaded_files=["yellow/test.parquet"],
            uploaded_checksums={"yellow/test.parquet": "abc"},
        )
        mock_load.return_value = {}

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator()
            result = orchestrator.run()

        assert "uploaded_files" in result["push"]
        assert result["push"]["uploaded_files"] == ["yellow/test.parquet"]


class TestOrchestratorRunChecksumPropagation:
    """Tests that Orchestrator.run() passes checksum from upload result to manifest."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_add_entry_receives_checksum_from_upload_result(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        captured: list[tuple[str, str, str]] = []

        def capture_add(manifest: dict, rel_path: str, s3_key: str, checksum: str) -> None:
            captured.append((rel_path, s3_key, checksum))
            add_entry(manifest, rel_path, s3_key, checksum)

        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=0, total=1,
            uploaded_files=["yellow/test.parquet"],
            uploaded_checksums={"yellow/test.parquet": "sha256abc"},
        )

        with patch("etl.orchestrator.add_entry", side_effect=capture_add):
            with patch.dict("os.environ", {"S3_BUCKET": "test-bucket", "S3_PREFIX": "data"}):
                orchestrator = Orchestrator()
                orchestrator.run()

        assert len(captured) == 1
        rel_path, s3_key, checksum = captured[0]
        assert checksum == "sha256abc"


class TestOrchestratorRunS3PrefixDefault:
    """Tests that Orchestrator.run() uses config s3_prefix when env var is not set."""

    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.save")
    def test_run_uses_config_s3_prefix_without_env_var(
        self,
        mock_save: MagicMock,
        mock_load: MagicMock,
        mock_upload: MagicMock,
        mock_extract: MagicMock,
        tmp_data_dir: Path,
    ) -> None:
        mock_load.return_value = {}
        mock_upload.return_value = MagicMock(
            uploaded=1, skipped=0, failed=0, total=1,
            uploaded_files=["yellow/test.parquet"],
            uploaded_checksums={"yellow/test.parquet": "abc"},
        )

        with patch.dict("os.environ", {"S3_BUCKET": "test-bucket"}):
            orchestrator = Orchestrator(ETLConfig(s3_prefix="data"))
            orchestrator.run()

        manifest = mock_save.call_args[0][1]
        assert manifest["yellow/test.parquet"]["s3_key"] == "data/yellow/test.parquet"
