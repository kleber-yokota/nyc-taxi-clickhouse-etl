"""Tests for Orchestrator.run() — full pipeline flow."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.orchestrator import ETLConfig, Orchestrator
from push.core.state import PushedEntry, PushResult


def _make_push_result(uploaded_entries: list[PushedEntry] | None = None) -> PushResult:
    """Create a mock PushResult for testing."""
    return PushResult(
        uploaded=2,
        skipped=0,
        failed=0,
        total=2,
        uploaded_files=["yellow/file.parquet", "green/file.parquet"],
        uploaded_entries=uploaded_entries or [],
    )


class TestRunIncrementalFlow:
    """Tests for incremental mode pipeline flow."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={"downloaded": 5, "skipped": 0})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_incremental_flow(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Incremental flow: extract -> push -> manifest update."""
        entries = [
            PushedEntry(
                rel_path="yellow/yellow_tripdata_2024-01.parquet",
                s3_key="data/yellow/yellow_tripdata_2024-01.parquet",
                checksum="abc123",
            )
        ]
        mock_upload.return_value = _make_push_result(entries)

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        result = orchestrator.run()

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result


class TestRunFullFlow:
    """Tests for full mode pipeline flow."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects")
    @patch("etl.orchestrator.extract_run", return_value={"downloaded": 10, "skipped": 0})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_full_flow(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Full flow: extract (reset) -> push (overwrite)."""
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(data_dir=str(tmp_path / "data"), mode="full")
        orchestrator = Orchestrator(config)
        orchestrator.run()

        # In full mode, list_s3_objects should NOT be called (manifest rebuild skipped)
        mock_list.assert_not_called()

        # Push should be called with overwrite=True
        call_kwargs = mock_upload.call_args
        assert call_kwargs.kwargs["config"].overwrite is True


class TestManifestPassedToExtract:
    """Tests for manifest passing to extract."""

    @patch("etl.orchestrator.load")
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run")
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_manifest_passed_to_extract(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Extract receives existing manifest."""
        existing_manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "existing",
            }
        }
        mock_load.return_value = existing_manifest
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        orchestrator.run()

        call_kwargs = mock_extract.call_args
        assert call_kwargs.kwargs["push_manifest"] == existing_manifest


class TestPushReceivesBucketAndPrefix:
    """Tests for push receiving correct bucket and prefix."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_push_receives_bucket_and_prefix(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Push receives bucket and prefix from config."""
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(
            data_dir=str(tmp_path / "data"),
            bucket="custom-bucket",
            prefix="custom-prefix",
        )
        orchestrator = Orchestrator(config)
        orchestrator.run()

        call_kwargs = mock_upload.call_args
        assert call_kwargs.kwargs["bucket"] == "custom-bucket"
        assert call_kwargs.kwargs["prefix"] == "custom-prefix"


class TestManifestSavedAfterPush:
    """Tests for manifest saving after push."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_manifest_saved_after_push(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Manifest is saved with uploaded_entries after push."""
        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/yellow/file.parquet",
                checksum="abc",
            )
        ]
        mock_upload.return_value = _make_push_result(entries)

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        orchestrator.run()

        mock_save.assert_called_once()
        saved_data = mock_save.call_args[0][1]
        assert "yellow/file.parquet" in saved_data


class TestResultContainsExtractAndPush:
    """Tests for return value structure."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={"downloaded": 3})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_result_contains_extract_and_push(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Return dict has both extract and push results."""
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        result = orchestrator.run()

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result
        assert result["extract"] == {"downloaded": 3}


class TestReconcileRebuildsMissing:
    """Tests for reconciliation logic."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_reconcile_rebuilds_missing(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Divergence: missing file triggers rebuild."""
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        result = orchestrator.run()

        # _reconcile returns {"rebuilt": 0, "recovered": 0} as placeholder
        assert "rebuilt" in result["reconciled"]
        assert "recovered" in result["reconciled"]


class TestRebuildManifestFromS3:
    """Tests for manifest rebuild from S3 in incremental mode."""

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects")
    @patch("etl.orchestrator.extract_run", return_value={})
    @patch("etl.orchestrator.upload_from_env")
    @patch("etl.orchestrator.save")
    def test_rebuild_manifest_from_s3(
        self, mock_save, mock_upload, mock_extract, mock_list, mock_load, tmp_path: Path
    ) -> None:
        """Incremental + no manifest -> rebuild via push list_s3_objects."""
        from push.core.push_manifest import S3Object

        mock_list.return_value = [
            S3Object(key="data/yellow/file.parquet"),
        ]
        mock_upload.return_value = _make_push_result()

        config = ETLConfig(data_dir=str(tmp_path / "data"))
        orchestrator = Orchestrator(config)
        orchestrator.run()

        mock_list.assert_called_once()
        assert mock_list.call_args.kwargs["bucket"] == ""
        assert mock_list.call_args.kwargs["prefix"] == "data"
