"""Tests for etl/orchestrator.py — ETLConfig and Orchestrator basics."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from etl.orchestrator import ETLConfig, Orchestrator


class TestETLConfigDefaults:
    """Tests for ETLConfig default values."""

    def test_defaults(self) -> None:
        """Default values are correct."""
        config = ETLConfig()
        assert config.data_dir == "data"
        assert config.bucket == ""
        assert config.prefix == "data"
        assert config.types == ["yellow", "green", "fhv", "fhvhv"]
        assert config.from_year == 2024
        assert config.to_year == 2024
        assert config.mode == "incremental"
        assert config.delete_after_push is True


class TestETLConfigCustom:
    """Tests for ETLConfig custom values."""

    def test_custom_values(self) -> None:
        """Custom values are preserved."""
        config = ETLConfig(
            data_dir="mydata",
            bucket="my-bucket",
            prefix="my-prefix",
            types=["yellow"],
            from_year=2023,
            to_year=2025,
            mode="full",
            delete_after_push=False,
        )
        assert config.data_dir == "mydata"
        assert config.bucket == "my-bucket"
        assert config.prefix == "my-prefix"
        assert config.types == ["yellow"]
        assert config.from_year == 2023
        assert config.to_year == 2025
        assert config.mode == "full"
        assert config.delete_after_push is False


class TestOrchestratorInit:
    """Tests for Orchestrator initialization."""

    def test_orchestrator_creates_data_dir(self, tmp_path) -> None:
        """Orchestrator creates data_dir if it doesn't exist."""
        config = ETLConfig(data_dir=str(tmp_path / "new_data"))
        orchestrator = Orchestrator(config)
        assert not (tmp_path / "new_data").exists()

        with patch("etl.orchestrator.load", return_value={}):
            with patch("etl.orchestrator.list_s3_objects", return_value=[]):
                with patch("etl.orchestrator.extract_run", return_value={}):
                    with patch("etl.orchestrator.upload_from_env", return_value=MagicMock(uploaded_entries=[])):
                        with patch("etl.orchestrator.save"):
                            orchestrator.run()

        assert (tmp_path / "new_data").exists()

    def test_orchestrator_with_no_config(self) -> None:
        """Orchestrator uses ETLConfig defaults when no config is provided."""
        orchestrator = Orchestrator()
        assert orchestrator.config.mode == "incremental"
        assert orchestrator.config.data_dir == "data"

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run")
    def test_run_calls_extract(self, mock_extract, mock_list, mock_load, tmp_path) -> None:
        """Extract is called with correct parameters."""
        mock_upload = MagicMock(uploaded_entries=[])
        with patch("etl.orchestrator.upload_from_env", return_value=mock_upload):
            with patch("etl.orchestrator.save"):
                with patch("etl.orchestrator.Orchestrator._reconcile", return_value={}):
                    config = ETLConfig(data_dir=str(tmp_path / "data"))
                    orchestrator = Orchestrator(config)
                    orchestrator.run()

        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args
        assert call_kwargs.kwargs["data_dir"] == str(tmp_path / "data")
        assert call_kwargs.kwargs["mode"] == "incremental"

    @patch("etl.orchestrator.load", return_value={})
    @patch("etl.orchestrator.list_s3_objects", return_value=[])
    @patch("etl.orchestrator.extract_run", return_value={})
    def test_run_calls_push(self, mock_extract, mock_list, mock_load, tmp_path) -> None:
        """Push is called with correct parameters."""
        mock_upload = MagicMock(uploaded_entries=[])
        with patch("etl.orchestrator.upload_from_env") as mock_upload_fn:
            mock_upload_fn.return_value = mock_upload
            with patch("etl.orchestrator.save"):
                with patch("etl.orchestrator.Orchestrator._reconcile", return_value={}):
                    config = ETLConfig(
                        data_dir=str(tmp_path / "data"),
                        bucket="test-bucket",
                        prefix="test-prefix",
                    )
                    orchestrator = Orchestrator(config)
                    orchestrator.run()

        mock_upload_fn.assert_called_once()
        call_kwargs = mock_upload_fn.call_args
        assert call_kwargs.kwargs["bucket"] == "test-bucket"
        assert call_kwargs.kwargs["prefix"] == "test-prefix"
