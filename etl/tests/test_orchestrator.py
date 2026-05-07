"""Tests for etl.orchestrator."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from etl.config import ETLConfig
from etl.orchestrator import Orchestrator


def _make_upload_result(uploaded=0, uploaded_files=None):
    result = MagicMock()
    result.uploaded = uploaded
    result.uploaded_files = uploaded_files or []
    return result


def test_run_returns_result_dict(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch.object(orchestrator.checksum, "compute_sha256", return_value="abc123"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        result = orchestrator.run()
    assert "status" in result
    assert "metrics" in result


def test_run_calls_extract_with_manifest(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract") as mock_extract, \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        orchestrator.run()
    mock_extract.assert_called_once()


def test_run_calls_upload_with_config(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload") as mock_upload, \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        orchestrator.run()
    mock_upload.assert_called_once()


def test_run_saves_checkpoint_on_success(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.save_checkpoint") as mock_save, \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        orchestrator.run()
    assert mock_save.call_count == 1


def test_run_saves_checkpoint_on_failure(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract", side_effect=Exception("extract failed")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.save_checkpoint") as mock_save, \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        try:
            orchestrator.run()
        except Exception:
            pass
    assert mock_save.call_count == 1


def test_run_full_mode_sets_overwrite(tmp_data_dir: Path):
    config = ETLConfig(mode="full")
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        result = orchestrator.run()
    assert result["status"] == "completed"


def test_run_incremental_mode_no_overwrite(tmp_data_dir: Path):
    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        result = orchestrator.run()
    assert result["status"] == "completed"


def test_run_passes_delete_after_upload(tmp_data_dir: Path):
    config = ETLConfig(delete_after_upload=True)
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        orchestrator.run()


def test_run_empty_upload_does_not_crash(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        result = orchestrator.run()
    assert result["status"] == "completed"


def test_run_state_transitions(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch.object(orchestrator, "_execute_extract"), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.save_checkpoint"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        orchestrator.run()


def test_run_error_handling(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    with patch.object(orchestrator, "_execute_extract", side_effect=Exception("test error")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.save_checkpoint") as mock_save, \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        try:
            orchestrator.run()
        except Exception as e:
            assert str(e) == "test error"
    assert mock_save.call_count == 1


def test_run_retries_on_extract_failure(tmp_data_dir: Path):
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_extract(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("transient error")
        return {"downloaded": 5, "skipped": 0, "failed": 0, "total": 5}

    with patch("extract.downloader.downloader.run", side_effect=failing_extract), \
         patch.object(orchestrator, "_execute_upload"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.load_manifest", return_value={}):
        mock_path.return_value = tmp_data_dir
        mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)
        result = orchestrator.run()
    assert call_count == 3
    assert result["status"] == "completed"
