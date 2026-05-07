"""Integration tests for etl.orchestrator — end-to-end pipeline flow.

Boundary mocks: extract.downloader.downloader.run + upload.core.runner.upload_from_env
(Infrastructure patches: load_dotenv, Path, save_checkpoint, save_manifest — not counted)
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.config import ETLConfig
from etl.orchestrator import Orchestrator


def _make_upload_result(uploaded=0, uploaded_files=None):
    """Create a fake UploadResult with the given attributes."""
    result = MagicMock()
    result.uploaded = uploaded
    result.uploaded_files = uploaded_files or []
    return result


def _make_extract_result(downloaded=5, skipped=0, failed=0, total=5):
    """Create a fake extract result dict."""
    return {"downloaded": downloaded, "skipped": skipped, "failed": failed, "total": total}


def _setup_path_mock(mock_path: MagicMock, tmp_data_dir: Path) -> None:
    """Configure Path mock to return tmp_data_dir for 'data' and real Path otherwise."""
    mock_path.return_value = tmp_data_dir
    mock_path.side_effect = lambda x: tmp_data_dir if x == "data" else Path(x)


# ---------------------------------------------------------------------------
# 1. test_run_success_path_calls_all_stages
# ---------------------------------------------------------------------------
def test_run_success_path_calls_all_stages(tmp_data_dir: Path):
    """Verify extract.run and upload_from_env are called in correct sequence."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_order = []

    def capture_extract(*args, **kwargs):
        call_order.append("extract")
        return _make_extract_result()

    def capture_upload(*args, **kwargs):
        call_order.append("upload")
        return _make_upload_result(uploaded=3)

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert result["status"] == "completed"
    assert call_order == ["extract", "upload"]


# ---------------------------------------------------------------------------
# 2. test_run_success_path_saves_checkpoint_with_correct_data
# ---------------------------------------------------------------------------
def test_run_success_path_saves_checkpoint_with_correct_data(tmp_data_dir: Path):
    """Verify checkpoint saved on success has status=completed and correct metrics."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_checkpoint = None

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result(
        downloaded=10, skipped=2, failed=0, total=12
    )), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(
             uploaded=8, uploaded_files=["yellow/tripdata.parquet"]
         )), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save, \
         patch.object(orchestrator.checksum, "compute", return_value="abc123"):
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert mock_save.call_count == 1
    captured_checkpoint = mock_save.call_args[0][1]
    assert captured_checkpoint.status == "completed"
    assert captured_checkpoint.extract["downloaded"] == 10
    assert captured_checkpoint.extract["skipped"] == 2
    assert captured_checkpoint.extract["total"] == 12
    assert captured_checkpoint.upload["uploaded"] == 8
    assert captured_checkpoint.upload["uploaded_files"] == ["yellow/tripdata.parquet"]


# ---------------------------------------------------------------------------
# 3. test_run_failure_path_saves_checkpoint_with_error
# ---------------------------------------------------------------------------
def test_run_failure_path_saves_checkpoint_with_error(tmp_data_dir: Path):
    """Verify checkpoint saved on failure has status=failed and error message."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_checkpoint = None

    with patch("extract.downloader.downloader.run", side_effect=Exception("network timeout")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="network timeout"):
            orchestrator.run()

    assert mock_save.call_count == 1
    captured_checkpoint = mock_save.call_args[0][1]
    assert captured_checkpoint.status == "failed"
    assert captured_checkpoint.error == "network timeout"


# ---------------------------------------------------------------------------
# 4. test_run_with_full_mode_sets_overwrite_config
# ---------------------------------------------------------------------------
def test_run_with_full_mode_sets_overwrite_config(tmp_data_dir: Path):
    """Verify ETLConfig(mode='full') sets overwrite=True on upload config."""
    config = ETLConfig(mode="full")
    orchestrator = Orchestrator(config)
    captured_config = None

    def capture_upload(*args, **kwargs):
        nonlocal captured_config
        captured_config = kwargs.get("config")
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_config is not None
    assert captured_config.overwrite is True


# ---------------------------------------------------------------------------
# 5. test_run_with_incremental_mode_no_overwrite
# ---------------------------------------------------------------------------
def test_run_with_incremental_mode_no_overwrite(tmp_data_dir: Path):
    """Verify ETLConfig(mode='incremental') sets overwrite=False on upload config."""
    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)
    captured_config = None

    def capture_upload(*args, **kwargs):
        nonlocal captured_config
        captured_config = kwargs.get("config")
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_config is not None
    assert captured_config.overwrite is False


# ---------------------------------------------------------------------------
# 6. test_run_retries_on_extract_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_extract_failure(tmp_data_dir: Path):
    """Verify extract is retried: fails 2x, succeeds on 3rd attempt."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_extract(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("transient network error")
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=failing_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"):
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert call_count == 3
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# 7. test_run_retries_on_upload_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_upload_failure(tmp_data_dir: Path):
    """Verify upload is retried: fails 2x, succeeds on 3rd attempt."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_upload(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("transient upload error")
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=failing_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"):
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert call_count == 3
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# 8. test_run_extract_failure_after_retries_raises
# ---------------------------------------------------------------------------
def test_run_extract_failure_after_retries_raises(tmp_data_dir: Path):
    """Verify extract raises Exception after exhausting 3 retry attempts."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def always_fail(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise Exception("permanent extract failure")

    with patch("extract.downloader.downloader.run", side_effect=always_fail), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"), \
         patch("etl.orchestrator.save_checkpoint"):
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="permanent extract failure"):
            orchestrator.run()

    assert call_count == 3


# ---------------------------------------------------------------------------
# 9. test_run_upload_failure_after_retries_raises
# ---------------------------------------------------------------------------
def test_run_upload_failure_after_retries_raises(tmp_data_dir: Path):
    """Verify upload raises Exception after exhausting 3 retry attempts."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def always_fail(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise Exception("permanent upload failure")

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=always_fail), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"), \
         patch("etl.orchestrator.save_checkpoint"):
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="permanent upload failure"):
            orchestrator.run()

    assert call_count == 3


# ---------------------------------------------------------------------------
# 10. test_run_with_types_filter_passes_to_extract
# ---------------------------------------------------------------------------
def test_run_with_types_filter_passes_to_extract(tmp_data_dir: Path):
    """Verify ETLConfig(types={'yellow'}) passes types list to extract."""
    config = ETLConfig(types={"yellow"})
    orchestrator = Orchestrator(config)
    captured_types = None

    def capture_extract(*args, **kwargs):
        nonlocal captured_types
        captured_types = kwargs.get("types")
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_types == ["yellow"]


# ---------------------------------------------------------------------------
# 11. test_run_with_types_none_passes_none_to_extract
# ---------------------------------------------------------------------------
def test_run_with_types_none_passes_none_to_extract(tmp_data_dir: Path):
    """Verify ETLConfig() with no types passes None to extract."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_types = None

    def capture_extract(*args, **kwargs):
        nonlocal captured_types
        captured_types = kwargs.get("types")
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_types is None


# ---------------------------------------------------------------------------
# 12. test_run_passes_checksum_func_to_extract
# ---------------------------------------------------------------------------
def test_run_passes_checksum_func_to_extract(tmp_data_dir: Path):
    """Verify checksum_func (non-None callable) is passed to extract."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_checksum_func = None

    def capture_extract(*args, **kwargs):
        nonlocal captured_checksum_func
        captured_checksum_func = kwargs.get("checksum_func")
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_checksum_func is not None
    assert callable(captured_checksum_func)


# ---------------------------------------------------------------------------
# 13. test_run_passes_checksum_func_to_upload
# ---------------------------------------------------------------------------
def test_run_passes_checksum_func_to_upload(tmp_data_dir: Path):
    """Verify checksum_func (non-None callable) is passed to upload."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_checksum_func = None

    def capture_upload(*args, **kwargs):
        nonlocal captured_checksum_func
        captured_checksum_func = kwargs.get("checksum_func")
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert captured_checksum_func is not None
    assert callable(captured_checksum_func)


# ---------------------------------------------------------------------------
# 14. test_run_updates_manifest_with_checksums
# ---------------------------------------------------------------------------
def test_run_updates_manifest_with_checksums(tmp_data_dir: Path):
    """Verify manifest is saved with entries containing checksums for uploaded files."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    captured_manifest = None

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(
             uploaded=1, uploaded_files=["yellow/test.parquet"]
         )), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.manifest_updater.save_manifest") as mock_save_manifest, \
         patch.object(orchestrator.checksum, "compute", return_value="abc123"):
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert mock_save_manifest.call_count == 1
    captured_manifest = mock_save_manifest.call_args[0][1]
    assert "yellow/test.parquet" in captured_manifest
    entry = captured_manifest["yellow/test.parquet"]
    assert "s3_key" in entry
    assert entry["s3_key"] == "data/yellow/test.parquet"
    assert "checksum" in entry
    assert isinstance(entry["checksum"], str)
    assert len(entry["checksum"]) > 0
