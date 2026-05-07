"""Tests for etl.orchestrator — max 2 external boundary mocks per test."""

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
# 1. test_run_returns_result_dict
# ---------------------------------------------------------------------------
def test_run_returns_result_dict(tmp_data_dir: Path):
    """Verify run returns a complete result dict with status and metrics."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(uploaded=5)), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert result["status"] == "completed"
    assert "metrics" in result
    assert result["metrics"]["extract"]["downloaded"] == 5
    assert result["metrics"]["upload"]["uploaded"] == 5


# ---------------------------------------------------------------------------
# 2. test_run_calls_extract_with_manifest
# ---------------------------------------------------------------------------
def test_run_calls_extract_with_manifest(tmp_data_dir: Path):
    """Verify extract is called with push_manifest parameter."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_extract(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert "push_manifest" in call_kwargs
    assert call_kwargs["push_manifest"] == {}
    assert call_kwargs["data_dir"] == str(tmp_data_dir)


# ---------------------------------------------------------------------------
# 3. test_run_calls_upload_with_config
# ---------------------------------------------------------------------------
def test_run_calls_upload_with_config(tmp_data_dir: Path):
    """Verify upload is called with a config object."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_upload(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert "config" in call_kwargs
    assert call_kwargs["config"].overwrite is False


# ---------------------------------------------------------------------------
# 4. test_run_saves_checkpoint_on_success
# ---------------------------------------------------------------------------
def test_run_saves_checkpoint_on_success(tmp_data_dir: Path):
    """Verify checkpoint is saved after successful pipeline."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 5. test_run_saves_checkpoint_on_failure
# ---------------------------------------------------------------------------
def test_run_saves_checkpoint_on_failure(tmp_data_dir: Path):
    """Verify checkpoint is saved after pipeline failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("extract failed")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="extract failed"):
            orchestrator.run()

    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 6. test_run_full_mode_sets_overwrite
# ---------------------------------------------------------------------------
def test_run_full_mode_sets_overwrite(tmp_data_dir: Path):
    """Verify full mode sets overwrite=True on upload config."""
    config = ETLConfig(mode="full")
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_upload(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert call_kwargs["config"].overwrite is True


# ---------------------------------------------------------------------------
# 7. test_run_incremental_mode_no_overwrite
# ---------------------------------------------------------------------------
def test_run_incremental_mode_no_overwrite(tmp_data_dir: Path):
    """Verify incremental mode sets overwrite=False on upload config."""
    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_upload(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert call_kwargs["config"].overwrite is False


# ---------------------------------------------------------------------------
# 8. test_run_passes_delete_after_upload
# ---------------------------------------------------------------------------
def test_run_passes_delete_after_upload(tmp_data_dir: Path):
    """Verify delete_after_upload config is passed to upload."""
    config = ETLConfig(delete_after_upload=True)
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_upload(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert call_kwargs["config"].delete_after_upload is True


# ---------------------------------------------------------------------------
# 9. test_run_empty_upload_does_not_crash
# ---------------------------------------------------------------------------
def test_run_empty_upload_does_not_crash(tmp_data_dir: Path):
    """Verify pipeline completes even with zero uploaded files."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(uploaded=0, uploaded_files=[])), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# 10. test_run_state_transitions
# ---------------------------------------------------------------------------
def test_run_state_transitions(tmp_data_dir: Path):
    """Verify pipeline stages are recorded in result metrics."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert result["status"] == "completed"
    metrics = result["metrics"]
    assert "extract" in metrics
    assert "upload" in metrics
    assert "duration_seconds" in metrics["extract"]


# ---------------------------------------------------------------------------
# 11. test_run_error_handling
# ---------------------------------------------------------------------------
def test_run_error_handling(tmp_data_dir: Path):
    """Verify error is re-raised and checkpoint is saved on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("test error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="test error"):
            orchestrator.run()

    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 12. test_run_retries_on_extract_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_extract_failure(tmp_data_dir: Path):
    """Verify extract retries 3 times before succeeding."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_extract(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("transient error")
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
# 13. test_run_retries_on_upload_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_upload_failure(tmp_data_dir: Path):
    """Verify upload retries 3 times before succeeding."""
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
# 14. test_run_extract_failure_exhausts_retries
# ---------------------------------------------------------------------------
def test_run_extract_failure_exhausts_retries(tmp_data_dir: Path):
    """Verify extract raises after exhausting 3 retry attempts."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("permanent error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"), \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="permanent error"):
            orchestrator.run()

    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 15. test_run_upload_failure_exhausts_retries
# ---------------------------------------------------------------------------
def test_run_upload_failure_exhausts_retries(tmp_data_dir: Path):
    """Verify upload raises after exhausting 3 retry attempts."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=Exception("permanent upload error")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("time.sleep"), \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception, match="permanent upload error"):
            orchestrator.run()

    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 16. test_run_with_types_filter_passes_to_extract
# ---------------------------------------------------------------------------
def test_run_with_types_filter_passes_to_extract(tmp_data_dir: Path):
    """Verify config.types is passed as list to extract."""
    config = ETLConfig(types={"yellow", "green"})
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_extract(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert isinstance(call_kwargs["types"], list)
    assert set(call_kwargs["types"]) == {"yellow", "green"}


# ---------------------------------------------------------------------------
# 17. test_run_with_types_none_passes_none_to_extract
# ---------------------------------------------------------------------------
def test_run_with_types_none_passes_none_to_extract(tmp_data_dir: Path):
    """Verify None types is passed as None to extract."""
    config = ETLConfig(types=None)
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_extract(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert call_kwargs["types"] is None


# ---------------------------------------------------------------------------
# 18. test_run_passes_checksum_func_to_extract
# ---------------------------------------------------------------------------
def test_run_passes_checksum_func_to_extract(tmp_data_dir: Path):
    """Verify checksum_func is passed to extract."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_extract(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert "checksum_func" in call_kwargs
    assert callable(call_kwargs["checksum_func"])


# ---------------------------------------------------------------------------
# 19. test_run_passes_checksum_func_to_upload
# ---------------------------------------------------------------------------
def test_run_passes_checksum_func_to_upload(tmp_data_dir: Path):
    """Verify checksum_func is passed to upload."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_kwargs = {}

    def capture_upload(*args, **kwargs):
        call_kwargs.update(kwargs)
        return _make_upload_result()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=capture_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert "checksum_func" in call_kwargs
    assert callable(call_kwargs["checksum_func"])


# ---------------------------------------------------------------------------
# 20. test_run_updates_manifest_with_checksums
# ---------------------------------------------------------------------------
def test_run_updates_manifest_with_checksums(tmp_data_dir: Path):
    """Verify manifest is saved with entries for uploaded files."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(
             uploaded=2, uploaded_files=["file1.csv", "file2.csv"]
         )), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.manifest_updater.save_manifest") as mock_save_manifest, \
         patch.object(orchestrator.checksum, "compute", return_value="abc123"):
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert mock_save_manifest.call_count == 1


# ---------------------------------------------------------------------------
# 21. test_init_creates_checksum_provider
# ---------------------------------------------------------------------------
def test_init_creates_checksum_provider():
    """Verify orchestrator creates a checksum provider on init."""
    orchestrator = Orchestrator()
    assert orchestrator.checksum is not None


# ---------------------------------------------------------------------------
# 22. test_init_with_none_config
# ---------------------------------------------------------------------------
def test_init_with_none_config():
    """Verify orchestrator uses default ETLConfig when config is None."""
    orchestrator = Orchestrator(config=None)
    assert orchestrator.config is not None
    assert orchestrator.config.mode == "incremental"


# ---------------------------------------------------------------------------
# 23. test_init_with_custom_config
# ---------------------------------------------------------------------------
def test_init_with_custom_config():
    """Verify orchestrator stores custom config as-is."""
    config = ETLConfig(mode="full", from_year=2020, to_year=2023, delete_after_upload=True)
    orchestrator = Orchestrator(config)
    assert orchestrator.config.mode == "full"
    assert orchestrator.config.from_year == 2020
    assert orchestrator.config.to_year == 2023
    assert orchestrator.config.delete_after_upload is True
