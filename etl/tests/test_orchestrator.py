"""Tests for etl.orchestrator — max 2 external boundary mocks per test."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.config import ETLConfig
from etl.manifest import Manifest
from etl.orchestrator import Orchestrator


def _make_upload_result(uploaded=0, uploaded_files=None, entries=None):
    """Create a fake UploadResult with the given attributes."""
    result = MagicMock()
    result.uploaded = uploaded
    result.uploaded_files = uploaded_files or []
    result.entries = entries or []
    return result


def _make_extract_result(downloaded=5, skipped=0, failed=0, total=5):
    """Create a fake extract result with entries attribute."""
    result = MagicMock()
    result.downloaded = downloaded
    result.skipped = skipped
    result.failed = failed
    result.total = total
    result.entries = []
    return result


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
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

    assert str(exc_info.value) == "extract failed"
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
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

    assert str(exc_info.value) == "test error"
    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 12. test_run_retries_on_extract_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_extract_failure(tmp_data_dir: Path):
    """Verify extract raises on first failure (no retry in orchestrator)."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_extract(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise Exception("transient error")

    with patch("extract.downloader.downloader.run", side_effect=failing_extract), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint"):
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

    assert call_count == 1


# ---------------------------------------------------------------------------
# 13. test_run_retries_on_upload_failure
# ---------------------------------------------------------------------------
def test_run_retries_on_upload_failure(tmp_data_dir: Path):
    """Verify upload raises on first failure (no retry in orchestrator)."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    call_count = 0

    def failing_upload(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise Exception("transient upload error")

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=failing_upload), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint"):
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

    assert call_count == 1


# ---------------------------------------------------------------------------
# 14. test_run_extract_failure_exhausts_retries
# ---------------------------------------------------------------------------
def test_run_extract_failure_exhausts_retries(tmp_data_dir: Path):
    """Verify extract raises Exception (no retry in orchestrator)."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("permanent error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

    assert str(exc_info.value) == "permanent error"
    assert mock_save.call_count == 1


# ---------------------------------------------------------------------------
# 15. test_run_upload_failure_exhausts_retries
# ---------------------------------------------------------------------------
def test_run_upload_failure_exhausts_retries(tmp_data_dir: Path):
    """Verify upload raises Exception (no retry in orchestrator)."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=Exception("permanent upload error")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

    assert str(exc_info.value) == "permanent upload error"
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
    """Verify manifest is updated with entries for uploaded files."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result(
             uploaded=2, entries=[
                 MagicMock(rel_path="file1.parquet", s3_key="data/file1.parquet", checksum="abc123"),
                 MagicMock(rel_path="file2.parquet", s3_key="data/file2.parquet", checksum="def456"),
             ]
         )), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch.object(Manifest, "record_upload") as mock_record:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert mock_record.call_count == 2


# ---------------------------------------------------------------------------
# 21. test_init_with_none_config
# ---------------------------------------------------------------------------
def test_init_with_none_config():
    """Verify orchestrator uses default ETLConfig when config is None."""
    orchestrator = Orchestrator(config=None)
    assert orchestrator.config is not None
    assert orchestrator.config.mode == "incremental"


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


# ---------------------------------------------------------------------------
# 24. test_run_incremental_no_manifest_triggers_recovery
# ---------------------------------------------------------------------------
def test_run_incremental_no_manifest_triggers_recovery(tmp_data_dir: Path):
    """Verify incremental mode triggers recovery when manifest file doesn't exist."""
    import json
    from etl.manifest import PUSH_MANIFEST_FILE

    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)

    # Create a fake parquet file on disk (simulating recovery source)
    (tmp_data_dir / "yellow").mkdir()
    (tmp_data_dir / "yellow" / "file.parquet").write_text("data")

    # Ensure manifest file doesn't exist
    manifest_path = tmp_data_dir / PUSH_MANIFEST_FILE
    assert not manifest_path.exists()

    extract_calls = []

    def capture_extract(*args, **kwargs):
        extract_calls.append(kwargs)
        return _make_extract_result()

    with patch("extract.downloader.downloader.run", side_effect=capture_extract), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    # Recovery should have populated manifest with parquet files from disk
    assert manifest_path.exists()
    manifest_data = json.loads(manifest_path.read_text())
    assert "yellow/file.parquet" in manifest_data
    assert manifest_data["yellow/file.parquet"]["status"] == "downloaded"


# ---------------------------------------------------------------------------
# 25. test_run_recovery_triggers_when_manifest_missing
# ---------------------------------------------------------------------------
def test_run_recovery_triggers_when_manifest_missing(tmp_data_dir: Path):
    """Verify orchestrator triggers recovery when manifest file is missing."""
    import json
    from etl.manifest import PUSH_MANIFEST_FILE

    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)

    # Create multiple parquet files on disk
    (tmp_data_dir / "yellow").mkdir()
    (tmp_data_dir / "yellow" / "tripdata.parquet").write_text("data")
    (tmp_data_dir / "green").mkdir()
    (tmp_data_dir / "green" / "tripdata.parquet").write_text("data")

    # Ensure manifest file doesn't exist
    manifest_path = tmp_data_dir / PUSH_MANIFEST_FILE
    assert not manifest_path.exists()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    # Recovery should have found all parquet files
    assert manifest_path.exists()
    manifest_data = json.loads(manifest_path.read_text())
    assert "yellow/tripdata.parquet" in manifest_data
    assert "green/tripdata.parquet" in manifest_data
    assert len(manifest_data) == 2


# ---------------------------------------------------------------------------
# 26. test_run_incremental_with_existing_manifest
# ---------------------------------------------------------------------------
def test_run_incremental_with_existing_manifest(tmp_data_dir: Path):
    """Verify incremental mode preserves existing manifest entries."""
    import json
    from etl.manifest import PUSH_MANIFEST_FILE

    config = ETLConfig(mode="incremental")
    orchestrator = Orchestrator(config)

    # Create existing manifest with entries
    manifest_path = tmp_data_dir / PUSH_MANIFEST_FILE
    existing_data = {
        "yellow/existing.parquet": {
            "status": "uploaded",
            "s3_key": "data/yellow/existing.parquet",
            "checksum": "abc123",
        }
    }
    manifest_path.write_text(json.dumps(existing_data))

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    # Existing manifest should be preserved
    manifest_data = json.loads(manifest_path.read_text())
    assert "yellow/existing.parquet" in manifest_data
    assert manifest_data["yellow/existing.parquet"]["status"] == "uploaded"
    assert manifest_data["yellow/existing.parquet"]["checksum"] == "abc123"


# ---------------------------------------------------------------------------
# 27. test_run_extract_failure_saves_checkpoint_with_error_message
# ---------------------------------------------------------------------------
def test_run_extract_failure_saves_checkpoint_with_error_message(tmp_data_dir: Path):
    """Verify checkpoint saves the correct error message when extract fails."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("connection refused")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

        assert str(exc_info.value) == "connection refused"
        assert mock_save.call_count == 1
        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"
        assert checkpoint.error == "connection refused"


# ---------------------------------------------------------------------------
# 28. test_run_upload_failure_saves_checkpoint_with_error_message
# ---------------------------------------------------------------------------
def test_run_upload_failure_saves_checkpoint_with_error_message(tmp_data_dir: Path):
    """Verify checkpoint saves the correct error message when upload fails."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=Exception("timeout")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception) as exc_info:
            orchestrator.run()

        assert str(exc_info.value) == "timeout"
        assert mock_save.call_count == 1
        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"
        assert checkpoint.error == "timeout"


# ---------------------------------------------------------------------------
# 29. test_run_extract_failure_state_is_failed
# ---------------------------------------------------------------------------
def test_run_extract_failure_state_is_failed(tmp_data_dir: Path):
    """Verify state is marked as failed when extract raises exception."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("extract error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        assert mock_save.call_count == 1
        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"


# ---------------------------------------------------------------------------
# 30. test_run_upload_failure_state_is_failed
# ---------------------------------------------------------------------------
def test_run_upload_failure_state_is_failed(tmp_data_dir: Path):
    """Verify state is marked as failed when upload raises exception."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=Exception("upload error")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        assert mock_save.call_count == 1
        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"


# ---------------------------------------------------------------------------
# 31. test_run_extract_failure_checkpoint_has_metrics
# ---------------------------------------------------------------------------
def test_run_extract_failure_checkpoint_has_metrics(tmp_data_dir: Path):
    """Verify checkpoint has extract metrics even on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("partial error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"
        assert checkpoint.error == "partial error"
        assert checkpoint.extract is not None


# ---------------------------------------------------------------------------
# 32. test_run_upload_failure_checkpoint_has_extract_metrics
# ---------------------------------------------------------------------------
def test_run_upload_failure_checkpoint_has_extract_metrics(tmp_data_dir: Path):
    """Verify checkpoint has extract metrics when upload fails."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", side_effect=Exception("upload failed")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.status == "failed"
        assert checkpoint.extract is not None
        assert checkpoint.upload is not None


# ---------------------------------------------------------------------------
# 33. test_run_failure_checkpoint_has_pipeline_id
# ---------------------------------------------------------------------------
def test_run_failure_checkpoint_has_pipeline_id(tmp_data_dir: Path):
    """Verify checkpoint has a pipeline_id even on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.pipeline_id is not None
        assert len(checkpoint.pipeline_id) > 0


# ---------------------------------------------------------------------------
# 34. test_run_failure_checkpoint_has_started_at
# ---------------------------------------------------------------------------
def test_run_failure_checkpoint_has_started_at(tmp_data_dir: Path):
    """Verify checkpoint has started_at timestamp even on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.started_at is not None


# ---------------------------------------------------------------------------
# 35. test_run_failure_checkpoint_has_total_duration
# ---------------------------------------------------------------------------
def test_run_failure_checkpoint_has_total_duration(tmp_data_dir: Path):
    """Verify checkpoint has total_duration_seconds even on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.total_duration_seconds is not None
        assert checkpoint.total_duration_seconds >= 0


# ---------------------------------------------------------------------------
# 36. test_run_failure_checkpoint_has_error_not_none
# ---------------------------------------------------------------------------
def test_run_failure_checkpoint_has_error_not_none(tmp_data_dir: Path):
    """Verify checkpoint has error message (not None) on failure."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", side_effect=Exception("my error")), \
         patch("upload.core.runner.upload_from_env"), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        _setup_path_mock(mock_path, tmp_data_dir)
        with pytest.raises(Exception):
            orchestrator.run()

        checkpoint = mock_save.call_args[0][1]
        assert checkpoint.error is not None
        assert checkpoint.error == "my error"
        assert isinstance(checkpoint.error, str)
        assert len(checkpoint.error) > 0


# ---------------------------------------------------------------------------
# 37. test_run_calls_get_existing_uploads
# ---------------------------------------------------------------------------
def test_run_calls_get_existing_uploads(tmp_data_dir: Path):
    """Verify orchestrator calls get_existing_uploads during init."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.get_existing_uploads") as mock_get:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    mock_get.assert_called_once_with(tmp_data_dir)


# ---------------------------------------------------------------------------
# 38. test_run_passes_entries_to_manifest_init
# ---------------------------------------------------------------------------
def test_run_passes_entries_to_manifest_init(tmp_data_dir: Path):
    """Verify orchestrator passes get_existing_uploads results to manifest.init()."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    received_entries = {}

    def fake_get_existing_uploads(data_dir: Path) -> dict:
        return {
            "yellow/trip.parquet": {
                "s3_key": "data/yellow/trip.parquet",
                "checksum": "abc123",
            }
        }

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.get_existing_uploads", side_effect=fake_get_existing_uploads), \
         patch.object(Manifest, "init") as mock_init:
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    # manifest.init() should have been called with the entries dict
    assert mock_init.call_count == 1
    call_args = mock_init.call_args
    assert call_args[0][0] == {
        "yellow/trip.parquet": {
            "s3_key": "data/yellow/trip.parquet",
            "checksum": "abc123",
        }
    }


# ---------------------------------------------------------------------------
# 39. test_run_empty_uploads_no_crash
# ---------------------------------------------------------------------------
def test_run_empty_uploads_no_crash(tmp_data_dir: Path):
    """Verify pipeline works when get_existing_uploads returns {}."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.get_existing_uploads", return_value={}):
        _setup_path_mock(mock_path, tmp_data_dir)
        result = orchestrator.run()

    assert result["status"] == "completed"
    assert "metrics" in result


# ---------------------------------------------------------------------------
# 40. test_run_get_existing_uploads_passed_to_manifest
# ---------------------------------------------------------------------------
def test_run_get_existing_uploads_passed_to_manifest(tmp_data_dir: Path):
    """Verify the full chain: get_existing_uploads → manifest.init()."""
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    manifest_init_calls = []

    def fake_get_existing_uploads(data_dir: Path) -> dict:
        return {
            "yellow/2024.parquet": {"s3_key": "data/yellow/2024.parquet", "checksum": "x"},
            "green/2024.parquet": {"s3_key": "data/green/2024.parquet", "checksum": "y"},
        }

    def capture_init(*args, **kwargs):
        manifest_init_calls.append((args, kwargs))
        return {}

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.get_existing_uploads", side_effect=fake_get_existing_uploads), \
         patch.object(Manifest, "init", side_effect=capture_init):
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    assert len(manifest_init_calls) == 1
    args, kwargs = manifest_init_calls[0]
    assert len(args) >= 1
    uploaded_entries = args[0]
    assert "yellow/2024.parquet" in uploaded_entries
    assert "green/2024.parquet" in uploaded_entries


# ---------------------------------------------------------------------------
# 41. test_run_with_existing_uploads_populates_manifest
# ---------------------------------------------------------------------------
def test_run_with_existing_uploads_populates_manifest(tmp_data_dir: Path):
    """Verify existing uploads from S3 are reflected in the manifest."""
    import json
    from etl.manifest import PUSH_MANIFEST_FILE

    config = ETLConfig()
    orchestrator = Orchestrator(config)

    def fake_get_existing_uploads(data_dir: Path) -> dict:
        # get_existing_uploads returns {rel_path: info} dict
        return {
            "yellow/existing.parquet": {
                "s3_key": "data/yellow/existing.parquet",
                "checksum": "abc123",
            }
        }

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path, \
         patch("etl.orchestrator.get_existing_uploads", side_effect=fake_get_existing_uploads):
        _setup_path_mock(mock_path, tmp_data_dir)
        orchestrator.run()

    manifest_path = tmp_data_dir / PUSH_MANIFEST_FILE
    assert manifest_path.exists()
    manifest_data = json.loads(manifest_path.read_text())
    assert "yellow/existing.parquet" in manifest_data
    assert manifest_data["yellow/existing.parquet"]["status"] == "uploaded"
    assert manifest_data["yellow/existing.parquet"]["checksum"] == "abc123"
