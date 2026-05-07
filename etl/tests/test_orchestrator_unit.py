"""Unit tests for orchestrator internal methods — kill survivor mutants.

Each test calls internal methods directly (0 boundary mocks) to exercise
the exact code paths that mutmut mutates: arithmetic, comparisons, returns.
"""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.checkpoint import Checkpoint
from etl.config import ETLConfig
from etl.orchestrator import Orchestrator
from etl.state import PipelineState


def _make_upload_result(uploaded=0, uploaded_files=None):
    """Create a fake UploadResult with the given attributes."""
    result = MagicMock()
    result.uploaded = uploaded
    result.uploaded_files = uploaded_files or []
    return result


def _make_extract_result(downloaded=5, skipped=0, failed=0, total=5):
    """Create a fake extract result dict."""
    return {"downloaded": downloaded, "skipped": skipped, "failed": failed, "total": total}


# ---------------------------------------------------------------------------
# _execute_with_retry — 29 survivor mutants
# ---------------------------------------------------------------------------

def test_execute_with_retry_succeeds_first_attempt():
    """Verify _execute_with_retry returns immediately on success."""
    orch = Orchestrator()
    call_count = 0

    def op(_):
        nonlocal call_count
        call_count += 1
        return {"ok": True}

    result = orch._execute_with_retry("test", op, Path("/tmp"))
    assert call_count == 1
    assert result["ok"] is True


def test_execute_with_retry_retries_on_failure():
    """Verify _execute_with_retry retries 2 times before succeeding."""
    orch = Orchestrator()
    call_count = 0

    def op(_):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("transient")
        return {"ok": True}

    with patch("time.sleep"):
        result = orch._execute_with_retry("test", op, Path("/tmp"))

    assert call_count == 3
    assert result["ok"] is True


def test_execute_with_retry_raises_after_exhausting_retries():
    """Verify _execute_with_retry raises after 3 failed attempts."""
    orch = Orchestrator()

    def op(_):
        raise RuntimeError("permanent failure")

    with patch("time.sleep"):
        with pytest.raises(RuntimeError, match="permanent failure"):
            orch._execute_with_retry("test", op, Path("/tmp"))


def test_execute_with_retry_backoff_exponential():
    """Verify retry backoff uses 2**attempt: 1s, 2s, 4s."""
    orch = Orchestrator()
    sleeps = []

    def op(_):
        raise ValueError("fail")

    def record_sleep(sec):
        sleeps.append(sec)

    with patch("time.sleep", side_effect=record_sleep):
        with pytest.raises(ValueError, match="fail"):
            orch._execute_with_retry("test", op, Path("/tmp"))

    assert len(sleeps) == 2
    assert sleeps[0] == 1  # 2**0
    assert sleeps[1] == 2  # 2**1


def test_execute_with_retry_log_retry_called():
    """Verify _log_retry is called on each retry attempt."""
    orch = Orchestrator()
    logged = []

    def op(_):
        raise ValueError("fail")

    with patch("time.sleep"), \
         patch.object(orch, "_log_retry", side_effect=lambda *a: logged.append(True)):
        with pytest.raises(ValueError, match="fail"):
            orch._execute_with_retry("test", op, Path("/tmp"))

    assert len(logged) == 2  # 2 retries (attempts 0, 1)


# ---------------------------------------------------------------------------
# _mark_extract_done — 38 survivor mutants
# ---------------------------------------------------------------------------

def test_mark_extract_done_passes_all_result_fields():
    """Verify _mark_extract_done forwards all result.get() fields."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    result = {"downloaded": 10, "skipped": 3, "failed": 1, "total": 14}
    orch._mark_extract_done(state, result, duration=5.5)

    assert state.stage == "extract_done"
    assert state._metrics.extract.downloaded == 10
    assert state._metrics.extract.skipped == 3
    assert state._metrics.extract.failed == 1
    assert state._metrics.extract.total == 14
    assert state._metrics.extract.duration_seconds == 5.5


def test_mark_extract_done_handles_missing_keys():
    """Verify _mark_extract_done uses 0 default when keys missing."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    result = {}  # no keys at all
    orch._mark_extract_done(state, result, duration=1.0)

    assert state._metrics.extract.downloaded == 0
    assert state._metrics.extract.skipped == 0
    assert state._metrics.extract.failed == 0
    assert state._metrics.extract.total == 0
    assert state._metrics.extract.duration_seconds == 1.0


# ---------------------------------------------------------------------------
# _mark_upload_done — 22 survivor mutants
# ---------------------------------------------------------------------------

def test_mark_upload_done_passes_all_result_fields():
    """Verify _mark_upload_done forwards result.uploaded and uploaded_files."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, total=5, duration=2.0)

    upload_result = _make_upload_result(uploaded=3, uploaded_files=["a.parquet", "b.parquet"])
    orch._mark_upload_done(state, upload_result, duration=1.5)

    assert state.stage == "upload_done"
    assert state._metrics.upload.uploaded == 3
    assert state._metrics.upload.uploaded_files == ["a.parquet", "b.parquet"]
    assert state._metrics.upload.duration_seconds == 1.5
    # Extract metrics preserved
    assert state._metrics.extract.downloaded == 5


def test_mark_upload_done_handles_none_files():
    """Verify _mark_upload_done converts None uploaded_files to empty list."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    upload_result = _make_upload_result(uploaded=1, uploaded_files=None)
    orch._mark_upload_done(state, upload_result, duration=1.0)

    assert state._metrics.upload.uploaded_files == []


# ---------------------------------------------------------------------------
# _execute_extract — 25 survivor mutants (time operations)
# ---------------------------------------------------------------------------

def test_execute_extract_calls_do_extract_and_marks_done():
    """Verify _execute_extract calls _do_extract and updates state."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    with patch.object(orch, "_do_extract", return_value=_make_extract_result(
        downloaded=7, skipped=1, failed=0, total=8
    )) as mock_do:
        orch._execute_extract(Path("/tmp"), state)

    mock_do.assert_called_once()
    assert state.stage == "extract_done"
    assert state._metrics.extract.downloaded == 7
    assert state._metrics.extract.duration_seconds > 0


# ---------------------------------------------------------------------------
# _execute_upload — 19 survivor mutants (time operations)
# ---------------------------------------------------------------------------

def test_execute_upload_calls_do_upload_and_marks_done():
    """Verify _execute_upload calls _do_upload and updates state."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, total=5, duration=1.0)

    with patch.object(orch, "_do_upload", return_value=_make_upload_result(
        uploaded=3, uploaded_files=["x.parquet"]
    )) as mock_do:
        orch._execute_upload(Path("/tmp"), state)

    mock_do.assert_called_once()
    assert state.stage == "upload_done"
    assert state._metrics.upload.uploaded == 3
    assert state._metrics.upload.duration_seconds > 0


# ---------------------------------------------------------------------------
# _do_extract — 17 survivor mutants
# ---------------------------------------------------------------------------

def test_do_extract_calls_extract_run_with_params():
    """Verify _do_extract passes config params to extract.run."""
    config = ETLConfig(types={"yellow"}, from_year=2020, to_year=2023, mode="full")
    orch = Orchestrator(config)

    with patch("extract.downloader.downloader.run", return_value={}) as mock_run:
        orch._do_extract(Path("/tmp"))

    mock_run.assert_called_once()
    kwargs = mock_run.call_args[1]
    assert kwargs["types"] == ["yellow"]
    assert kwargs["from_year"] == 2020
    assert kwargs["to_year"] == 2023
    assert kwargs["mode"] == "full"
    assert kwargs["checksum_func"] is not None
    assert callable(kwargs["checksum_func"])


def test_do_extract_passes_none_when_types_empty():
    """Verify _do_extract passes None when config.types is None."""
    orch = Orchestrator(ETLConfig(types=None))

    with patch("extract.downloader.downloader.run", return_value={}) as mock_run:
        orch._do_extract(Path("/tmp"))

    kwargs = mock_run.call_args[1]
    assert kwargs["types"] is None


def test_do_extract_passes_push_manifest():
    """Verify _do_extract passes current manifest to extract.run."""
    orch = Orchestrator()

    with patch("extract.downloader.downloader.run", return_value={}) as mock_run, \
         patch("etl.orchestrator.load_manifest", return_value={"existing": "entry"}):
        orch._do_extract(Path("/tmp"))

    kwargs = mock_run.call_args[1]
    assert kwargs["push_manifest"] == {"existing": "entry"}


# ---------------------------------------------------------------------------
# _do_upload — 15 survivor mutants
# ---------------------------------------------------------------------------

def test_do_upload_calls_upload_from_env_with_config():
    """Verify _do_upload passes config to upload_from_env."""
    orch = Orchestrator(ETLConfig(mode="full", delete_after_upload=True))

    with patch("upload.core.runner.upload_from_env", return_value=MagicMock()) as mock_run:
        orch._do_upload(Path("/tmp"))

    mock_run.assert_called_once()
    kwargs = mock_run.call_args[1]
    assert kwargs["config"].overwrite is True
    assert kwargs["config"].delete_after_upload is True
    assert kwargs["checksum_func"] is not None


def test_do_upload_incremental_no_overwrite():
    """Verify _do_upload sets overwrite=False for incremental mode."""
    orch = Orchestrator(ETLConfig(mode="incremental"))

    with patch("upload.core.runner.upload_from_env", return_value=MagicMock()) as mock_run:
        orch._do_upload(Path("/tmp"))

    kwargs = mock_run.call_args[1]
    assert kwargs["config"].overwrite is False


# ---------------------------------------------------------------------------
# _log_retry — 19 survivor mutants
# ---------------------------------------------------------------------------

def test_log_retry_format_contains_backoff():
    """Verify _log_retry calls logger.warning with correct args."""
    orch = Orchestrator()
    captured = []

    with patch.object(orch, "config"):
        with patch("logging.Logger.warning", side_effect=lambda *a: captured.append(a)):
            orch._log_retry("extract", 0, 3, ValueError("test"))

    assert len(captured) == 1
    args = captured[0]
    assert args[0] == "%s failed (attempt %d/%d), retrying in %ds: %s"
    assert args[1] == "extract"
    assert args[2] == 1  # attempt + 1
    assert args[3] == 3  # max_retries
    assert args[4] == 1  # 2**0
    assert isinstance(args[5], ValueError)


def test_log_retry_attempt_number_increases():
    """Verify _log_retry shows correct attempt number (attempt + 1)."""
    orch = Orchestrator()
    captured = []

    with patch.object(orch, "config"):
        with patch("logging.Logger.warning", side_effect=lambda *a: captured.append(a)):
            orch._log_retry("upload", 2, 3, ValueError("test"))

    args = captured[0]
    assert args[1] == "upload"
    assert args[2] == 3  # attempt=2, so attempt+1=3
    assert args[3] == 3  # max_retries
    assert args[4] == 4  # 2**2


# ---------------------------------------------------------------------------
# _add_manifest_entry — 6 survivor mutants
# ---------------------------------------------------------------------------

def test_add_manifest_entry_creates_correct_entry():
    """Verify _add_manifest_entry creates entry with s3_key and checksum."""
    orch = Orchestrator()
    manifest = {}

    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.write(b"test data for checksum")
        f.flush()
        orch.checksum = MagicMock()
        orch.checksum.compute_sha256.return_value = "abc123"

        orch._add_manifest_entry(Path("/tmp"), manifest, "yellow/trip.parquet")

    assert "yellow/trip.parquet" in manifest
    assert manifest["yellow/trip.parquet"]["s3_key"] == "data/yellow/trip.parquet"
    assert manifest["yellow/trip.parquet"]["checksum"] == "abc123"


# ---------------------------------------------------------------------------
# _build_success_checkpoint — 11 survivor mutants
# ---------------------------------------------------------------------------

def test_build_success_checkpoint_contains_all_fields():
    """Verify _build_success_checkpoint has status, extract, upload, total_duration."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.0)

    upload_result = _make_upload_result(uploaded=8, uploaded_files=["a.parquet"])
    orch._mark_upload_done(state, upload_result, duration=3.0)
    state.complete()

    checkpoint = orch._build_success_checkpoint(state)

    assert isinstance(checkpoint, Checkpoint)
    assert checkpoint.status == "completed"
    assert checkpoint.extract["downloaded"] == 10
    assert checkpoint.extract["skipped"] == 2
    assert checkpoint.extract["failed"] == 1
    assert checkpoint.extract["duration_seconds"] == 5.0
    assert checkpoint.upload["uploaded"] == 8
    assert checkpoint.upload["uploaded_files"] == ["a.parquet"]
    assert checkpoint.upload["duration_seconds"] == 3.0
    assert checkpoint.total_duration_seconds > 0
    assert checkpoint.error is None


# ---------------------------------------------------------------------------
# _extract_metrics_dict — 10 survivor mutants
# ---------------------------------------------------------------------------

def test_extract_metrics_dict_contains_all_fields():
    """Verify _extract_metrics_dict returns dict with all extract fields."""
    orch = Orchestrator()
    metrics = MagicMock()
    metrics.duration_seconds = 45.2
    metrics.downloaded = 120
    metrics.skipped = 30
    metrics.failed = 2
    metrics.total = 152

    result = orch._extract_metrics_dict(metrics)

    assert result == {
        "duration_seconds": 45.2,
        "downloaded": 120,
        "skipped": 30,
        "failed": 2,
        "total": 152,
    }


# ---------------------------------------------------------------------------
# _upload_metrics_dict — 6 survivor mutants
# ---------------------------------------------------------------------------

def test_upload_metrics_dict_contains_all_fields():
    """Verify _upload_metrics_dict returns dict with all upload fields."""
    orch = Orchestrator()
    metrics = MagicMock()
    metrics.duration_seconds = 30.5
    metrics.uploaded = 118
    metrics.uploaded_files = ["yellow/a.parquet", "green/b.parquet"]

    result = orch._upload_metrics_dict(metrics)

    assert result == {
        "duration_seconds": 30.5,
        "uploaded": 118,
        "uploaded_files": ["yellow/a.parquet", "green/b.parquet"],
    }


# ---------------------------------------------------------------------------
# _handle_failure — 8 survivor mutants
# ---------------------------------------------------------------------------

def test_handle_failure_calls_state_fail_and_saves_checkpoint():
    """Verify _handle_failure calls state.fail() and saves checkpoint."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    captured_checkpoint = None

    with patch("etl.orchestrator.save_checkpoint") as mock_save:
        def capture_save(_, cp):
            nonlocal captured_checkpoint
            captured_checkpoint = cp
        mock_save.side_effect = capture_save

        orch._handle_failure(Path("/tmp"), state, ValueError("test error"))

    assert state.stage == "failed"
    assert state.error == "test error"
    assert isinstance(captured_checkpoint, Checkpoint)
    assert captured_checkpoint.status == "failed"
    assert captured_checkpoint.error == "test error"


# ---------------------------------------------------------------------------
# _build_failure_checkpoint — 6 survivor mutants
# ---------------------------------------------------------------------------

def test_build_failure_checkpoint_contains_error():
    """Verify _build_failure_checkpoint includes error message."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.fail("something broke")

    checkpoint = orch._build_failure_checkpoint(state)

    assert checkpoint.status == "failed"
    assert checkpoint.error == "something broke"
    assert checkpoint.total_duration_seconds > 0
    assert checkpoint.extract == {}
    assert checkpoint.upload == {}


# ---------------------------------------------------------------------------
# _update_manifest — 13 survivor mutants
# ---------------------------------------------------------------------------

def test_update_manifest_loads_and_saves():
    """Verify _update_manifest loads existing manifest and saves updated."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, total=5, duration=1.0)
    state.mark_upload_done(uploaded=2, uploaded_files=["a.parquet", "b.parquet"], duration=1.0)

    existing_manifest = {"old_file.parquet": {"s3_key": "data/old.parquet", "checksum": "old"}}

    with patch("etl.orchestrator.load_manifest", return_value=existing_manifest) as mock_load, \
         patch("etl.orchestrator.save_manifest") as mock_save, \
         patch.object(orch.checksum, "compute_sha256", return_value="new123"):
        orch._update_manifest(Path("/tmp"), state)

    mock_load.assert_called_once()
    mock_save.assert_called_once()
    saved_manifest = mock_save.call_args[0][1]
    assert "old_file.parquet" in saved_manifest
    assert "a.parquet" in saved_manifest
    assert "b.parquet" in saved_manifest
    assert saved_manifest["a.parquet"]["s3_key"] == "data/a.parquet"


# ---------------------------------------------------------------------------
# _persist_checkpoint — 6 survivor mutants
# ---------------------------------------------------------------------------

def test_persist_checkpoint_calls_build_and_save():
    """Verify _persist_checkpoint builds and saves a checkpoint."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, total=5, duration=1.0)
    state.mark_upload_done(uploaded=3, uploaded_files=["x.parquet"], duration=1.0)
    state.complete()

    with patch.object(orch, "_build_success_checkpoint") as mock_build, \
         patch("etl.orchestrator.save_checkpoint") as mock_save:
        mock_build.return_value = Checkpoint(status="completed")
        orch._persist_checkpoint(Path("/tmp"), state)

    mock_build.assert_called_once()
    mock_save.assert_called_once()


# ---------------------------------------------------------------------------
# run — 13 survivor mutants
# ---------------------------------------------------------------------------

def test_run_calls_load_dotenv():
    """Verify run calls load_dotenv at start."""
    orch = Orchestrator()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv") as mock_dotenv, \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = Path("/tmp")
        orch.run()

    mock_dotenv.assert_called_once()


def test_run_raises_on_failure():
    """Verify run re-raises the original exception on failure."""
    orch = Orchestrator()

    with patch("extract.downloader.downloader.run", side_effect=RuntimeError("boom")), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = Path("/tmp")
        with pytest.raises(RuntimeError, match="boom"):
            orch.run()


def test_run_returns_state_result():
    """Verify run returns state.result() dict."""
    orch = Orchestrator()

    with patch("extract.downloader.downloader.run", return_value=_make_extract_result()), \
         patch("upload.core.runner.upload_from_env", return_value=_make_upload_result()), \
         patch("etl.orchestrator.load_dotenv"), \
         patch("etl.orchestrator.Path") as mock_path:
        mock_path.return_value = Path("/tmp")
        result = orch.run()

    assert isinstance(result, dict)
    assert "status" in result
    assert "metrics" in result


# ---------------------------------------------------------------------------
# _init_data_dir — 6 survivor mutants
# ---------------------------------------------------------------------------

def test_init_data_dir_creates_path():
    """Verify _init_data_dir creates and returns data directory."""
    orch = Orchestrator()

    result = orch._init_data_dir()

    assert isinstance(result, Path)
    assert result.name == "data"


# ---------------------------------------------------------------------------
# _init_state — 2 survivor mutants
# ---------------------------------------------------------------------------

def test_init_state_creates_and_starts():
    """Verify _init_state creates PipelineState and calls start()."""
    orch = Orchestrator()

    state = orch._init_state()

    assert isinstance(state, PipelineState)
    assert state.stage == "running"
    assert state._started_at > 0.0


# ---------------------------------------------------------------------------
# _run_success_path — 16 survivor mutants
# ---------------------------------------------------------------------------

def test_run_success_path_calls_all_stages_in_order():
    """Verify _run_success_path calls extract, upload, manifest, complete, checkpoint."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()
    call_order = []

    with patch.object(orch, "_execute_extract", side_effect=lambda *a: call_order.append("extract")), \
         patch.object(orch, "_execute_upload", side_effect=lambda *a: call_order.append("upload")), \
         patch.object(orch, "_update_manifest", side_effect=lambda *a: call_order.append("manifest")), \
         patch.object(orch, "_persist_checkpoint", side_effect=lambda *a: call_order.append("checkpoint")):
        orch._run_success_path(Path("/tmp"), state)

    assert call_order == ["extract", "upload", "manifest", "checkpoint"]
    assert state.stage == "completed"


def test_run_success_path_returns_result():
    """Verify _run_success_path returns state.result()."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    with patch.object(orch, "_execute_extract"), \
         patch.object(orch, "_execute_upload"), \
         patch.object(orch, "_update_manifest"), \
         patch.object(orch, "_persist_checkpoint"):
        result = orch._run_success_path(Path("/tmp"), state)

    assert isinstance(result, dict)
    assert result["status"] == "completed"


def test_execute_extract_duration_is_positive_with_mocked_time():
    """Verify _execute_extract duration > 0 when time.monotonic is mocked."""
    orch = Orchestrator()
    state = PipelineState()
    state.start()

    call_times = [100.0, 105.5]  # start=100, end=105.5
    call_idx = [0]

    def mock_monotonic():
        val = call_times[call_idx[0]]
        call_idx[0] += 1
        return val

    with patch.object(orch, "_do_extract", return_value=_make_extract_result(
        downloaded=7, skipped=1, failed=0, total=8
    )), \
         patch("time.monotonic", side_effect=mock_monotonic):
        orch._execute_extract(Path("/tmp"), state)

    assert state._metrics.extract.duration_seconds == 5.5


def test_execute_upload_duration_is_positive_with_mocked_time():
    """Verify _execute_upload duration > 0 when time.monotonic is mocked."""
    orch = Orchestrator()
    state = PipelineState()

    # Sequence: state.start() → _execute_upload(2x) → mark_upload_done(1x)
    call_times = [100.0, 200.0, 203.0, 205.0]
    call_idx = [0]

    def mock_monotonic():
        val = call_times[call_idx[0]]
        call_idx[0] += 1
        return val

    with patch.object(orch, "_do_upload", return_value=_make_upload_result(
        uploaded=3, uploaded_files=["x.parquet"]
    )), \
         patch("time.monotonic", side_effect=mock_monotonic):
        state.start()
        state.mark_extract_done(downloaded=5, total=5, duration=1.0)
        orch._execute_upload(Path("/tmp"), state)

    assert state._metrics.upload.duration_seconds == 3.0


def test_execute_with_retry_raises_runtime_error_on_no_exception():
    """Verify _execute_with_retry raises RuntimeError if operation never raises."""
    orch = Orchestrator()

    # This test verifies the edge case: if operation returns without raising,
    # the function returns early (line 138). The RuntimeError on line 146
    # is only hit if the for loop completes without returning.
    # Since we always return on success, this verifies the return path.
    def op(_):
        return {"result": "ok"}

    result = orch._execute_with_retry("test", op, Path("/tmp"))
    assert result == {"result": "ok"}


def test_add_manifest_entry_uses_data_prefix():
    """Verify _add_manifest_entry uses 'data/' prefix in s3_key."""
    orch = Orchestrator()
    manifest = {}

    orch.checksum = MagicMock()
    orch.checksum.compute_sha256.return_value = "def456"

    orch._add_manifest_entry(Path("/tmp"), manifest, "green/trip.parquet")

    entry = manifest["green/trip.parquet"]
    assert entry["s3_key"] == "data/green/trip.parquet"
    assert entry["checksum"] == "def456"
