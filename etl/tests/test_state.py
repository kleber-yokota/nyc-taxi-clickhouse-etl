"""Tests for etl.state."""

import time

from etl.state import PipelineMetrics, PipelineStage, PipelineState, StageMetrics


def test_initial_state_is_not_running():
    state = PipelineState()
    assert state.stage == PipelineStage.NOT_RUNNING


def test_start_sets_running():
    state = PipelineState()
    state.start()
    assert state.stage == PipelineStage.RUNNING


def test_mark_extract_done_records_metrics():
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    assert state.stage == PipelineStage.EXTRACT_DONE
    assert state._metrics.extract.downloaded == 10
    assert state._metrics.extract.skipped == 2
    assert state._metrics.extract.failed == 1
    assert state._metrics.extract.total == 13
    assert state._metrics.extract.duration_seconds == 5.5


def test_mark_upload_done_records_metrics():
    state = PipelineState()
    state.start()
    time.sleep(0.01)
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet", "b.parquet"], duration=3.2)
    assert state.stage == PipelineStage.UPLOAD_DONE
    assert state._metrics.upload.uploaded == 8
    assert state._metrics.upload.uploaded_files == ["a.parquet", "b.parquet"]
    assert state._metrics.upload.duration_seconds == 3.2


def test_complete_calculates_total_duration():
    state = PipelineState()
    state.start()
    time.sleep(0.01)
    state.complete()
    assert state.stage == PipelineStage.COMPLETED
    assert state._metrics.total_duration_seconds > 0


def test_fail_records_error():
    state = PipelineState()
    state.start()
    state.fail("something went wrong")
    assert state.stage == PipelineStage.FAILED
    assert state.error == "something went wrong"


def test_result_dict_structure():
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, skipped=1, failed=0, total=6, duration=2.0)
    state.mark_upload_done(uploaded=4, uploaded_files=["x.parquet"], duration=1.5)
    state.complete()
    result = state.result()
    assert result["status"] == "completed"
    assert "metrics" in result
    assert "extract" in result["metrics"]
    assert "upload" in result["metrics"]
    assert result["metrics"]["extract"]["downloaded"] == 5
    assert result["metrics"]["upload"]["uploaded"] == 4


def test_is_complete():
    state = PipelineState()
    assert state.stage == PipelineStage.NOT_RUNNING
    state.start()
    assert state.stage == PipelineStage.RUNNING
    state.complete()
    assert state.stage == PipelineStage.COMPLETED
