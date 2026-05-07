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


def test_init_sets_all_attributes():
    state = PipelineState()
    assert state.stage == PipelineStage.NOT_RUNNING
    assert state._started_at == 0.0
    assert state.error is None
    assert isinstance(state._metrics, PipelineMetrics)


def test_start_sets_timing():
    state = PipelineState()
    assert state._started_at == 0.0
    state.start()
    assert state._started_at > 0.0


def test_mark_extract_done_creates_correct_metrics():
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    m = state._metrics.extract
    assert m.duration_seconds == 5.5
    assert m.downloaded == 10
    assert m.skipped == 2
    assert m.failed == 1
    assert m.total == 13
    assert m.uploaded == 0
    assert m.uploaded_files == []


def test_mark_upload_done_preserves_extract():
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet"], duration=3.2)
    assert state._metrics.extract.downloaded == 10
    assert state._metrics.extract.skipped == 2
    assert state._metrics.extract.failed == 1
    assert state._metrics.extract.total == 13
    assert state._metrics.extract.duration_seconds == 5.5


def test_complete_preserves_all_metrics():
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet"], duration=3.2)
    state.complete()
    assert state._metrics.extract.downloaded == 10
    assert state._metrics.upload.uploaded == 8
    assert state._metrics.upload.uploaded_files == ["a.parquet"]
    assert state._metrics.total_duration_seconds > 0


def test_fail_preserves_metrics_and_sets_error():
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    state.fail("something broke")
    assert state.error == "something broke"
    assert state._metrics.total_duration_seconds > 0


def test_result_has_all_required_keys():
    state = PipelineState()
    state.start()
    state.mark_extract_done(downloaded=5, skipped=1, failed=0, total=6, duration=2.0)
    state.mark_upload_done(uploaded=4, uploaded_files=["x.parquet"], duration=1.5)
    state.complete()
    result = state.result()
    assert "status" in result
    assert "metrics" in result
    extract_keys = set(result["metrics"]["extract"].keys())
    assert extract_keys == {"duration_seconds", "downloaded", "skipped", "failed", "total"}
    upload_keys = set(result["metrics"]["upload"].keys())
    assert upload_keys == {"duration_seconds", "uploaded", "uploaded_files"}


def test_extract_metrics_section_keys():
    state = PipelineState()
    state.mark_extract_done(downloaded=5, skipped=1, failed=0, total=6, duration=2.0)
    section = state._extract_metrics_section()
    assert set(section.keys()) == {"duration_seconds", "downloaded", "skipped", "failed", "total"}


def test_upload_metrics_section_keys():
    state = PipelineState()
    state.mark_upload_done(uploaded=4, uploaded_files=["x.parquet"], duration=1.5)
    section = state._upload_metrics_section()
    assert set(section.keys()) == {"duration_seconds", "uploaded", "uploaded_files"}


def test_complete_preserves_extract_and_upload_metrics():
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=0, total=12, duration=5.0)
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet"], duration=3.0)
    state.complete()
    assert state._metrics.extract.duration_seconds == 5.0
    assert state._metrics.extract.downloaded == 10
    assert state._metrics.upload.duration_seconds == 3.0
    assert state._metrics.upload.uploaded == 8
    assert state._metrics.upload.uploaded_files == ["a.parquet"]
    assert state._metrics.total_duration_seconds > 0


def test_fail_sets_total_duration_from_started():
    state = PipelineState()
    state.start()
    time.sleep(0.02)
    state.fail("error")
    assert state._metrics.total_duration_seconds > 0
    assert state._metrics.extract.duration_seconds == 0.0
    assert state._metrics.upload.uploaded == 0


def test_mark_upload_done_calculates_total_duration():
    state = PipelineState()
    state.start()
    time.sleep(0.02)
    state.mark_upload_done(uploaded=5, uploaded_files=["x.parquet"], duration=1.0)
    assert state._metrics.total_duration_seconds > 0
    assert state._metrics.upload.duration_seconds == 1.0


def test_mark_extract_done_creates_stage_metrics():
    """Verify mark_extract_done creates StageMetrics with all fields."""
    state = PipelineState()
    state.mark_extract_done(downloaded=10, skipped=2, failed=1, total=13, duration=5.5)
    m = state._metrics.extract
    assert isinstance(m, StageMetrics)
    assert m.duration_seconds == 5.5
    assert m.downloaded == 10
    assert m.skipped == 2
    assert m.failed == 1
    assert m.total == 13


def test_mark_upload_done_creates_stage_metrics():
    """Verify mark_upload_done creates StageMetrics with all fields."""
    state = PipelineState()
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet"], duration=3.2)
    m = state._metrics.upload
    assert isinstance(m, StageMetrics)
    assert m.duration_seconds == 3.2
    assert m.uploaded == 8
    assert m.uploaded_files == ["a.parquet"]


def test_complete_creates_pipeline_metrics():
    """Verify complete creates PipelineMetrics with extract/upload preserved."""
    state = PipelineState()
    state.mark_extract_done(downloaded=10, total=10, duration=5.0)
    state.mark_upload_done(uploaded=8, uploaded_files=["a.parquet"], duration=3.0)
    state.complete()
    assert isinstance(state._metrics, PipelineMetrics)
    assert isinstance(state._metrics.extract, StageMetrics)
    assert isinstance(state._metrics.upload, StageMetrics)
