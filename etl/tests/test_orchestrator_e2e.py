"""End-to-end tests for orchestrator using real state and checkpoint machinery."""

from pathlib import Path

import time

from etl.checkpoint import Checkpoint, load_checkpoint, save_checkpoint
from etl.state import PipelineState


def test_e2e_run_success_with_real_state() -> None:
    """Create PipelineState, run full happy path, verify result() is complete."""
    state = PipelineState()
    assert state.stage == "not_running"

    state.start()
    assert state.stage == "running"
    assert state._started_at > 0.0

    state.mark_extract_done(
        downloaded=10, skipped=2, failed=1, total=13, duration=5.5,
    )
    assert state.stage == "extract_done"

    time.sleep(0.01)
    state.mark_upload_done(
        uploaded=8,
        uploaded_files=["yellow/tripdata_2024-01.parquet", "green/tripdata_2024-01.parquet"],
        duration=3.2,
    )
    assert state.stage == "upload_done"

    time.sleep(0.01)
    state.complete()
    assert state.stage == "completed"

    result = state.result()
    assert result["status"] == "completed"
    assert "metrics" in result
    assert "extract" in result["metrics"]
    assert "upload" in result["metrics"]
    assert result["metrics"]["extract"]["downloaded"] == 10
    assert result["metrics"]["extract"]["skipped"] == 2
    assert result["metrics"]["extract"]["failed"] == 1
    assert result["metrics"]["extract"]["total"] == 13
    assert result["metrics"]["extract"]["duration_seconds"] == 5.5
    assert result["metrics"]["upload"]["uploaded"] == 8
    assert result["metrics"]["upload"]["uploaded_files"] == [
        "yellow/tripdata_2024-01.parquet",
        "green/tripdata_2024-01.parquet",
    ]
    assert result["metrics"]["upload"]["duration_seconds"] == 3.2
    assert result["metrics"]["total_duration_seconds"] > 0


def test_e2e_run_failure_with_real_state() -> None:
    """Create PipelineState, fail early, verify error and timing."""
    state = PipelineState()
    state.start()
    time.sleep(0.01)
    state.fail("test error")

    assert state.stage == "failed"
    assert state.error == "test error"
    assert state._metrics.total_duration_seconds > 0

    result = state.result()
    assert result["status"] == "failed"
    assert result["metrics"]["total_duration_seconds"] > 0


def test_e2e_checkpoint_save_load_roundtrip(tmp_data_dir: Path) -> None:
    """Save a realistic Checkpoint, reload it, verify every field."""
    checkpoint = Checkpoint(
        status="completed",
        extract={
            "duration": 45.2,
            "downloaded": 120,
            "skipped": 30,
            "failed": 2,
            "total": 152,
        },
        upload={
            "duration": 30.5,
            "uploaded": 118,
            "uploaded_files": [
                "yellow/tripdata_2024-01.parquet",
                "green/tripdata_2024-01.parquet",
            ],
        },
        total_duration_seconds=75.7,
    )

    save_checkpoint(tmp_data_dir, checkpoint)

    loaded = load_checkpoint(tmp_data_dir)
    assert loaded is not None
    assert loaded.status == "completed"
    assert loaded.extract["downloaded"] == 120
    assert loaded.extract["skipped"] == 30
    assert loaded.extract["failed"] == 2
    assert loaded.extract["total"] == 152
    assert loaded.upload["uploaded"] == 118
    assert loaded.upload["uploaded_files"] == [
        "yellow/tripdata_2024-01.parquet",
        "green/tripdata_2024-01.parquet",
    ]
    assert loaded.total_duration_seconds == 75.7
    assert loaded.pipeline_id is not None
    assert loaded.started_at is not None
