"""Tests for etl.checkpoint."""

import json
from pathlib import Path

from etl.checkpoint import Checkpoint, load_checkpoint, save_checkpoint


def test_save_and_load(tmp_data_dir: Path):
    checkpoint = Checkpoint(
        status="completed",
        extract={"downloaded": 10},
        upload={"uploaded": 8},
    )
    save_checkpoint(tmp_data_dir, checkpoint)
    loaded = load_checkpoint(tmp_data_dir)
    assert loaded is not None
    assert loaded.status == "completed"
    assert loaded.extract["downloaded"] == 10
    assert loaded.upload["uploaded"] == 8


def test_load_nonexistent(tmp_data_dir: Path):
    result = load_checkpoint(tmp_data_dir)
    assert result is None


def test_load_invalid_json(tmp_data_dir: Path):
    checkpoint_path = tmp_data_dir / ".etl_checkpoint.json"
    checkpoint_path.write_text("not valid json{{{")
    result = load_checkpoint(tmp_data_dir)
    assert result is None


def test_checkpoint_serialization():
    checkpoint = Checkpoint(
        status="failed",
        error="test error",
        extract={"failed": 1},
        upload={},
        total_duration_seconds=42.0,
    )
    data = checkpoint.to_dict()
    restored = Checkpoint.from_dict(data)
    assert restored.status == "failed"
    assert restored.error == "test error"
    assert restored.extract["failed"] == 1
    assert restored.total_duration_seconds == 42.0
