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


def test_save_checkpoint_creates_file(tmp_data_dir: Path):
    checkpoint = Checkpoint(status="completed", extract={"downloaded": 10}, upload={"uploaded": 8})
    save_checkpoint(tmp_data_dir, checkpoint)
    path = tmp_data_dir / ".etl_checkpoint.json"
    assert path.exists()
    content = path.read_text()
    assert "completed" in content
    assert "pipeline_id" in content


def test_save_checkpoint_writes_valid_json(tmp_data_dir: Path):
    checkpoint = Checkpoint(status="completed", extract={"downloaded": 5}, upload={})
    save_checkpoint(tmp_data_dir, checkpoint)
    path = tmp_data_dir / ".etl_checkpoint.json"
    data = json.loads(path.read_text())
    assert data["status"] == "completed"
    assert data["extract"]["downloaded"] == 5


def test_save_checkpoint_uses_correct_filename(tmp_data_dir: Path):
    checkpoint = Checkpoint()
    save_checkpoint(tmp_data_dir, checkpoint)
    assert (tmp_data_dir / ".etl_checkpoint.json").exists()
    assert not (tmp_data_dir / "checkpoint.json").exists()


def test_load_checkpoint_returns_none_for_empty_file(tmp_data_dir: Path):
    path = tmp_data_dir / ".etl_checkpoint.json"
    path.write_text("")
    result = load_checkpoint(tmp_data_dir)
    assert result is None


def test_load_checkpoint_returns_none_for_non_json(tmp_data_dir: Path):
    path = tmp_data_dir / ".etl_checkpoint.json"
    path.write_text("this is not json at all")
    result = load_checkpoint(tmp_data_dir)
    assert result is None
