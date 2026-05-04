"""Tests to kill upload.py and pusher.py mutations."""

from __future__ import annotations

from pathlib import Path

from push.core.state import PushResult, PushState, UploadConfig


class TestPushResult:
    """Tests for PushResult — ensure arithmetic mutations are caught."""

    def test_default_zero(self):
        r = PushResult()
        assert r.uploaded == 0
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0

    def test_nonzero(self):
        r = PushResult(uploaded=3, skipped=2, failed=1, total=6)
        assert r.uploaded == 3
        assert r.skipped == 2
        assert r.failed == 1
        assert r.total == 6

    def test_only_uploaded(self):
        r = PushResult(uploaded=5)
        assert r.uploaded == 5
        assert r.skipped == 0
        assert r.failed == 0
        assert r.total == 0

    def test_is_frozen(self):
        r = PushResult(uploaded=1)
        try:
            r.uploaded = 2  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass


class TestUploadConfig:
    """Tests for UploadConfig — verify frozen dataclass defaults."""

    def test_default_values(self):
        config = UploadConfig()
        assert config.include is None
        assert config.exclude is None
        assert config.overwrite is False

    def test_custom_values(self):
        config = UploadConfig(
            include={"*.parquet"},
            exclude={".tmp"},
            overwrite=True,
        )
        assert config.include == {"*.parquet"}
        assert config.exclude == {".tmp"}
        assert config.overwrite is True

    def test_is_frozen(self):
        config = UploadConfig(overwrite=True)
        try:
            config.overwrite = False  # type: ignore[call-arg]
            assert False, "Should have raised"
        except Exception:
            pass
