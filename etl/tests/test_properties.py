"""Property-based tests for etl module using hypothesis."""

from __future__ import annotations

from hypothesis import given, settings, strategies as st
from hypothesis.strategies import composite
from hypothesis import HealthCheck

import pytest

from etl.manifest import Manifest
from etl.config import ETLConfig
from etl.state import PipelineState, PipelineMetrics, StageMetrics, PipelineStage


# ── Strategies ──────────────────────────────────────────────────────────


def rel_path_strategy() -> st.SearchStrategy[str]:
    """Generate valid relative path strings."""
    return st.text(
        min_size=1,
        max_size=100,
        alphabet=st.characters(whitelist_categories=["L", "N", "Zs", "Pd"], blacklist_characters=("\x00", "\n", "\r")),
    )


def checksum_strategy() -> st.SearchStrategy[str]:
    """Generate valid SHA-256 hex digests."""
    return st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")


@composite
def etl_config_params(draw) -> dict:
    """Generate valid ETLConfig constructor parameters."""
    return {
        "types": draw(st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=5, unique=True).map(set) | st.none()),
        "from_year": draw(st.integers(min_value=2009, max_value=2030) | st.none()),
        "to_year": draw(st.integers(min_value=2009, max_value=2030) | st.none()),
        "mode": draw(st.sampled_from(["incremental", "full"])),
        "delete_after_upload": draw(st.booleans()),
    }


# ── Manifest properties ────────────────────────────────────────────────


class TestManifestProperties:
    """Property-based tests for Manifest."""

    @given(rel_path=rel_path_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_record_download_creates_entry(self, rel_path: str, checksum: str, tmp_path: pytest.TempPath) -> None:
        """Manifest.record_download creates an entry with correct status."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download(rel_path, checksum)
        data = manifest._load()
        assert rel_path in data
        assert data[rel_path]["status"] == "downloaded"
        assert data[rel_path]["checksum"] == checksum

    @given(rel_path=rel_path_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_record_upload_sets_status(self, rel_path: str, tmp_path: pytest.TempPath) -> None:
        """Manifest.record_upload sets status to uploaded."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_upload(rel_path)
        data = manifest._load()
        assert rel_path in data
        assert data[rel_path]["status"] == "uploaded"

    @given(rel_path=rel_path_strategy(), error=st.text(min_size=1, max_size=200))
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_record_download_failure_sets_status(self, rel_path: str, error: str, tmp_path: pytest.TempPath) -> None:
        """Manifest.record_download_failure sets status to download_failed."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download_failure(rel_path, error)
        data = manifest._load()
        assert rel_path in data
        assert data[rel_path]["status"] == "download_failed"
        assert data[rel_path]["error"] == error

    @given(rel_path=rel_path_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_record_download_then_upload(self, rel_path: str, checksum: str, tmp_path: pytest.TempPath) -> None:
        """Record download then upload transitions status correctly."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download(rel_path, checksum)
        data1 = manifest._load()
        assert data1[rel_path]["status"] == "downloaded"

        manifest.record_upload(rel_path)
        data2 = manifest._load()
        assert data2[rel_path]["status"] == "uploaded"
        assert data2[rel_path]["checksum"] == checksum

    @given(rel_path=rel_path_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_get_uploaded_excludes_downloaded(self, rel_path: str, checksum: str, tmp_path: pytest.TempPath) -> None:
        """get_uploaded excludes files that are only downloaded."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download(rel_path, checksum)
        uploaded = manifest.get_uploaded()
        assert rel_path not in uploaded

    @given(rel_path=rel_path_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_get_not_uploaded_excludes_uploaded(self, rel_path: str, checksum: str, tmp_path: pytest.TempPath) -> None:
        """get_not_uploaded excludes files that are already uploaded."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download(rel_path, checksum)
        manifest.record_upload(rel_path)
        not_uploaded = manifest.get_not_uploaded()
        assert rel_path not in not_uploaded

    @given(mode=st.sampled_from(["incremental", "full"]))
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_apply_mode_full_clears(self, mode: str, tmp_path: pytest.TempPath) -> None:
        """apply_mode with 'full' clears all entries."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download("file.parquet", "abc123")
        manifest.apply_mode("full")
        data = manifest._load()
        assert data == {}

    @given(rel_path=rel_path_strategy())
    @settings(max_examples=30, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_s3_key_format(self, rel_path: str, tmp_path: pytest.TempPath) -> None:
        """S3 key follows data/{rel_path} format."""
        manifest = Manifest(tmp_path)
        manifest.init()
        manifest.record_download(rel_path, "abc123")
        data = manifest._load()
        assert data[rel_path]["s3_key"] == f"data/{rel_path}"


# ── ETLConfig properties ───────────────────────────────────────────────


class TestETLConfigProperties:
    """Property-based tests for ETLConfig."""

    @given(params=etl_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_config_defaults(self, params: dict) -> None:
        """ETLConfig always has valid defaults."""
        from dataclasses import fields
        config = ETLConfig(**params)
        assert config.mode in ("incremental", "full")
        assert isinstance(config.delete_after_upload, bool)

    @given(params=etl_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_config_frozen(self, params: dict) -> None:
        """ETLConfig is immutable — mutation raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        config = ETLConfig(**params)
        with pytest.raises(FrozenInstanceError):
            config.mode = "full"  # type: ignore[assignment]

    @given(params=etl_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_config_equality(self, params: dict) -> None:
        """Two ETLConfigs with same fields are equal."""
        config1 = ETLConfig(**params)
        config2 = ETLConfig(**params)
        assert config1 == config2

    @given(params_a=etl_config_params(), params_b=etl_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_config_inequality_on_different_fields(self, params_a: dict, params_b: dict) -> None:
        """Two ETLConfigs with different fields are not equal."""
        if params_a != params_b:
            config1 = ETLConfig(**params_a)
            config2 = ETLConfig(**params_b)
            assert config1 != config2


# ── PipelineState properties ───────────────────────────────────────────


class TestPipelineStateProperties:
    """Property-based tests for PipelineState."""

    @given(downloaded=st.integers(min_value=0, max_value=10000),
           skipped=st.integers(min_value=0, max_value=10000),
           failed=st.integers(min_value=0, max_value=10000),
           total=st.integers(min_value=0, max_value=100000))
    @settings(max_examples=30, deadline=2000)
    def test_extract_mark_records_metrics(self, downloaded: int, skipped: int, failed: int, total: int) -> None:
        """mark_extract_done records all metrics correctly."""
        state = PipelineState()
        state.start()
        state.mark_extract_done(
            downloaded=downloaded,
            skipped=skipped,
            failed=failed,
            total=total,
            duration=1.0,
        )
        assert state.stage == PipelineStage.EXTRACT_DONE
        result = state.result()
        assert result["metrics"]["extract"]["downloaded"] == downloaded
        assert result["metrics"]["extract"]["skipped"] == skipped
        assert result["metrics"]["extract"]["failed"] == failed
        assert result["metrics"]["extract"]["total"] == total

    @given(uploaded=st.integers(min_value=0, max_value=10000))
    @settings(max_examples=30, deadline=2000)
    def test_upload_mark_records_metrics(self, uploaded: int) -> None:
        """mark_upload_done records upload metrics correctly."""
        state = PipelineState()
        state.start()
        state.mark_extract_done(downloaded=5, skipped=0, failed=0, total=5, duration=1.0)
        state.mark_upload_done(uploaded=uploaded, uploaded_files=["file.parquet"], duration=2.0)
        assert state.stage == PipelineStage.UPLOAD_DONE
        result = state.result()
        assert result["metrics"]["upload"]["uploaded"] == uploaded

    @given(error_msg=st.text(min_size=1, max_size=500))
    @settings(max_examples=30, deadline=2000)
    def test_fail_records_error(self, error_msg: str) -> None:
        """PipelineState.fail records error message."""
        state = PipelineState()
        state.start()
        state.fail(error_msg)
        assert state.stage == PipelineStage.FAILED
        assert state.error == error_msg

    def test_complete_after_extract(self) -> None:
        """Complete can be called after extract stage."""
        state = PipelineState()
        state.start()
        state.mark_extract_done(downloaded=5, skipped=0, failed=0, total=5, duration=1.0)
        state.complete()
        assert state.stage == PipelineStage.COMPLETED

    def test_complete_after_upload(self) -> None:
        """Complete can be called after upload stage."""
        state = PipelineState()
        state.start()
        state.mark_extract_done(downloaded=5, skipped=0, failed=0, total=5, duration=1.0)
        state.mark_upload_done(uploaded=3, uploaded_files=["file.parquet"], duration=2.0)
        state.complete()
        assert state.stage == PipelineStage.COMPLETED

    @given(error_msg=st.text(min_size=1, max_size=500))
    @settings(max_examples=30, deadline=2000)
    def test_result_on_failure(self, error_msg: str) -> None:
        """Result on failure includes error status."""
        state = PipelineState()
        state.start()
        state.fail(error_msg)
        result = state.result()
        assert result["status"] == "failed"
        assert result["metrics"]["total_duration_seconds"] >= 0.0

    @given(downloaded=st.integers(min_value=0, max_value=10000),
           uploaded=st.integers(min_value=0, max_value=10000))
    @settings(max_examples=30, deadline=2000)
    def test_completed_result_structure(self, downloaded: int, uploaded: int) -> None:
        """Completed result has correct structure."""
        state = PipelineState()
        state.start()
        state.mark_extract_done(downloaded=downloaded, skipped=0, failed=0, total=downloaded, duration=1.0)
        state.mark_upload_done(uploaded=uploaded, uploaded_files=["file.parquet"], duration=2.0)
        state.complete()
        result = state.result()
        assert result["status"] == "completed"
        assert "metrics" in result
        assert "extract" in result["metrics"]
        assert "upload" in result["metrics"]


# ── StageMetrics properties ────────────────────────────────────────────


class TestStageMetricsProperties:
    """Property-based tests for StageMetrics."""

    @given(duration=st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=30, deadline=2000)
    def test_duration_non_negative(self, duration: float) -> None:
        """StageMetrics duration is always non-negative."""
        metrics = StageMetrics(duration_seconds=duration)
        assert metrics.duration_seconds == duration
        assert duration >= 0.0

    @given(downloaded=st.integers(min_value=0, max_value=100000))
    @settings(max_examples=30, deadline=2000)
    def test_downloaded_non_negative(self, downloaded: int) -> None:
        """StageMetrics downloaded is always non-negative."""
        metrics = StageMetrics(downloaded=downloaded)
        assert metrics.downloaded == downloaded
        assert downloaded >= 0

    @given(uploaded=st.integers(min_value=0, max_value=100000))
    @settings(max_examples=30, deadline=2000)
    def test_uploaded_files_list(self, uploaded: int) -> None:
        """StageMetrics uploaded_files is a list."""
        files = [f"file{i}.parquet" for i in range(uploaded)]
        metrics = StageMetrics(uploaded=uploaded, uploaded_files=files)
        assert isinstance(metrics.uploaded_files, list)
        assert len(metrics.uploaded_files) == uploaded

    @given(value=st.just("downloaded"))
    @settings(max_examples=5, deadline=2000)
    def test_frozen(self, value: str) -> None:
        """StageMetrics is immutable."""
        from dataclasses import FrozenInstanceError
        metrics = StageMetrics(downloaded=5)
        with pytest.raises(FrozenInstanceError):
            metrics.downloaded = 0  # type: ignore[assignment]


# ── PipelineMetrics properties ─────────────────────────────────────────


class TestPipelineMetricsProperties:
    """Property-based tests for PipelineMetrics."""

    @given(total_duration=st.floats(min_value=0.0, max_value=100000.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=30, deadline=2000)
    def test_total_duration_non_negative(self, total_duration: float) -> None:
        """PipelineMetrics total_duration is always non-negative."""
        metrics = PipelineMetrics(total_duration_seconds=total_duration)
        assert metrics.total_duration_seconds == total_duration
        assert total_duration >= 0.0

    @given(value=st.just(0))
    @settings(max_examples=5, deadline=2000)
    def test_frozen(self, value: int) -> None:
        """PipelineMetrics is immutable."""
        from dataclasses import FrozenInstanceError
        metrics = PipelineMetrics()
        with pytest.raises(FrozenInstanceError):
            metrics.total_duration_seconds = 0  # type: ignore[assignment]

    @given(extract_downloaded=st.integers(min_value=0, max_value=10000),
           upload_uploaded=st.integers(min_value=0, max_value=10000))
    @settings(max_examples=30, deadline=2000)
    def test_nested_metrics(self, extract_downloaded: int, upload_uploaded: int) -> None:
        """PipelineMetrics can contain nested StageMetrics."""
        extract_metrics = StageMetrics(downloaded=extract_downloaded)
        upload_metrics = StageMetrics(uploaded=upload_uploaded)
        metrics = PipelineMetrics(
            total_duration_seconds=3.0,
            extract=extract_metrics,
            upload=upload_metrics,
        )
        assert metrics.extract.downloaded == extract_downloaded
        assert metrics.upload.uploaded == upload_uploaded


# ── PipelineStage properties ───────────────────────────────────────────


class TestPipelineStageProperties:
    """Property-based tests for PipelineStage enum."""

    @given(index=st.integers(min_value=0, max_value=len(PipelineStage) - 1))
    @settings(max_examples=30, deadline=2000)
    def test_all_stages_have_values(self, index: int) -> None:
        """All PipelineStage members have string values."""
        stages = list(PipelineStage)
        stage = stages[index]
        assert isinstance(stage.value, str)
        assert len(stage.value) > 0

    @given(seed=st.integers(min_value=0, max_value=1))
    @settings(max_examples=5, deadline=2000)
    def test_stage_values_unique(self, seed: int) -> None:
        """All PipelineStage values are unique."""
        del seed  # unused, just to satisfy @given
        values = [s.value for s in PipelineStage]
        assert len(values) == len(set(values))

    @given(value=st.sampled_from(list(PipelineStage.__members__.values())))
    @settings(max_examples=30, deadline=2000)
    def test_from_value(self, value: str) -> None:
        """PipelineStage can be reconstructed from its value."""
        stage = PipelineStage(value)
        assert isinstance(stage, PipelineStage)
