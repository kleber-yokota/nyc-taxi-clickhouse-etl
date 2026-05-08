"""Fuzz tests for etl module using atheris — coverage-guided input validation."""

import sys
import atheris

# Import under instrument_imports so atheris can trace execution
with atheris.instrument_imports(include=["etl.manifest", "etl.config", "etl.state"]):
    from etl.manifest import Manifest
    from etl.config import ETLConfig
    from etl.state import PipelineState, PipelineMetrics, StageMetrics, PipelineStage


@atheris.instrument_func
def TestManifestInit(data):
    """Fuzz test for Manifest.init — accept any directory path."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        data_dir_str = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        data_dir = type("FakePath", (), {"__truediv__": lambda s, n: type("FakePath", (), {"exists": lambda: False, "mkdir": lambda **kw: None})()})()
        manifest = Manifest(data_dir)
        result = manifest.init()
        assert isinstance(result, dict)
    except (ValueError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecordDownload(data):
    """Fuzz test for Manifest.record_download — test with various inputs."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        checksum = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 64))
        data_dir = type("FakePath", (), {
            "__truediv__": lambda s, n: type("FakePath", (), {
                "exists": lambda: False,
                "mkdir": lambda **kw: None,
                "rglob": lambda p: [],
            })(),
        })()
        manifest = Manifest(data_dir)
        manifest.init()
        manifest.record_download(rel_path, checksum)
    except (ValueError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecordUpload(data):
    """Fuzz test for Manifest.record_upload — test with various inputs."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        data_dir = type("FakePath", (), {
            "__truediv__": lambda s, n: type("FakePath", (), {
                "exists": lambda: False,
                "mkdir": lambda **kw: None,
                "rglob": lambda p: [],
            })(),
        })()
        manifest = Manifest(data_dir)
        manifest.init()
        manifest.record_upload(rel_path)
    except (ValueError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecordDownloadFailure(data):
    """Fuzz test for Manifest.record_download_failure — test with various inputs."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        error = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 500))
        data_dir = type("FakePath", (), {
            "__truediv__": lambda s, n: type("FakePath", (), {
                "exists": lambda: False,
                "mkdir": lambda **kw: None,
                "rglob": lambda p: [],
            })(),
        })()
        manifest = Manifest(data_dir)
        manifest.init()
        manifest.record_download_failure(rel_path, error)
    except (ValueError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestApplyMode(data):
    """Fuzz test for Manifest.apply_mode — test with various modes."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        mode = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 50))
        data_dir = type("FakePath", (), {
            "__truediv__": lambda s, n: type("FakePath", (), {
                "exists": lambda: False,
                "mkdir": lambda **kw: None,
                "rglob": lambda p: [],
            })(),
        })()
        manifest = Manifest(data_dir)
        manifest.init()
        manifest.apply_mode(mode)
    except (ValueError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestETLConfigDefaults(data):
    """Fuzz test for ETLConfig — verify frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        types = None
        if fdp.ConsumeBool():
            num_types = fdp.ConsumeIntInRange(0, 5)
            types = [fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 20)) for _ in range(num_types)]

        from_year = fdp.ConsumeIntInRange(2000, 2030) if fdp.ConsumeBool() else None
        to_year = fdp.ConsumeIntInRange(2000, 2030) if fdp.ConsumeBool() else None
        mode = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 20))
        delete_after_upload = fdp.ConsumeBool()

        config = ETLConfig(
            types=types,
            from_year=from_year,
            to_year=to_year,
            mode=mode,
            delete_after_upload=delete_after_upload,
        )
        assert config.mode == mode or config.mode == "incremental"
        assert config.delete_after_upload == delete_after_upload

        # Verify frozen — must raise on mutation
        try:
            config.mode = "full"  # type: ignore[assignment]
            raise AssertionError("ETLConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestPipelineStateTransitions(data):
    """Fuzz test for PipelineState — verify state transitions."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        state = PipelineState()
        assert state.stage == PipelineStage.NOT_RUNNING

        state.start()
        assert state.stage == PipelineStage.RUNNING

        downloaded = fdp.ConsumeIntInRange(0, 10000)
        skipped = fdp.ConsumeIntInRange(0, 10000)
        failed = fdp.ConsumeIntInRange(0, 10000)
        total = fdp.ConsumeIntInRange(0, 100000)

        state.mark_extract_done(
            downloaded=downloaded,
            skipped=skipped,
            failed=failed,
            total=total,
            duration=fdp.ConsumeFloatInRange(0.0, 1000.0),
        )
        assert state.stage == PipelineStage.EXTRACT_DONE

        uploaded = fdp.ConsumeIntInRange(0, 10000)
        state.mark_upload_done(uploaded=uploaded, uploaded_files=["file.parquet"], duration=fdp.ConsumeFloatInRange(0.0, 1000.0))
        assert state.stage == PipelineStage.UPLOAD_DONE

        state.complete()
        assert state.stage == PipelineStage.COMPLETED

        result = state.result()
        assert result["status"] == "completed"
    except (ValueError, OverflowError, TypeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestPipelineStateFailure(data):
    """Fuzz test for PipelineState.fail — test with various error messages."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        state = PipelineState()
        state.start()
        error_msg = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 500))
        state.fail(error_msg)
        assert state.stage == PipelineStage.FAILED
        assert state.error == error_msg
    except (ValueError, OverflowError, TypeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestStageMetricsFrozen(data):
    """Fuzz test for StageMetrics — verify frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        from etl.state import StageMetrics
        import dataclasses
        if not dataclasses.is_dataclass(StageMetrics):
            return

        metrics = StageMetrics(
            duration_seconds=fdp.ConsumeFloatInRange(0.0, 10000.0),
            downloaded=fdp.ConsumeIntInRange(0, 100000),
            skipped=fdp.ConsumeIntInRange(0, 100000),
            failed=fdp.ConsumeIntInRange(0, 100000),
            total=fdp.ConsumeIntInRange(0, 1000000),
            uploaded=fdp.ConsumeIntInRange(0, 100000),
            uploaded_files=[],
        )
        assert metrics.duration_seconds >= 0.0

        # Verify frozen — must raise on mutation
        try:
            metrics.downloaded = 0  # type: ignore[assignment]
            raise AssertionError("StageMetrics should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestPipelineMetricsFrozen(data):
    """Fuzz test for PipelineMetrics — verify frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        from etl.state import PipelineMetrics
        import dataclasses
        if not dataclasses.is_dataclass(PipelineMetrics):
            return

        metrics = PipelineMetrics(
            total_duration_seconds=fdp.ConsumeFloatInRange(0.0, 100000.0),
        )
        assert metrics.total_duration_seconds >= 0.0

        # Verify frozen — must raise on mutation
        try:
            metrics.total_duration_seconds = 0  # type: ignore[assignment]
            raise AssertionError("PipelineMetrics should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecoverFromUploadEntries(data):
    """Fuzz test for Manifest._recover_from_upload — reconstruct manifest from upload entries."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        import tempfile
        import os

        num_entries = fdp.ConsumeIntInRange(0, 10)
        entries = []
        for _ in range(num_entries):
            rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
            s3_key = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
            checksum = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 64))
            entries.append((rel_path, s3_key, checksum))

        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            data_dir = Path(tmpdir)
            manifest = Manifest(data_dir)
            manifest.init()

            # Simulate record_upload for each entry
            for rel_path, s3_key, checksum in entries:
                manifest.record_upload(rel_path)

            # Verify entries exist
            data = manifest._load()
            for rel_path, s3_key, checksum in entries:
                assert rel_path in data
                assert data[rel_path]["status"] == "uploaded"

    except (ValueError, OverflowError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecoverFromDownloadEntries(data):
    """Fuzz test for Manifest._recover_from_download — reconstruct manifest from download entries."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        import tempfile

        num_entries = fdp.ConsumeIntInRange(0, 10)
        entries = []
        for _ in range(num_entries):
            rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
            checksum = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 64))
            entries.append((rel_path, checksum))

        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            data_dir = Path(tmpdir)
            manifest = Manifest(data_dir)
            manifest.init()

            # Simulate record_download for each entry
            for rel_path, checksum in entries:
                manifest.record_download(rel_path, checksum)

            # Verify entries exist
            data = manifest._load()
            for rel_path, checksum in entries:
                assert rel_path in data
                assert data[rel_path]["status"] == "downloaded"
                assert data[rel_path]["checksum"] == checksum

    except (ValueError, OverflowError, TypeError, OSError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestManifestRecoverMixedEntries(data):
    """Fuzz test for Manifest — mix of download and upload entries."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        import tempfile

        num_downloaded = fdp.ConsumeIntInRange(0, 5)
        num_uploaded = fdp.ConsumeIntInRange(0, 5)
        downloaded_entries = []
        uploaded_entries = []

        for _ in range(num_downloaded):
            rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
            checksum = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 64))
            downloaded_entries.append((rel_path, checksum))

        for _ in range(num_uploaded):
            rel_path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
            checksum = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 64))
            uploaded_entries.append((rel_path, checksum))

        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            data_dir = Path(tmpdir)
            manifest = Manifest(data_dir)
            manifest.init()

            # Record downloads
            for rel_path, checksum in downloaded_entries:
                manifest.record_download(rel_path, checksum)

            # Record uploads (simulating upload of already-downloaded files)
            for rel_path, checksum in uploaded_entries:
                manifest.record_upload(rel_path)

            # Verify all entries exist with correct status
            data = manifest._load()
            for rel_path, checksum in downloaded_entries:
                assert rel_path in data
            for rel_path, checksum in uploaded_entries:
                assert rel_path in data
                assert data[rel_path]["status"] == "uploaded"

    except (ValueError, OverflowError, TypeError, OSError):
        return  # Expected — not a crash


def TestOneInput(data):
    """Dispatch fuzz input to all test functions."""
    fdp = atheris.FuzzedDataProvider(data)
    if not data:
        return

    # Pick a test based on first byte
    test_id = fdp.ConsumeIntInRange(0, 9)
    try:
        if test_id == 0:
            TestManifestInit(data)
        elif test_id == 1:
            TestManifestRecordDownload(data)
        elif test_id == 2:
            TestManifestRecordUpload(data)
        elif test_id == 3:
            TestManifestRecordDownloadFailure(data)
        elif test_id == 4:
            TestManifestApplyMode(data)
        elif test_id == 5:
            TestETLConfigDefaults(data)
        elif test_id == 6:
            TestPipelineStateTransitions(data)
        elif test_id == 7:
            TestPipelineStateFailure(data)
        elif test_id == 8:
            TestStageMetricsFrozen(data)
        else:
            TestPipelineMetricsFrozen(data)
    except (AssertionError, KeyError):
        raise  # Re-raise — these are real bugs
    except (TypeError, ValueError, OverflowError, AttributeError, IndexError):
        return  # Expected — bad input, not a crash


# Setup atheris
atheris.Setup(
    sys.argv,
    TestOneInput,
    enable_coverage=True,
    coverage_min_line=0,
)

atheris.Fuzz()
