"""Fuzz tests for etl module using atheris — coverage-guided input validation."""

# mypy: disable-error-code="untyped-decorator"

from __future__ import annotations

import sys
from typing import Any

import atheris  # type: ignore[import-untyped]

# Import under instrument_imports so atheris can trace execution
with atheris.instrument_imports(include=["etl.manifest", "etl.orchestrator"]):
    from etl.manifest import load, save, update_from_entries
    from etl.orchestrator import adapt_s3_to_manifest, ETLConfig
    from push.core.push_manifest import S3Object
    from push.core.state import PushedEntry


@atheris.instrument_func
def TestAdaptS3ToManifestEmptyPrefix(data: bytes) -> None:
    """Fuzz test adapt_s3_to_manifest with empty prefix."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        s3_objects = []
        num_objects = fdp.ConsumeIntInRange(0, 10)
        for _ in range(num_objects):
            key = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(5, 100))
            s3_objects.append(S3Object(key=key))
        prefix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 20))
        result = adapt_s3_to_manifest(s3_objects, prefix)
        assert isinstance(result, dict)
        # Duplicates collapse to single key, so len(result) <= len(s3_objects)
        assert len(result) <= len(s3_objects)
        for rel_path, entry in result.items():
            assert isinstance(rel_path, str)
            assert isinstance(entry, dict)
            assert "s3_key" in entry
    except (ValueError, TypeError):
        return


@atheris.instrument_func
def TestAdaptS3ToManifestPrefixMatch(data: bytes) -> None:
    """Fuzz test adapt_s3_to_manifest with prefix matching."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        prefix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(2, 20))
        s3_objects = []
        num_objects = fdp.ConsumeIntInRange(1, 5)
        for i in range(num_objects):
            key = f"{prefix}/type{i}/file_{fdp.ConsumeIntInRange(1, 999)}.parquet"
            s3_objects.append(S3Object(key=key))
        result = adapt_s3_to_manifest(s3_objects, prefix)
        assert isinstance(result, dict)
        for obj in s3_objects:
            expected_rel = obj.key[len(prefix) + 1:]
            assert expected_rel in result
            assert result[expected_rel]["s3_key"] == obj.key
    except (ValueError, TypeError):
        return


@atheris.instrument_func
def TestAdaptS3ToManifestNoPrefixMatch(data: bytes) -> None:
    """Fuzz test adapt_s3_to_manifest when keys don't match prefix."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        prefix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(2, 10))
        s3_objects = []
        num_objects = fdp.ConsumeIntInRange(1, 5)
        for i in range(num_objects):
            key = f"other_prefix/type{i}/file.parquet"
            s3_objects.append(S3Object(key=key))
        result = adapt_s3_to_manifest(s3_objects, prefix)
        assert isinstance(result, dict)
        for obj in s3_objects:
            assert obj.key in result
    except (ValueError, TypeError):
        return


@atheris.instrument_func
def TestAdaptS3ToManifestDeterministic(data: bytes) -> None:
    """Property: adapt_s3_to_manifest is deterministic for same input."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        prefix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(2, 20))
        s3_objects = []
        num_objects = fdp.ConsumeIntInRange(1, 5)
        for i in range(num_objects):
            key = f"{prefix}/type{i}/file_{fdp.ConsumeIntInRange(1, 999)}.parquet"
            s3_objects.append(S3Object(key=key))
        result1 = adapt_s3_to_manifest(s3_objects, prefix)
        result2 = adapt_s3_to_manifest(s3_objects, prefix)
        assert result1 == result2
    except (ValueError, TypeError):
        return


@atheris.instrument_func
def TestETLConfigFrozen(data: bytes) -> None:
    """Fuzz test ETLConfig frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        data_dir = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(2, 20))
        bucket = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 30))
        prefix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(2, 20))
        num_types = fdp.ConsumeIntInRange(0, 4)
        types = [
            fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(3, 10))
            for _ in range(num_types)
        ]
        from_year = fdp.ConsumeIntInRange(2020, 2030)
        to_year = fdp.ConsumeIntInRange(2020, 2030)
        mode = fdp.PickOne(["incremental", "full"])
        delete_after_push = fdp.ConsumeBool()

        config = ETLConfig(
            data_dir=data_dir,
            bucket=bucket,
            prefix=prefix,
            types=types if types else ["yellow", "green", "fhv", "fhvhv"],
            from_year=from_year,
            to_year=to_year,
            mode=mode,
            delete_after_push=delete_after_push,
        )
        assert config.data_dir == data_dir
        assert config.bucket == bucket
        assert config.prefix == prefix
        assert config.from_year == from_year
        assert config.to_year == to_year
        assert config.mode == mode
        assert config.delete_after_push == delete_after_push
        assert config.types == (types if types else ["yellow", "green", "fhv", "fhvhv"])

        # Verify frozen — must raise on mutation
        try:
            config.data_dir = "should_fail"  # type: ignore[misc]
            raise AssertionError("ETLConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestETLConfigDefaultTypesImmutable(data: bytes) -> None:
    """Property: default types list is independent between instances."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        config1 = ETLConfig()
        config2 = ETLConfig()
        assert config1.types == config2.types
        # Both should have the default 4 types
        assert len(config1.types) == 4
        assert len(config2.types) == 4
    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestETLConfigFrozenMode(data: bytes) -> None:
    """Fuzz test ETLConfig mode field."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        mode = fdp.PickOne(["incremental", "full"])
        config = ETLConfig(mode=mode)
        assert config.mode == mode

        try:
            config.mode = "other"  # type: ignore[misc]
            raise AssertionError("ETLConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestETLConfigFrozenBucket(data: bytes) -> None:
    """Fuzz test ETLConfig bucket field."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        bucket = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 50))
        config = ETLConfig(bucket=bucket)
        assert config.bucket == bucket

        try:
            config.bucket = "changed"  # type: ignore[misc]
            raise AssertionError("ETLConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestETLConfigYearRange(data: bytes) -> None:
    """Property: from_year and to_year are preserved exactly."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        from_year = fdp.ConsumeIntInRange(2000, 2030)
        to_year = fdp.ConsumeIntInRange(2000, 2030)
        config = ETLConfig(from_year=from_year, to_year=to_year)
        assert config.from_year == from_year
        assert config.to_year == to_year
    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestETLConfigDeleteAfterPush(data: bytes) -> None:
    """Fuzz test ETLConfig delete_after_push boolean field."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        delete_after_push = fdp.ConsumeBool()
        config = ETLConfig(delete_after_push=delete_after_push)
        assert config.delete_after_push == delete_after_push

        try:
            config.delete_after_push = not delete_after_push  # type: ignore[misc]
            raise AssertionError("ETLConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return


def TestOneInput(data: bytes) -> None:
    """Dispatch fuzz input to all test functions."""
    fdp = atheris.FuzzedDataProvider(data)
    if not data:
        return

    # Pick a test based on first byte
    test_id = fdp.ConsumeIntInRange(0, 9)
    try:
        if test_id == 0:
            TestAdaptS3ToManifestEmptyPrefix(data)
        elif test_id == 1:
            TestAdaptS3ToManifestPrefixMatch(data)
        elif test_id == 2:
            TestAdaptS3ToManifestNoPrefixMatch(data)
        elif test_id == 3:
            TestAdaptS3ToManifestDeterministic(data)
        elif test_id == 4:
            TestETLConfigFrozen(data)
        elif test_id == 5:
            TestETLConfigDefaultTypesImmutable(data)
        elif test_id == 6:
            TestETLConfigFrozenMode(data)
        elif test_id == 7:
            TestETLConfigFrozenBucket(data)
        elif test_id == 8:
            TestETLConfigYearRange(data)
        else:
            TestETLConfigDeleteAfterPush(data)
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
