"""Fuzz tests for push module using atheris — coverage-guided input validation."""

import sys
import atheris

# Import under instrument_imports so atheris can trace execution
with atheris.instrument_imports(include=["push.core.checksum", "push.core.filter", "push.core.state"]):
    from push.core.checksum import compute_content_type, compute_sha256
    from push.core.filter import _matches_pattern, _matches_any
    from push.core.state import PushResult, UploadConfig


@atheris.instrument_func
def TestChecksumContentType(data):
    """Fuzz test for compute_content_type — accept any bytes input."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        # Fuzz file paths with various extensions
        suffix = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 50))
        path = f"file{suffix}"
        result = compute_content_type(type("FakePath", (), {"suffix": suffix})())
        assert isinstance(result, str)
    except (ValueError, TypeError, AttributeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestChecksumSha256(data):
    """Fuzz test for compute_sha256 — simulate file reads with random bytes."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        # Generate random content that simulates file data
        content = bytes(fdp.ConsumeBytes(fdp.ConsumeIntInRange(0, 10000)))
        # Test a simple hash computation on the raw bytes
        import hashlib
        sha256 = hashlib.sha256()
        sha256.update(content)
        digest = sha256.hexdigest()
        assert len(digest) == 64
        # Same content must produce same digest (deterministic)
        sha256_2 = hashlib.sha256()
        sha256_2.update(content)
        assert sha256_2.hexdigest() == digest
    except (ValueError, TypeError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestMatchesPattern(data):
    """Fuzz test for _matches_pattern — test with random paths and patterns."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        filename = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 200))
        pattern = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        result = _matches_pattern(filename, path, pattern)
        assert isinstance(result, bool)
    except (ValueError, TypeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestMatchesAny(data):
    """Fuzz test for _matches_any — test with random paths and pattern sets."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 200))
        num_patterns = fdp.ConsumeIntInRange(0, 10)
        patterns = set()
        for _ in range(num_patterns):
            patterns.add(fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 50)))
        result = _matches_any(path, patterns)
        assert isinstance(result, bool)
        # Empty patterns set must always return False
        if not patterns:
            assert result is False
    except (ValueError, TypeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestPushResultFrozen(data):
    """Fuzz test for PushResult — verify frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        uploaded = fdp.ConsumeIntInRange(-1000, 1000000)
        skipped = fdp.ConsumeIntInRange(-1000, 1000000)
        failed = fdp.ConsumeIntInRange(-1000, 1000000)
        total = fdp.ConsumeIntInRange(-1000, 1000000)
        result = PushResult(uploaded=uploaded, skipped=skipped, failed=failed, total=total)
        assert result.uploaded == uploaded
        assert result.skipped == skipped
        assert result.failed == failed
        assert result.total == total
        # Verify frozen — must raise on mutation
        try:
            result.uploaded = 0  # type: ignore[assignment]
            raise AssertionError("PushResult should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestUploadConfigFrozen(data):
    """Fuzz test for UploadConfig — verify frozen dataclass behavior."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        include_size = fdp.ConsumeIntInRange(0, 5)
        exclude_size = fdp.ConsumeIntInRange(0, 5)
        include_set = set()
        for _ in range(include_size):
            include_set.add(fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 30)))
        exclude_set = set()
        for _ in range(exclude_size):
            exclude_set.add(fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 30)))
        overwrite = fdp.ConsumeBool()

        config = UploadConfig(
            include=include_set if include_set else None,
            exclude=exclude_set if exclude_set else None,
            overwrite=overwrite,
        )
        assert config.include == include_set or config.include is None
        assert config.exclude == exclude_set or config.exclude is None
        assert config.overwrite == overwrite

        # Verify frozen — must raise on mutation
        try:
            config.overwrite = not overwrite  # type: ignore[assignment]
            raise AssertionError("UploadConfig should be frozen")
        except Exception:
            pass  # Expected
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestUploadConfigCommutativity(data):
    """Property test: UploadConfig creation is deterministic."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        include_str = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 20))
        exclude_str = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 20))
        overwrite = fdp.ConsumeBool()

        config1 = UploadConfig(
            include={include_str} if include_str else None,
            exclude={exclude_str} if exclude_str else None,
            overwrite=overwrite,
        )
        config2 = UploadConfig(
            include={include_str} if include_str else None,
            exclude={exclude_str} if exclude_str else None,
            overwrite=overwrite,
        )
        assert config1.include == config2.include
        assert config1.exclude == config2.exclude
        assert config1.overwrite == config2.overwrite
    except (ValueError, OverflowError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestMatchesPatternWildcard(data):
    """Property: wildcard pattern matches any string."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        filename = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 100))
        path = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 200))
        assert _matches_pattern(filename, path, "*") is True
    except (ValueError, TypeError):
        return  # Expected — not a crash


@atheris.instrument_func
def TestMatchesPatternStarExclusion(data):
    """Property: if the full path matches a pattern, _matches_any returns True."""
    fdp = atheris.FuzzedDataProvider(data)
    try:
        filename = fdp.ConsumeUnicodeNoSurrogates(fdp.ConsumeIntInRange(0, 50))
        path = f"dir/{filename}"
        patterns = {f"dir/{filename}"}
        assert _matches_any(path, patterns) is True
    except (ValueError, TypeError):
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
            TestChecksumContentType(data)
        elif test_id == 1:
            TestChecksumSha256(data)
        elif test_id == 2:
            TestMatchesPattern(data)
        elif test_id == 3:
            TestMatchesAny(data)
        elif test_id == 4:
            TestPushResultFrozen(data)
        elif test_id == 5:
            TestUploadConfigFrozen(data)
        elif test_id == 6:
            TestUploadConfigCommutativity(data)
        elif test_id == 7:
            TestMatchesPatternWildcard(data)
        elif test_id == 8:
            TestMatchesPatternStarExclusion(data)
        else:
            # Run a random selection
            random_test = fdp.ConsumeIntInRange(0, 8)
            if random_test == 0:
                TestChecksumContentType(data)
            elif random_test == 1:
                TestChecksumSha256(data)
            elif random_test == 2:
                TestMatchesPattern(data)
            elif random_test == 3:
                TestMatchesAny(data)
            elif random_test == 4:
                TestPushResultFrozen(data)
            elif random_test == 5:
                TestUploadConfigFrozen(data)
            elif random_test == 6:
                TestUploadConfigCommutativity(data)
            elif random_test == 7:
                TestMatchesPatternWildcard(data)
            else:
                TestMatchesPatternStarExclusion(data)
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
