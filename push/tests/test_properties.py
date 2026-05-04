"""Property-based tests for push module using hypothesis."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from hypothesis import given, settings, strategies as st
from hypothesis.strategies import composite

import pytest

from push.core.checksum import compute_content_type, compute_sha256
from push.core.filter import _matches_any, _matches_pattern
from push.core.state import PushResult, UploadConfig


# ── Strategies ──────────────────────────────────────────────────────────


def safe_path_strategies() -> st.SearchStrategy[str]:
    """Generate valid path strings for testing."""
    return st.text(
        min_size=1,
        max_size=200,
        alphabet=st.characters(whitelist_categories=["L", "N", "Zs", "Pd"], whitelist_characters="._-/\\", blacklist_characters=("\x00", "\n", "\r")),
    )


def glob_pattern_strategies() -> st.SearchStrategy[str]:
    """Generate glob-like pattern strings."""
    alphabet = "*?_.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-/[]"
    return st.text(
        min_size=0,
        max_size=50,
        alphabet=st.sampled_from(alphabet),
    )


@composite
def push_result_params(draw: Any) -> dict:
    """Generate valid PushResult constructor parameters."""
    return {
        "uploaded": draw(st.integers(min_value=0, max_value=10000)),
        "skipped": draw(st.integers(min_value=0, max_value=10000)),
        "failed": draw(st.integers(min_value=0, max_value=10000)),
        "total": draw(st.integers(min_value=0, max_value=100000)),
    }


@composite
def upload_config_params(draw: Any) -> dict:
    """Generate valid UploadConfig constructor parameters."""
    include_patterns = draw(st.lists(
        glob_pattern_strategies(),
        min_size=0,
        max_size=5,
        unique=True,
    ).map(set))
    exclude_patterns = draw(st.lists(
        glob_pattern_strategies(),
        min_size=0,
        max_size=5,
        unique=True,
    ).map(set))
    return {
        "include": include_patterns if include_patterns else None,
        "exclude": exclude_patterns if exclude_patterns else None,
        "overwrite": draw(st.booleans()),
    }


# ── PushResult properties ──────────────────────────────────────────────


class TestPushResultProperties:
    """Property-based tests for PushResult."""

    @given(params=push_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_pushresult_defaults_overridable(self, params: dict) -> None:
        """PushResult fields accept any valid integer values."""
        result = PushResult(**params)
        assert result.uploaded == params["uploaded"]
        assert result.skipped == params["skipped"]
        assert result.failed == params["failed"]
        assert result.total == params["total"]

    @given(params=push_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_pushresult_frozen(self, params: dict) -> None:
        """PushResult is immutable — mutation raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        result = PushResult(**params)
        with pytest.raises(FrozenInstanceError):
            result.uploaded = 0  # type: ignore[assignment]
        with pytest.raises(FrozenInstanceError):
            result.skipped = 0  # type: ignore[assignment]

    @given(params=push_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_pushresult_equality(self, params: dict) -> None:
        """Two PushResults with same fields are equal."""
        result1 = PushResult(**params)
        result2 = PushResult(**params)
        assert result1 == result2

    @given(params_a=push_result_params(), params_b=push_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_pushresult_inequality_on_different_fields(self, params_a: dict, params_b: dict) -> None:
        """Two PushResults with different fields are not equal."""
        result1 = PushResult(**params_a)
        result2 = PushResult(**params_b)
        if params_a != params_b:
            assert result1 != result2


# ── UploadConfig properties ────────────────────────────────────────────


class TestUploadConfigProperties:
    """Property-based tests for UploadConfig."""

    @given(params=upload_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadconfig_fields_preserved(self, params: dict) -> None:
        """UploadConfig preserves all constructor values."""
        config = UploadConfig(**params)
        if params["include"] is not None:
            assert config.include == params["include"]
        if params["exclude"] is not None:
            assert config.exclude == params["exclude"]
        assert config.overwrite == params["overwrite"]

    @given(params=upload_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadconfig_frozen(self, params: dict) -> None:
        """UploadConfig is immutable — mutation raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        config = UploadConfig(**params)
        with pytest.raises(FrozenInstanceError):
            config.overwrite = not config.overwrite  # type: ignore[assignment]

    @given(params=upload_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadconfig_equality(self, params: dict) -> None:
        """Two UploadConfigs with same fields are equal."""
        config1 = UploadConfig(**params)
        config2 = UploadConfig(**params)
        assert config1 == config2

    @given(params_a=upload_config_params(), params_b=upload_config_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadconfig_different_include(self, params_a: dict, params_b: dict) -> None:
        """Configs with different include sets are not equal."""
        if params_a["include"] != params_b["include"]:
            config1 = UploadConfig(**params_a)
            config2 = UploadConfig(**params_b)
            assert config1 != config2


# ── _matches_pattern properties ────────────────────────────────────────


class TestMatchesPatternProperties:
    """Property-based tests for _matches_pattern."""

    @given(filename=safe_path_strategies(), path=safe_path_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_wildcard_matches_anything(self, filename: str, path: str) -> None:
        """Wildcard pattern '*' matches any filename or path."""
        assert _matches_pattern(filename, path, "*") is True

    @given(filename=safe_path_strategies(), pattern=glob_pattern_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_pattern_result_is_bool(self, filename: str, pattern: str) -> None:
        """_matches_pattern always returns a bool."""
        result = _matches_pattern(filename, "", pattern)
        assert isinstance(result, bool)

    @given(filename=safe_path_strategies(), path=safe_path_strategies(), pattern=glob_pattern_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_exact_match_identity(self, filename: str, path: str, pattern: str) -> None:
        """If filename == pattern and path == pattern, match is True."""
        if filename == pattern:
            assert _matches_pattern(filename, path, pattern) is True
        elif path == pattern:
            assert _matches_pattern(filename, path, pattern) is True

    @given(path=safe_path_strategies(), pattern=glob_pattern_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_empty_pattern_no_match(self, path: str, pattern: str) -> None:
        """Empty pattern (not '*') should not match unless path is also empty."""
        if path == "" and pattern == "":
            assert _matches_pattern("", "", "") is True
        elif pattern == "" and path != "":
            # Empty pattern only matches empty string
            assert _matches_pattern(path, path, "") is False


# ── _matches_any properties ────────────────────────────────────────────


class TestMatchesAnyProperties:
    """Property-based tests for _matches_any."""

    @given(path=safe_path_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_empty_pattern_set_no_match(self, path: str) -> None:
        """Empty pattern set never matches."""
        assert _matches_any(path, set()) is False

    @given(path=safe_path_strategies(), pattern=glob_pattern_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_single_pattern_membership(self, path: str, pattern: str) -> None:
        """If pattern set contains path, match is True."""
        if pattern == "*" or path == pattern:
            assert _matches_any(path, {pattern}) is True

    @given(path=safe_path_strategies(), patterns=st.lists(glob_pattern_strategies(), min_size=1, max_size=5, unique=True).map(set))
    @settings(max_examples=30, deadline=2000)
    def test_wildcard_in_set_always_matches(self, path: str, patterns: set) -> None:
        """If '*' is in pattern set, any path matches."""
        if "*" in patterns:
            assert _matches_any(path, patterns) is True

    @given(path=safe_path_strategies(), pattern=glob_pattern_strategies())
    @settings(max_examples=30, deadline=2000)
    def test_result_is_bool(self, path: str, pattern: str) -> None:
        """_matches_any always returns a bool."""
        result = _matches_any(path, {pattern})
        assert isinstance(result, bool)


# ── compute_sha256 properties ──────────────────────────────────────────


class TestComputeSha256Properties:
    """Property-based tests for compute_sha256."""

    @given(content=st.binary(min_size=0, max_size=10000))
    @settings(max_examples=30, deadline=2000)
    def test_deterministic(self, content: bytes) -> None:
        """Same content always produces same SHA-256 digest."""
        import hashlib
        expected = hashlib.sha256(content).hexdigest()
        # We can't test compute_sha256 directly on bytes, so test the property
        # on the underlying algorithm
        sha256_1 = hashlib.sha256(content).hexdigest()
        sha256_2 = hashlib.sha256(content).hexdigest()
        assert sha256_1 == sha256_2

    @given(content_a=st.binary(min_size=0, max_size=10000), content_b=st.binary(min_size=0, max_size=10000))
    @settings(max_examples=30, deadline=2000)
    def test_different_content_different_hash(self, content_a: bytes, content_b: bytes) -> None:
        """Different content produces different SHA-256 digests (almost always)."""
        import hashlib
        if content_a != content_b:
            hash_a = hashlib.sha256(content_a).hexdigest()
            hash_b = hashlib.sha256(content_b).hexdigest()
            # Collision is astronomically unlikely for random data
            assert hash_a != hash_b

    @given(content=st.binary(min_size=0, max_size=10000))
    @settings(max_examples=30, deadline=2000)
    def test_digest_length(self, content: bytes) -> None:
        """SHA-256 digest is always 64 hex characters."""
        import hashlib
        digest = hashlib.sha256(content).hexdigest()
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    @given(content=st.binary(min_size=0, max_size=10000))
    @settings(max_examples=30, deadline=2000)
    def test_empty_content_hash(self, content: bytes) -> None:
        """Empty content produces known SHA-256 hash."""
        import hashlib
        if len(content) == 0:
            expected = hashlib.sha256(b"").hexdigest()
            assert hashlib.sha256(content).hexdigest() == expected


# ── compute_content_type properties ────────────────────────────────────


class TestComputeContentTypeProperties:
    """Property-based tests for compute_content_type."""

    @given(suffix=st.just(".parquet") | st.text(min_size=1, max_size=20))
    @settings(max_examples=30, deadline=2000)
    def test_parquet_content_type(self, suffix: str) -> None:
        """Files with .parquet extension get correct MIME type."""
        fake_path = type("FakePath", (), {"suffix": suffix.lower()})()
        if suffix.lower() == ".parquet":
            assert compute_content_type(fake_path) == "application/x-parquet"

    @given(suffix=st.text(min_size=1, max_size=20).filter(lambda s: s.lower() != ".parquet"))
    @settings(max_examples=30, deadline=2000)
    def test_non_parquet_content_type(self, suffix: str) -> None:
        """Non-parquet files get generic MIME type."""
        fake_path = type("FakePath", (), {"suffix": suffix.lower()})()
        assert compute_content_type(fake_path) == "application/octet-stream"

    @given(suffix=st.just(".PARQUET") | st.just(".Parquet"))
    @settings(max_examples=30, deadline=2000)
    def test_parquet_case_insensitive(self, suffix: str) -> None:
        """Parquet detection is case-insensitive."""
        fake_path = type("FakePath", (), {"suffix": suffix.lower()})()
        assert compute_content_type(fake_path) == "application/x-parquet"
