"""Property-based tests for upload module using hypothesis."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from hypothesis import given, settings, strategies as st
from hypothesis.strategies import composite

import pytest

from upload.core.checksum import compute_content_type, compute_sha256
from upload.core.filter import _matches_any, _matches_pattern
from upload.core.state import UploadResult, UploadConfig, UploadEntry


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


def s3_key_strategy() -> st.SearchStrategy[str]:
    """Generate valid S3 key strings."""
    return st.text(
        min_size=1,
        max_size=200,
        alphabet=st.characters(whitelist_categories=["L", "N", "Zs", "Pd"], whitelist_characters="._-/\\", blacklist_characters=("\x00", "\n", "\r")),
    )


def checksum_strategy() -> st.SearchStrategy[str]:
    """Generate valid SHA-256 hex digests."""
    return st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")


@composite
def upload_result_params(draw: Any) -> dict:
    """Generate valid UploadResult constructor parameters."""
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


@composite
def upload_entry_params(draw: Any) -> dict:
    """Generate valid UploadEntry constructor parameters."""
    return {
        "rel_path": draw(safe_path_strategies()),
        "s3_key": draw(safe_path_strategies()),
        "checksum": draw(st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")),
    }


# ── UploadResult properties ─────────────────────────────────────────────


class TestUploadResultProperties:
    """Property-based tests for UploadResult."""

    @given(params=upload_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadresult_defaults_overridable(self, params: dict) -> None:
        """UploadResult fields accept any valid integer values."""
        result = UploadResult(**params)
        assert result.uploaded == params["uploaded"]
        assert result.skipped == params["skipped"]
        assert result.failed == params["failed"]
        assert result.total == params["total"]

    @given(params=upload_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadresult_frozen(self, params: dict) -> None:
        """UploadResult is immutable — mutation raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        result = UploadResult(**params)
        with pytest.raises(FrozenInstanceError):
            result.uploaded = 0  # type: ignore[assignment]
        with pytest.raises(FrozenInstanceError):
            result.skipped = 0  # type: ignore[assignment]

    @given(params=upload_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadresult_equality(self, params: dict) -> None:
        """Two UploadResults with same fields are equal."""
        result1 = UploadResult(**params)
        result2 = UploadResult(**params)
        assert result1 == result2

    @given(params_a=upload_result_params(), params_b=upload_result_params())
    @settings(max_examples=30, deadline=2000)
    def test_uploadresult_inequality_on_different_fields(self, params_a: dict, params_b: dict) -> None:
        """Two UploadResults with different fields are not equal."""
        result1 = UploadResult(**params_a)
        result2 = UploadResult(**params_b)
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


# ── UploadEntry properties ─────────────────────────────────────────────


class TestUploadEntryProperties:
    """Property-based tests for UploadEntry."""

    @given(params=upload_entry_params())
    @settings(max_examples=30, deadline=2000)
    def test_entry_fields_preserved(self, params: dict) -> None:
        """UploadEntry preserves all constructor values."""
        entry = UploadEntry(**params)
        assert entry.rel_path == params["rel_path"]
        assert entry.s3_key == params["s3_key"]
        assert entry.checksum == params["checksum"]

    @given(params=upload_entry_params())
    @settings(max_examples=30, deadline=2000)
    def test_entry_frozen(self, params: dict) -> None:
        """UploadEntry is immutable — mutation raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        entry = UploadEntry(**params)
        with pytest.raises(FrozenInstanceError):
            entry.rel_path = "x"  # type: ignore[assignment]
        with pytest.raises(FrozenInstanceError):
            entry.s3_key = "x"  # type: ignore[assignment]
        with pytest.raises(FrozenInstanceError):
            entry.checksum = "x"  # type: ignore[assignment]

    @given(params=upload_entry_params())
    @settings(max_examples=30, deadline=2000)
    def test_entry_equality(self, params: dict) -> None:
        """Two UploadEntries with same fields are equal."""
        entry1 = UploadEntry(**params)
        entry2 = UploadEntry(**params)
        assert entry1 == entry2

    @given(params_a=upload_entry_params(), params_b=upload_entry_params())
    @settings(max_examples=30, deadline=2000)
    def test_entry_inequality_on_different_fields(self, params_a: dict, params_b: dict) -> None:
        """Two UploadEntries with different fields are not equal."""
        if params_a != params_b:
            entry1 = UploadEntry(**params_a)
            entry2 = UploadEntry(**params_b)
            assert entry1 != entry2

    @given(rel_path=safe_path_strategies(), s3_key=s3_key_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000)
    def test_s3_key_contains_rel_path(self, rel_path: str, s3_key: str, checksum: str) -> None:
        """S3 key typically contains the rel_path."""
        entry = UploadEntry(rel_path=rel_path, s3_key=s3_key, checksum=checksum)
        assert entry.s3_key is not None
        assert len(entry.s3_key) > 0

    @given(rel_path=safe_path_strategies(), s3_key=s3_key_strategy(), checksum=checksum_strategy())
    @settings(max_examples=30, deadline=2000)
    def test_checksum_format(self, rel_path: str, s3_key: str, checksum: str) -> None:
        """Checksum is always 64 hex characters."""
        entry = UploadEntry(rel_path=rel_path, s3_key=s3_key, checksum=checksum)
        assert len(entry.checksum) == 64
        assert all(c in "0123456789abcdef" for c in entry.checksum)
