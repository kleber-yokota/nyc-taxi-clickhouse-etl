"""Unit tests for file filtering — target filter.py mutations."""

from __future__ import annotations

from pathlib import Path

from push.core.filter import (
    _collect_and_filter,
    _matches_any,
    _matches_pattern,
)


class TestMatchesPattern:
    """Tests for _matches_pattern — kill pattern matching mutations."""

    def test_exact_match(self) -> None:
        assert _matches_pattern("file.parquet", "file.parquet", "file.parquet") is True

    def test_wildcard_match(self) -> None:
        assert _matches_pattern("file.parquet", "file.parquet", "*.parquet") is True

    def test_no_match(self) -> None:
        assert _matches_pattern("file.txt", "file.txt", "*.parquet") is False

    def test_star_pattern(self) -> None:
        assert _matches_pattern("anything", "anything", "*") is True

    def test_path_match(self) -> None:
        assert _matches_pattern("yellow.parquet", "yellow/file.parquet", "yellow*") is True

    def test_no_match_path(self) -> None:
        assert _matches_pattern("green.parquet", "green/file.parquet", "yellow*") is False

    def test_or_not_and_mutation(self) -> None:
        """Kills or→and mutation: filename matches but path doesn't."""
        assert _matches_pattern("file.parquet", "subdir/file.parquet", "file.parquet") is True


class TestMatchesAny:
    """Tests for _matches_any — kill exclusion pattern mutations."""

    def test_single_exclude(self) -> None:
        assert _matches_any("file.parquet", {".push_state.json"}) is False
        assert _matches_any(".push_state.json", {".push_state.json"}) is True

    def test_multiple_exclude(self) -> None:
        patterns = {".push_state.json", "temp*.parquet"}
        assert _matches_any("temp.parquet", patterns) is True
        assert _matches_any("data.parquet", patterns) is False

    def test_empty_patterns(self) -> None:
        assert _matches_any("anything", set()) is False

    def test_star_pattern(self) -> None:
        assert _matches_any("anything", {"*"}) is True


class TestCollectAndFilter:
    """Tests for _collect_and_filter — kill collection mutations."""

    def test_collect_wildcard(self, push_dir: Path) -> None:
        push_dir.mkdir(parents=True, exist_ok=True)
        (push_dir / "a.parquet").write_bytes(b"x")
        (push_dir / "sub").mkdir()
        (push_dir / "sub" / "b.parquet").write_bytes(b"y")

        result = _collect_and_filter(push_dir, {"*"}, set())
        paths = [str(p.relative_to(push_dir)) for p in result]
        assert "a.parquet" in paths
        assert "sub/b.parquet" in paths

    def test_collect_extension(self, push_dir: Path) -> None:
        push_dir.mkdir(parents=True, exist_ok=True)
        (push_dir / "a.parquet").write_bytes(b"x")
        (push_dir / "b.txt").write_bytes(b"y")

        result = _collect_and_filter(push_dir, {"*.parquet"}, set())
        names = [p.name for p in result]
        assert "a.parquet" in names
        assert "b.txt" not in names

    def test_exclude(self, push_dir: Path) -> None:
        push_dir.mkdir(parents=True, exist_ok=True)
        (push_dir / "a.parquet").write_bytes(b"x")
        (push_dir / "b.parquet").write_bytes(b"y")

        result = _collect_and_filter(push_dir, {"*.parquet"}, {"b*"})
        names = [p.name for p in result]
        assert "a.parquet" in names
        assert "b.parquet" not in names

    def test_collect_file_only(self, push_dir: Path) -> None:
        push_dir.mkdir(parents=True, exist_ok=True)
        (push_dir / "a.parquet").write_bytes(b"x")
        (push_dir / "dir").mkdir()

        result = _collect_and_filter(push_dir, {"*"}, set())
        dirs = [p.name for p in result if p.is_dir()]
        assert "dir" not in dirs
