"""Unit tests for file filtering — target filter.py mutations."""

from __future__ import annotations

from upload.core.filter import (
    _collect_and_filter,
    _matches_any,
    _matches_pattern,
)


class TestMatchesPattern:
    """Tests for _matches_pattern — kill pattern matching mutations."""

    def test_exact_match(self):
        assert _matches_pattern("file.parquet", "file.parquet", "file.parquet") is True

    def test_wildcard_match(self):
        assert _matches_pattern("file.parquet", "file.parquet", "*.parquet") is True

    def test_no_match(self):
        assert _matches_pattern("file.txt", "file.txt", "*.parquet") is False

    def test_star_pattern(self):
        assert _matches_pattern("anything", "anything", "*") is True

    def test_path_match(self):
        assert _matches_pattern("yellow.parquet", "yellow/file.parquet", "yellow*") is True

    def test_no_match_path(self):
        assert _matches_pattern("green.parquet", "green/file.parquet", "yellow*") is False

    def test_or_not_and_mutation(self):
        """Kills or→and mutation: filename matches but path doesn't."""
        assert _matches_pattern("file.parquet", "subdir/file.parquet", "file.parquet") is True


class TestMatchesAny:
    """Tests for _matches_any — kill exclusion pattern mutations."""

    def test_single_exclude(self):
        assert _matches_any("file.parquet", {".upload_state.json"}) is False
        assert _matches_any(".upload_state.json", {".upload_state.json"}) is True

    def test_multiple_exclude(self):
        patterns = {".upload_state.json", "temp*.parquet"}
        assert _matches_any("temp.parquet", patterns) is True
        assert _matches_any("data.parquet", patterns) is False

    def test_empty_patterns(self):
        assert _matches_any("anything", set()) is False

    def test_star_pattern(self):
        assert _matches_any("anything", {"*"}) is True


class TestCollectAndFilter:
    """Tests for _collect_and_filter — kill collection mutations."""

    def test_collect_wildcard(self, upload_dir):
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / "a.parquet").write_bytes(b"x")
        (upload_dir / "sub").mkdir()
        (upload_dir / "sub" / "b.parquet").write_bytes(b"y")

        result = _collect_and_filter(upload_dir, {"*"}, set())
        paths = [str(p.relative_to(upload_dir)) for p in result]
        assert "a.parquet" in paths
        assert "sub/b.parquet" in paths

    def test_collect_extension(self, upload_dir):
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / "a.parquet").write_bytes(b"x")
        (upload_dir / "b.txt").write_bytes(b"y")

        result = _collect_and_filter(upload_dir, {"*.parquet"}, set())
        names = [p.name for p in result]
        assert "a.parquet" in names
        assert "b.txt" not in names

    def test_exclude(self, upload_dir):
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / "a.parquet").write_bytes(b"x")
        (upload_dir / "b.parquet").write_bytes(b"y")

        result = _collect_and_filter(upload_dir, {"*.parquet"}, {"b*"})
        names = [p.name for p in result]
        assert "a.parquet" in names
        assert "b.parquet" not in names

    def test_collect_file_only(self, upload_dir):
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / "a.parquet").write_bytes(b"x")
        (upload_dir / "dir").mkdir()

        result = _collect_and_filter(upload_dir, {"*"}, set())
        dirs = [p.name for p in result if p.is_dir()]
        assert "dir" not in dirs
