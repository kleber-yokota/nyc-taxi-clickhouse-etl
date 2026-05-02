"""Unit tests for the known_missing module."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.core.known_missing import KnownMissing


class TestKnownMissing:
    def test_initially_empty(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        assert len(known._urls) == 0

    def test_add_records_url(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        url = "https://example.com/file.parquet"
        known.add(url)
        assert known.is_missing(url)

    def test_is_missing_returns_false_for_unknown(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        assert not known.is_missing("https://example.com/unknown.parquet")

    def test_add_multiple_urls(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        known.add("https://example.com/a.parquet")
        known.add("https://example.com/b.parquet")
        known.add("https://example.com/c.parquet")
        assert known.is_missing("https://example.com/a.parquet")
        assert known.is_missing("https://example.com/b.parquet")
        assert known.is_missing("https://example.com/c.parquet")

    def test_add_duplicate_no_error(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        url = "https://example.com/file.parquet"
        known.add(url)
        known.add(url)
        assert known.is_missing(url)
        assert len(known._urls) == 1

    def test_loads_existing_file(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        missing_file.write_text(
            "https://example.com/a.parquet\n"
            "https://example.com/b.parquet\n"
        )
        known = KnownMissing(missing_file)
        assert known.is_missing("https://example.com/a.parquet")
        assert known.is_missing("https://example.com/b.parquet")

    def test_loads_empty_file(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        missing_file.write_text("")
        known = KnownMissing(missing_file)
        assert len(known._urls) == 0

    def test_loads_corrupt_file(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        missing_file.write_text("???")
        known = KnownMissing(missing_file)
        assert len(known._urls) == 0

    def test_persists_to_file(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/file.parquet")
        content = missing_file.read_text()
        assert "https://example.com/file.parquet" in content

    def test_clear_removes_all(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "missing.txt")
        known.add("https://example.com/a.parquet")
        known.add("https://example.com/b.parquet")
        known.clear()
        assert not known.is_missing("https://example.com/a.parquet")
        assert not known.is_missing("https://example.com/b.parquet")
        assert len(known._urls) == 0

    def test_persists_after_clear(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/a.parquet")
        known.clear()
        content = missing_file.read_text().strip()
        assert content == ""

    def test_loads_after_clear(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/a.parquet")
        known.clear()
        known2 = KnownMissing(missing_file)
        assert not known2.is_missing("https://example.com/a.parquet")

    def test_creates_parent_directory(self, tmp_path: Path):
        nested = tmp_path / "deep" / "path" / "missing.txt"
        known = KnownMissing(nested)
        known.add("https://example.com/file.parquet")
        assert nested.exists()

    def test_urls_sorted_in_file(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/z.parquet")
        known.add("https://example.com/a.parquet")
        known.add("https://example.com/m.parquet")
        lines = missing_file.read_text().strip().split("\n")
        assert lines == sorted(lines)
