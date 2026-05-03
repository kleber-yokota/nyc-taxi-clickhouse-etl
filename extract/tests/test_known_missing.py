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

    def test_init_with_default_path(self, tmp_path: Path):
        import os
        os.chdir(str(tmp_path))
        known = KnownMissing()
        assert known._known_missing_path.name == "known_missing.txt"

    def test_init_with_absolute_path(self, tmp_path: Path):
        import os
        known = KnownMissing(str(tmp_path / "abs_missing.txt"))
        assert os.path.isabs(str(known._known_missing_path))

    def test_load_ignores_non_http_lines(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        missing_file.write_text(
            "https://example.com/a.parquet\n"
            "not a url\n"
            "https://example.com/b.parquet\n"
            "\n"
        )
        known = KnownMissing(missing_file)
        assert known.is_missing("https://example.com/a.parquet")
        assert known.is_missing("https://example.com/b.parquet")
        assert not known.is_missing("not a url")

    def test_persist_with_single_url(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/single.parquet")
        content = missing_file.read_text()
        assert "https://example.com/single.parquet\n" == content

    def test_loads_existing_with_trailing_newlines(self, tmp_path: Path):
        missing_file = tmp_path / "missing.txt"
        missing_file.write_text(
            "https://example.com/a.parquet\n"
            "https://example.com/b.parquet\n"
            "\n"
        )
        known = KnownMissing(missing_file)
        assert known.is_missing("https://example.com/a.parquet")
        assert known.is_missing("https://example.com/b.parquet")

    def test_persist_creates_nested_parent_dirs(self, tmp_path: Path):
        missing_file = tmp_path / "a" / "b" / "c" / "deep_missing.txt"
        known = KnownMissing(missing_file)
        known.add("https://example.com/deep.parquet")
        assert missing_file.exists()
        content = missing_file.read_text()
        assert "https://example.com/deep.parquet" in content

    def test_persist_overwrites_existing_content(self, tmp_path: Path):
        missing_file = tmp_path / "overwrite.txt"
        missing_file.write_text("https://old.com/old.parquet\n")
        known = KnownMissing(missing_file)
        known.add("https://new.com/new.parquet")
        known.clear()
        content = missing_file.read_text().strip()
        assert content == ""
        assert "https://old.com" not in content
        assert "https://new.com" not in content
