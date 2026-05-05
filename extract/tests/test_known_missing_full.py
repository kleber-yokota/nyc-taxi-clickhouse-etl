"""Tests for known_missing module."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.core.known_missing import KnownMissing


class TestKnownMissingPersist:
    """Tests for KnownMissing._persist method."""

    def test_persist_creates_file(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/missing.parquet")

        assert (tmp_path / "known_missing.txt").exists()

    def test_persist_sorted_urls(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/zzz.parquet")
        known.add("https://example.com/aaa.parquet")
        known.add("https://example.com/mmm.parquet")

        content = (tmp_path / "known_missing.txt").read_text()
        lines = [l for l in content.strip().split("\n") if l]
        assert lines == sorted(lines)

    def test_persist_overwrites_existing(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/first.parquet")
        known.add("https://example.com/second.parquet")
        known.add("https://example.com/third.parquet")

        known.clear()
        known.add("https://example.com/new.parquet")

        content = (tmp_path / "known_missing.txt").read_text()
        assert "first" not in content
        assert "new.parquet" in content

    def test_persist_creates_parent_dir(self, tmp_path: Path):
        nested_path = tmp_path / "nested" / "dir" / "known_missing.txt"
        known = KnownMissing(nested_path)
        known.add("https://example.com/file.parquet")

        assert nested_path.exists()

    def test_persist_empty_set(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.clear()

        content = (tmp_path / "known_missing.txt").read_text()
        assert content == "\n"

    def test_persist_multiple_urls_sorted(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        urls = [
            "https://example.com/z.parquet",
            "https://example.com/a.parquet",
            "https://example.com/m.parquet",
        ]
        for url in urls:
            known.add(url)

        content = (tmp_path / "known_missing.txt").read_text()
        lines = [l for l in content.strip().split("\n") if l]
        assert lines == sorted(urls)


class TestKnownMissingLoad:
    """Tests for KnownMissing._load method."""

    def test_load_existing_file(self, tmp_path: Path):
        known_file = tmp_path / "known_missing.txt"
        known_file.write_text("https://example.com/file1.parquet\nhttps://example.com/file2.parquet\n")

        known = KnownMissing(known_file)
        assert known.is_missing("https://example.com/file1.parquet")
        assert known.is_missing("https://example.com/file2.parquet")

    def test_load_empty_file(self, tmp_path: Path):
        known_file = tmp_path / "known_missing.txt"
        known_file.write_text("")

        known = KnownMissing(known_file)
        assert not known.is_missing("https://example.com/anything.parquet")

    def test_load_nonexistent_file(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "nonexistent.txt")
        assert not known.is_missing("https://example.com/anything.parquet")

    def test_load_filters_non_http_lines(self, tmp_path: Path):
        known_file = tmp_path / "known_missing.txt"
        known_file.write_text("https://example.com/valid.parquet\nnot a url\nftp://invalid.parquet\n")

        known = KnownMissing(known_file)
        assert known.is_missing("https://example.com/valid.parquet")
        assert not known.is_missing("ftp://invalid.parquet")

    def test_load_os_error(self, tmp_path: Path):
        known_file = tmp_path / "known_missing.txt"
        known_file.write_text("https://example.com/file.parquet\n")

        known = KnownMissing(known_file)
        assert known.is_missing("https://example.com/file.parquet")


class TestKnownMissingAdd:
    """Tests for KnownMissing.add method."""

    def test_add_single_url(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/file.parquet")

        assert known.is_missing("https://example.com/file.parquet")

    def test_add_duplicate_url(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/file.parquet")
        known.add("https://example.com/file.parquet")

        assert known.is_missing("https://example.com/file.parquet")
        content = (tmp_path / "known_missing.txt").read_text()
        assert content.count("file.parquet") == 1

    def test_add_persists_immediately(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/file.parquet")

        content = (tmp_path / "known_missing.txt").read_text()
        assert "file.parquet" in content


class TestKnownMissingClear:
    """Tests for KnownMissing.clear method."""

    def test_clear_removes_all_urls(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/file1.parquet")
        known.add("https://example.com/file2.parquet")
        known.clear()

        assert not known.is_missing("https://example.com/file1.parquet")
        assert not known.is_missing("https://example.com/file2.parquet")

    def test_clear_persists_empty(self, tmp_path: Path):
        known = KnownMissing(tmp_path / "known_missing.txt")
        known.add("https://example.com/file.parquet")
        known.clear()

        content = (tmp_path / "known_missing.txt").read_text()
        assert content == "\n"
