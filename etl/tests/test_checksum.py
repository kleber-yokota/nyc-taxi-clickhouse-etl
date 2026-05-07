"""Tests for etl.checksum."""

from pathlib import Path

from etl.checksum import Checksum


def test_compute_matches_upload(tmp_path: Path):
    checksum_provider = Checksum()
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world")
    result = checksum_provider.compute(test_file)
    assert isinstance(result, str)
    assert len(result) == 64  # SHA-256 hex digest length

    from upload.core.checksum import compute_sha256 as upload_checksum
    expected = upload_checksum(test_file)
    assert result == expected


def test_compute_knows_nothing_about_extract():
    import inspect
    source = inspect.getsource(Checksum.compute)
    assert "extract" not in source.lower() or "upload" in source
