"""E2E tests for error handling using VCR cassettes."""

from __future__ import annotations

import json
from pathlib import Path

import vcr

from extract.downloader.downloader import run

CASSETTE_DIR = "extract/tests/downloaders/cassettes"

e2e_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    match_on=["method", "uri"],
    filter_headers=["authorization"],
)


class Test404Handled:
    """Uses cassette with 12 x 404 responses."""
    @e2e_vcr.use_cassette("all_404.yaml")
    def test_404_handled_gracefully(self, download_dir: Path):
        """Test that 404 responses are logged and recorded as known missing."""
        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
        )

        assert result["failed"] == 12
        assert result["downloaded"] == 0

        errors_log = download_dir / "errors" / "download_errors.log"
        assert errors_log.exists()
        lines = errors_log.read_text().strip().split("\n")
        assert len(lines) == 12

        missing_file = download_dir / "known_missing.txt"
        assert missing_file.exists()
        lines = missing_file.read_text().strip().split("\n")
        assert len(lines) == 12


class TestChecksumMismatch:
    """Uses cassette serving different content than existing file."""
    @e2e_vcr.use_cassette("checksum_mismatch.yaml")
    def test_checksum_mismatch_backs_up_old_file(self, download_dir: Path):
        """Test that files with checksum mismatch get backed up."""
        target_dir = download_dir / "yellow"
        target_dir.mkdir(parents=True, exist_ok=True)
        old_content = b"old content"
        (target_dir / "yellow_tripdata_2024-01.parquet").write_bytes(old_content)

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=1,
        )

        backup_path = target_dir / "yellow_tripdata_2024-01.parquet.old"
        assert backup_path.exists()
        assert backup_path.read_bytes() == old_content


class TestHTTP500:
    """Uses cassette with 12 x 500 responses."""
    @e2e_vcr.use_cassette("all_500.yaml")
    def test_http_500_handled(self, download_dir: Path):
        """Test that 500 errors are logged correctly."""
        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
        )

        assert result["failed"] == 12

        errors_log = download_dir / "errors" / "download_errors.log"
        assert errors_log.exists()
        lines = errors_log.read_text().strip().split("\n")
        entry = json.loads(lines[0])
        assert entry["type"] == "http_error"
