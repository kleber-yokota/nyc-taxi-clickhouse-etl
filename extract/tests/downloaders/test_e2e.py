"""E2E tests for download flow using VCR cassettes."""

from __future__ import annotations

import json
from pathlib import Path

import vcr

from extract.downloader.downloader import run
from extract.core.state_manager import State

CASSETTE_DIR = "extract/tests/downloaders/cassettes"

e2e_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    match_on=["method", "uri"],
    filter_headers=["authorization"],
)


class TestDownloadNewFiles:
    """Uses cassette with 3 x 200 OK responses (max_entries=3)."""
    @e2e_vcr.use_cassette("success_200.yaml")
    def test_download_new_files(self, download_dir: Path):
        """Test that new files are downloaded and state is persisted."""
        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        assert result["downloaded"] == 3
        assert result["failed"] == 0

        state = State(download_dir / ".download_state.json")
        assert len(state.checksums) == 3


class TestPartialDownload:
    """Uses cassette with mixed 200/404 responses (max_entries=3)."""
    @e2e_vcr.use_cassette("partial_200_404.yaml")
    def test_partial_download(self, download_dir: Path):
        """Test that download stops gracefully after failures."""
        result = run(
            data_dir=download_dir,
            types=["yellow", "green"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        assert result["downloaded"] == 3
        assert result["failed"] == 0


class TestStatePersisted:
    @e2e_vcr.use_cassette("success_200.yaml")
    def test_state_is_persisted(self, download_dir: Path):
        """Test that state file is persisted after download."""
        run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        state_file = download_dir / ".download_state.json"
        assert state_file.exists()

        data = json.loads(state_file.read_text())
        assert "checksums" in data
        assert len(data["checksums"]) == 3


class TestMultipleRunsSkipDownloaded:
    @e2e_vcr.use_cassette("success_200.yaml")
    def test_multiple_runs_skip_downloaded(self, download_dir: Path):
        """Test that running download twice skips already downloaded files."""
        result1 = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        assert result1["downloaded"] == 3

        # Second run: all files are skipped (state says already downloaded)
        result2 = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="incremental",
            max_entries=3,
        )
        assert result2["skipped"] == 3
        assert result2["failed"] == 0
        assert result2["downloaded"] == 0


class TestTmpCleaned:
    @e2e_vcr.use_cassette("success_200.yaml")
    def test_tmp_file_removed_on_success(self, download_dir: Path):
        """Test that .download.tmp files are removed after successful download."""
        run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        for root, dirs, files in download_dir.walk():
            for f in files:
                assert not str(f).endswith(".download.tmp")
