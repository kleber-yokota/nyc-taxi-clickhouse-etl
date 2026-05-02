"""E2E test for known_missing skip behavior on retry using VCR."""

from __future__ import annotations

from pathlib import Path

import vcr

from extract.core.downloader import run

# VCR cassette directory — 2 small cassettes (3 requests each)
CASSETTE_DIR = "extract/core/tests/cassettes"

e2e_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    match_on=["method", "uri"],
    filter_headers=["authorization"],
)


@e2e_vcr.use_cassette("success_200.yaml")
def test_retry_skips_known_missing(download_dir: Path):
    """Test that on second run, already downloaded files are skipped.

    VCR cassette has 3 x 200 OK (max_entries=3).
    First run downloads all 3. Second run skips all 3.
    """
    # First run: all 3 download successfully
    result1 = run(
        data_dir=download_dir,
        types=["yellow"],
        from_year=2024,
        to_year=2024,
        mode="full",
        max_entries=3,
    )

    assert result1["downloaded"] == 3

    # Second run: all 3 are skipped (state says already downloaded)
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
