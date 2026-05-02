"""E2E test for known_missing skip behavior on retry."""

from __future__ import annotations

from pathlib import Path

import responses

from extract.core.downloader import run


def test_retry_skips_known_missing(download_dir: Path):
    """Test that on second run, 404 URLs from known_missing.txt are skipped (not failed)."""
    from extract.core.catalog import Catalog

    catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)

    # First run: all 404 → all recorded as known missing
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        for entry in catalog.generate():
            rsps.add(responses.GET, entry.url, body="", status=404)

        result1 = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
        )

    assert result1["failed"] == 12

    # Second run: same 404s, but now they're in known_missing.txt → skipped
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        for entry in catalog.generate():
            rsps.add(responses.GET, entry.url, body="", status=404)

        result2 = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="incremental",
        )

    assert result2["failed"] == 0
    assert result2["skipped"] == 12
