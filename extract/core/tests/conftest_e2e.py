"""Shared fixtures for e2e tests using responses to emulate TLC CDN."""

from __future__ import annotations

from pathlib import Path

import pytest
import responses

from extract.core.catalog import Catalog


@pytest.fixture
def fake_parquet_content() -> bytes:
    """Generate fake parquet content (~1KB, safe for hypothesis)."""
    return b"FAKE_PARQUET\x00" + b"x" * 1024


@responses.activate
@pytest.fixture
def mock_tlc_cdn(fake_parquet_content: bytes):
    """Mock all TLC CDN URLs with fake parquet content.

    Register responses for all types and years from 2024.
    Each URL returns the fake parquet content.
    """
    catalog = Catalog(types=["yellow", "green", "fhv", "fhvhv"], from_year=2024, to_year=2024)
    entries = catalog.generate()

    for entry in entries:
        body = fake_parquet_content + entry.url.encode()
        responses.add(
            responses.GET,
            entry.url,
            body=body,
            status=200,
            content_type="application/octet-stream",
        )

    yield responses
