"""Shared fixtures for extract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses
import vcr


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for state files."""
    return tmp_path / "data"


@pytest.fixture
def fake_parquet_content() -> bytes:
    """Return fake parquet content for testing."""
    return b"FAKE_PARQUET_CONTENT_" + b"x" * 100


@pytest.fixture
def state_file(state_dir: Path) -> Path:
    """Return path to state file."""
    return state_dir / ".download_state.json"


@pytest.fixture
def existing_state(state_file: Path) -> Path:
    """Create a state file with pre-existing checksums."""
    state_data = {
        "checksums": {
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet": "abc123",
            "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet": "def456",
        }
    }
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state_data))
    return state_file


@pytest.fixture
def errors_dir(state_dir: Path) -> Path:
    """Return path to errors directory."""
    errors = state_dir / "errors"
    errors.mkdir(parents=True, exist_ok=True)
    return errors


@pytest.fixture
def errors_log(state_dir: Path) -> Path:
    """Return path to errors log file."""
    log = state_dir / "errors" / "download_errors.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    return log


@pytest.fixture
def download_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory for e2e tests."""
    return tmp_path / "data"


# --- E2E VCR ---

E2E_CASSETTE_DIR = "extract/tests/downloaders/cassettes"

e2e_vcr = vcr.VCR(
    cassette_library_dir=E2E_CASSETTE_DIR,
    record_mode="new_episodes",
    match_on=["method", "uri"],
    filter_headers=["authorization"],
    before_record_response=lambda resp: {
        **resp,
        "body": {
            "string": resp["body"]["string"][:512] if resp.get("body", {}).get("string") else b""
        },
    },
)


# --- E2E RESPONSES FIXTURES ---

@pytest.fixture
def mock_200():
    """Mock a single URL returning 200 OK."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(
            responses.GET,
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
            body=b"fake_parquet",
            status=200,
            content_type="application/octet-stream",
        )
        yield rsps


@pytest.fixture
def mock_404():
    """Mock all yellow 2024 URLs returning 404."""
    from extract.core.catalog import Catalog

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
        for entry in catalog.generate():
            rsps.add(responses.GET, entry.url, body="", status=404)
        yield rsps


@pytest.fixture
def mock_500():
    """Mock all yellow 2024 URLs returning 500."""
    from extract.core.catalog import Catalog

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
        for entry in catalog.generate():
            rsps.add(responses.GET, entry.url, body="", status=500)
        yield rsps
