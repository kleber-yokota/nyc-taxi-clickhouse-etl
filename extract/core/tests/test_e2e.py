"""E2E tests for the downloader module using httpx mocking."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.core.catalog import Catalog, CatalogEntry
from extract.core.downloader import run
from extract.core.state import compute_sha256
from extract.core.state_manager import State


@pytest.fixture
def fake_parquet_content() -> bytes:
    """Generate fake parquet content for a given URL."""
    return b"FAKE_PARQUET\x00" + b"x" * 1024


@pytest.fixture
def mock_httpx_response(fake_parquet_content: bytes) -> MagicMock:
    """Create a mock httpx Response."""
    response = MagicMock()
    response.status_code = 200
    response.content = fake_parquet_content
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_httpx_client(mock_httpx_response: MagicMock):
    """Mock httpx.Client context manager."""
    with patch("extract.core.downloader.httpx.Client") as mock_client:
        mock_client.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get = MagicMock(return_value=mock_httpx_response)
        yield mock_client


@pytest.fixture
def download_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory."""
    return tmp_path / "data"


def test_e2e_download_new_files(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that new files are downloaded and state is persisted."""
    result = run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

    assert result["downloaded"] == 12
    assert result["failed"] == 0

    state = State(download_dir / ".download_state.json")
    assert len(state.checksums) == 12


def test_e2e_404_handled_gracefully(download_dir: Path):
    """Test that 404 responses are logged as errors and don't crash the run."""
    from httpx import HTTPStatusError

    response = MagicMock()
    response.status_code = 404

    def raise_404(*args, **kwargs):
        raise HTTPStatusError("Not Found", request=MagicMock(), response=response)

    with patch("extract.core.downloader.httpx.Client") as mock_client:
        mock_client.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get = MagicMock(side_effect=raise_404)

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


def test_e2e_network_error_handled(download_dir: Path):
    """Test that network errors are logged and don't crash the run."""
    from httpx import RequestError

    with patch("extract.core.downloader.httpx.Client") as mock_client:
        mock_client.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get = MagicMock(side_effect=RequestError("test", request=MagicMock()))

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


def test_e2e_partial_download(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that download stops gracefully after failures and continues with next entries."""
    call_count = 0

    def side_effect(url):
        nonlocal call_count
        call_count += 1
        if call_count <= 6:
            response = MagicMock()
            response.status_code = 200
            response.content = b"fake" + url.encode()
            response.raise_for_status = MagicMock()
            return response
        else:
            from httpx import HTTPStatusError
            response = MagicMock()
            response.status_code = 404
            raise HTTPStatusError("Not Found", request=MagicMock(), response=response)

    mock_httpx_client.get.side_effect = side_effect

    result = run(
        data_dir=download_dir,
        types=["yellow", "green"],
        from_year=2024,
        to_year=2024,
        mode="full",
    )

    assert result["downloaded"] == 6
    assert result["failed"] == 18


def test_e2e_checksum_mismatch_backs_up_old_file(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that files with checksum mismatch get backed up before download."""
    entry = CatalogEntry("yellow", 2024, 1)

    # Create old file with known content
    target_dir = download_dir / entry.target_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    old_content = b"old content"
    (target_dir / entry.filename).write_bytes(old_content)

    state = State(download_dir / ".download_state.json")
    old_checksum = compute_sha256(target_dir / entry.filename)
    state.save(entry.url, old_checksum)

    # Make httpx return different content
    mock_httpx_client.get.return_value.content = b"new content"

    result = run(
        data_dir=download_dir,
        types=["yellow"],
        from_year=2024,
        to_year=2024,
        mode="full",
    )

    backup_path = target_dir / f"{entry.filename}.old"
    assert backup_path.exists()
    assert backup_path.read_bytes() == old_content


def test_e2e_state_is_persisted(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that state file is persisted after download."""
    result = run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

    state_file = download_dir / ".download_state.json"
    assert state_file.exists()

    data = json.loads(state_file.read_text())
    assert "checksums" in data
    assert len(data["checksums"]) == 12


def test_e2e_multiple_runs_skip_downloaded(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that running download twice skips already downloaded files."""
    result1 = run(
        data_dir=download_dir,
        types=["yellow"],
        from_year=2024,
        to_year=2024,
        mode="full",
    )
    assert result1["downloaded"] > 0

    # Second run should skip all files
    result2 = run(
        data_dir=download_dir,
        types=["yellow"],
        from_year=2024,
        to_year=2024,
        mode="incremental",
    )
    assert result2["skipped"] == 12
    assert result2["downloaded"] == 0


def test_e2e_http_500_handled(download_dir: Path):
    """Test that 500 errors are logged correctly."""
    from httpx import HTTPStatusError

    response = MagicMock()
    response.status_code = 500

    def raise_500(*args, **kwargs):
        raise HTTPStatusError("Internal Server Error", request=MagicMock(), response=response)

    with patch("extract.core.downloader.httpx.Client") as mock_client:
        mock_client.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get = MagicMock(side_effect=raise_500)

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


def test_e2e_tmp_file_removed_on_success(download_dir: Path, mock_httpx_client: MagicMock):
    """Test that .download.tmp files are removed after successful download."""
    result = run(
        data_dir=download_dir,
        types=["yellow"],
        from_year=2024,
        to_year=2024,
        mode="full",
    )

    # No tmp files should remain
    for root, dirs, files in download_dir.walk():
        for f in files:
            assert not str(f).endswith(".download.tmp")
