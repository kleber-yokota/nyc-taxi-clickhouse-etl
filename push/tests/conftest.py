"""Shared fixtures for push tests."""

from __future__ import annotations

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch, MagicMock

import pytest
import testcontainers.minio as mc  # type: ignore[import-untyped]

from push.core.client import S3Client
from push.core.state import PushState


# Deterministic checksum for fake parquet content (cached to avoid recomputation)
_FAKE_CHECKSUMS: dict[str, str] = {}


@pytest.fixture(scope="module")
def minio_container() -> Iterator[dict[str, str | int]]:
    """Start a MinIO container for integration tests."""
    container = mc.MinioContainer()
    container.start()
    config = container.get_config()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(9000)
    yield {
        "host": host,
        "port": port,
        "access_key": config["access_key"],
        "secret_key": config["secret_key"],
        "endpoint_url": f"http://{host}:{port}",
    }
    container.stop()


@pytest.fixture
def real_client(minio_container: dict[str, str | int]) -> Any:
    """Create a real boto3 client connected to MinIO."""
    endpoint_url = minio_container["endpoint_url"]
    access_key = minio_container["access_key"]
    secret_key = minio_container["secret_key"]
    return boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    ).client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture
def s3_client(minio_container: dict[str, str | int], real_client: object) -> Iterator[S3Client]:
    """Create an S3Client connected to the MinIO container."""
    with patch("push.core.client.get_s3_client", return_value=real_client):
        yield S3Client.from_env(
            bucket="e2e-bucket",
            prefix="data",
            endpoint_url=str(minio_container["endpoint_url"]),
        )


@pytest.fixture
def push_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory for push tests."""
    return tmp_path / "data"


@pytest.fixture
def fake_parquet_content() -> bytes:
    """Return fake parquet content for testing."""
    return b"FAKE_PARQUET_CONTENT_" + b"x" * 100


@pytest.fixture
def push_state_file(push_dir: Path) -> Path:
    """Return path to push state file."""
    return push_dir / ".push_state.json"


@pytest.fixture
def sample_files(push_dir: Path, fake_parquet_content: bytes) -> Path:
    """Create sample parquet files in the push directory."""
    (push_dir / "yellow").mkdir(parents=True, exist_ok=True)
    (push_dir / "green").mkdir(parents=True, exist_ok=True)

    (push_dir / "yellow" / "yellow_tripdata_2024-01.parquet").write_bytes(fake_parquet_content)
    (push_dir / "yellow" / "yellow_tripdata_2024-02.parquet").write_bytes(fake_parquet_content * 2)
    (push_dir / "green" / "green_tripdata_2024-01.parquet").write_bytes(fake_parquet_content)

    return push_dir


@pytest.fixture
def sample_files_with_state(push_dir: Path, fake_parquet_content: bytes, push_state_file: Path) -> Path:
    """Create sample parquet files with pre-populated push state."""
    (push_dir / "yellow").mkdir(parents=True, exist_ok=True)
    (push_dir / "yellow" / "yellow_tripdata_2024-01.parquet").write_bytes(fake_parquet_content)

    checksum = sha256(fake_parquet_content).hexdigest()

    state = PushState(push_state_file)
    state.record_push(
        str(push_dir / "yellow" / "yellow_tripdata_2024-01.parquet"),
        "data/yellow/yellow_tripdata_2024-01.parquet",
        checksum,
    )
    state.save()

    return push_dir


def pytest_sessionstart(session: pytest.Session) -> None:
    """Speed up push tests by caching checksums.

    - Caches compute_sha256 results (reads file once, hashes in-memory)
    - Only patches push_module.compute_sha256 (where upload() imports it).
    - test_checksum.py imports compute_sha256 directly from checksum_module,
      so it is unaffected and still tests the real function.

    boto3.Session mock is applied in pytest_collection_modifyitems after
    e2e tests are identified (session.items is empty at sessionstart).
    """
    from push.core import push as push_module

    # Cache compute_sha256
    def cached_sha256(file_path: Path) -> str:
        """Compute SHA-256 from in-memory content, cache for repeated calls."""
        content = file_path.read_bytes()
        content_key = content.hex()
        if content_key not in _FAKE_CHECKSUMS:
            _FAKE_CHECKSUMS[content_key] = sha256(content).hexdigest()
        return _FAKE_CHECKSUMS[content_key]

    push_module.compute_sha256 = cached_sha256  # type: ignore[attr-defined]


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mock boto3.session.Session after collection to avoid 2s startup per test.

    Skips the mock when e2e tests are present (they need real boto3 to connect
    to MinIO). At this point session.items is populated so e2e detection is
    reliable.

    Mutations in client.py are still tested — get_s3_client is still called,
    only the expensive Session() initialization is skipped.
    """
    from unittest.mock import MagicMock

    # Check if any e2e tests are in this session
    e2e_items = [item for item in items if "test_e2e" in item.nodeid]
    if e2e_items:
        return  # e2e tests need real boto3

    # Mock boto3.session.Session — avoids 2s startup per test
    mock_session = MagicMock()
    mock_session.client.return_value = MagicMock()
    import boto3.session  # type: ignore[import-untyped]

    boto3.session.Session = lambda *a, **k: mock_session


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """No-op: Python process exits after session, no cleanup needed."""
    pass
