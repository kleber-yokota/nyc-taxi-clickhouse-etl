"""Shared fixtures for push tests."""

from __future__ import annotations

import boto3
from botocore.config import Config
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import testcontainers.minio as mc

from push.core.client import S3Client
from push.core.state import PushState


# Deterministic checksum for fake parquet content (cached to avoid recomputation)
_FAKE_CHECKSUMS: dict[str, str] = {}


@pytest.fixture(scope="module")
def minio_container():
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
def real_client(minio_container):
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
def s3_client(minio_container, real_client):
    """Create an S3Client connected to the MinIO container."""
    with patch("push.core.client.get_s3_client", return_value=real_client):
        yield S3Client.from_env(
            bucket="e2e-bucket",
            prefix="data",
            endpoint_url=minio_container["endpoint_url"],
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
def sample_files(push_dir: Path, fake_parquet_content: bytes):
    """Create sample parquet files in the push directory."""
    (push_dir / "yellow").mkdir(parents=True, exist_ok=True)
    (push_dir / "green").mkdir(parents=True, exist_ok=True)

    (push_dir / "yellow" / "yellow_tripdata_2024-01.parquet").write_bytes(fake_parquet_content)
    (push_dir / "yellow" / "yellow_tripdata_2024-02.parquet").write_bytes(fake_parquet_content * 2)
    (push_dir / "green" / "green_tripdata_2024-01.parquet").write_bytes(fake_parquet_content)

    return push_dir


@pytest.fixture
def sample_files_with_state(push_dir: Path, fake_parquet_content: bytes, push_state_file: Path):
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
    """Speed up push tests by caching checksums and mocking boto3.

    - Caches compute_sha256 results (reads file once, hashes in-memory)
    - Mocks get_s3_client to avoid 2s boto3 session startup per test

    Only patches push_module.compute_sha256 (where upload() imports it).
    test_checksum.py imports compute_sha256 directly from checksum_module,
    so it is unaffected and still tests the real function.
    """
    from push.core import push as push_module
    from push.core import client as client_module

    # Cache compute_sha256
    def cached_sha256(file_path: Path) -> str:
        """Compute SHA-256 from in-memory content, cache for repeated calls."""
        content = file_path.read_bytes()
        content_key = content.hex()
        if content_key not in _FAKE_CHECKSUMS:
            _FAKE_CHECKSUMS[content_key] = sha256(content).hexdigest()
        return _FAKE_CHECKSUMS[content_key]

    push_module.compute_sha256 = cached_sha256

    # Mock boto3 session — avoids 2s startup per test
    # test_core_env.py tests config resolution, not actual S3 connection
    def mock_get_s3_client(endpoint_url: str | None = None):
        """Return a mock S3 client that implements S3Ops."""
        from unittest.mock import MagicMock

        mock = MagicMock()
        mock.put_object.return_value = {"ETag": '"abc123"'}
        mock.head_object.return_value = {"ContentLength": 100, "ETag": '"abc123"'}
        mock.list_objects.return_value = {"Contents": []}
        mock.delete_object.return_value = None
        mock.create_bucket.return_value = None
        mock.upload_fileobj.return_value = None
        return mock

    client_module.get_s3_client = mock_get_s3_client


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """No-op: Python process exits after session, no cleanup needed."""
    pass
