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


@pytest.fixture(autouse=True)
def _cache_sha256(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    """Cache compute_sha256 results to avoid redundant file reads.

    Wraps the original function (doesn't replace it) so mutations in
    checksum.py are still tested. Caching speeds up repeated calls with
    the same file content by ~200x.

    Skips only tests in test_checksum.py, test_properties.py, test_fuzz.py
    which specifically test checksum correctness.
    """
    # Skip checksum-specific tests
    test_path = request.node.fspath.strpath
    if "test_checksum.py" in test_path or "test_properties.py" in test_path or "test_fuzz.py" in test_path:
        return

    from push.core import checksum as checksum_module
    from push.core import push as push_module

    original_compute_sha256 = checksum_module.compute_sha256

    def cached_sha256(file_path: Path) -> str:
        """Wrap compute_sha256 with content-based caching."""
        content = file_path.read_bytes()
        content_key = content.hex()
        if content_key not in _FAKE_CHECKSUMS:
            _FAKE_CHECKSUMS[content_key] = original_compute_sha256(file_path)
        return _FAKE_CHECKSUMS[content_key]

    # Patch only in push module where it's imported — mutations in
    # checksum.py still execute the real function (wrapped by cache)
    monkeypatch.setattr(push_module, "compute_sha256", cached_sha256)
