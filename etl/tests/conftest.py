"""Shared fixtures for etl tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etl.orchestrator import ETLConfig, Orchestrator
from push.core.push_manifest import S3Object
from push.core.state import PushedEntry, PushResult


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory for ETL tests."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def fake_pushed_entries() -> list[PushedEntry]:
    """Return sample PushedEntry records for testing."""
    return [
        PushedEntry(
            rel_path="yellow/yellow_tripdata_2024-01.parquet",
            s3_key="data/yellow/yellow_tripdata_2024-01.parquet",
            checksum="abc123",
        ),
        PushedEntry(
            rel_path="green/green_tripdata_2024-01.parquet",
            s3_key="data/green/green_tripdata_2024-01.parquet",
            checksum="def456",
        ),
    ]


@pytest.fixture
def fake_s3_list() -> FakeS3List:
    """Return a fake S3 list client for testing."""
    return FakeS3List()


@pytest.fixture
def fake_extract() -> FakeExtract:
    """Return a fake extract client for testing."""
    return FakeExtract()


@pytest.fixture
def fake_push() -> FakePush:
    """Return a fake push client for testing."""
    return FakePush()


@pytest.fixture
def test_orchestrator(tmp_data_dir: Path) -> Orchestrator:
    """Return an Orchestrator with default config for testing."""
    config = ETLConfig(data_dir=str(tmp_data_dir))
    return Orchestrator(config)


class FakeS3List:
    """Fake S3 list client for testing — no real S3 calls."""

    def __init__(self, objects: list[S3Object] | None = None) -> None:
        """Initialize with pre-defined S3 objects.

        Args:
            objects: List of S3Object to return on list().
        """
        self.objects = objects or []
        self.call_count = 0
        self.last_bucket = ""
        self.last_prefix = ""

    def list(self, bucket: str, prefix: str) -> list[S3Object]:
        """Return pre-defined S3 objects.

        Args:
            bucket: S3 bucket name (ignored, recorded for inspection).
            prefix: Key prefix (ignored, recorded for inspection).

        Returns:
            List of S3Object instances.
        """
        self.call_count += 1
        self.last_bucket = bucket
        self.last_prefix = prefix
        return list(self.objects)


class FakeExtract:
    """Fake extract client that simulates downloading without real I/O."""

    def __init__(
        self,
        downloaded: int = 0,
        skipped: int = 0,
        should_raise: bool = False,
    ) -> None:
        """Initialize the fake extract client.

        Args:
            downloaded: Number of files reported as downloaded.
            skipped: Number of files reported as skipped.
            should_raise: If True, raise RuntimeError on extract.
        """
        self.downloaded = downloaded
        self.skipped = skipped
        self.should_raise = should_raise
        self.call_count = 0
        self.last_data_dir: str | None = None
        self.last_mode: str = ""
        self.last_types: list[str] | None = None
        self.last_push_manifest: dict[str, Any] | None = None

    def run(
        self,
        data_dir: str,
        types: list[str] | None = None,
        from_year: int = 2024,
        to_year: int = 2024,
        mode: str = "incremental",
        push_manifest: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Simulate an extract operation.

        Args:
            data_dir: Data directory path.
            types: List of data types to extract.
            from_year: Starting year.
            to_year: Ending year.
            mode: Extract mode (incremental or full).
            push_manifest: Current push manifest dict.

        Returns:
            Dict with downloaded and skipped counts.

        Raises:
            RuntimeError: If should_raise is True.
        """
        self.call_count += 1
        self.last_data_dir = data_dir
        self.last_mode = mode
        self.last_types = types
        self.last_push_manifest = push_manifest

        if self.should_raise:
            raise RuntimeError("fake extract failure")

        return {"downloaded": self.downloaded, "skipped": self.skipped}


class FakePush:
    """Fake push client that produces PushResult without real I/O."""

    def __init__(
        self,
        uploaded_entries: list[PushedEntry] | None = None,
        uploaded_count: int = 0,
        skipped_count: int = 0,
        failed_count: int = 0,
        should_raise: bool = False,
    ) -> None:
        """Initialize the fake push client.

        Args:
            uploaded_entries: PushedEntry records to return.
            uploaded_count: Number of files reported as uploaded.
            skipped_count: Number of files reported as skipped.
            failed_count: Number of files reported as failed.
            should_raise: If True, raise RuntimeError on upload.
        """
        self.uploaded_entries = uploaded_entries or []
        self.uploaded_count = uploaded_count
        self.skipped_count = skipped_count
        self.failed_count = failed_count
        self.should_raise = should_raise
        self.call_count = 0
        self.last_data_dir: str | None = None
        self.last_bucket: str = ""
        self.last_prefix: str = ""
        self.last_overwrite: bool | None = None
        self.last_delete_after_push: bool | None = None

    def upload(
        self,
        data_dir: str,
        bucket: str = "",
        prefix: str = "",
        overwrite: bool = False,
        delete_after_push: bool = False,
    ) -> PushResult:
        """Simulate a push operation.

        Args:
            data_dir: Data directory path.
            bucket: S3 bucket name.
            prefix: S3 key prefix.
            overwrite: Whether to overwrite existing files.
            delete_after_push: Whether to delete after upload.

        Returns:
            PushResult with configured values.

        Raises:
            RuntimeError: If should_raise is True.
        """
        self.call_count += 1
        self.last_data_dir = data_dir
        self.last_bucket = bucket
        self.last_prefix = prefix
        self.last_overwrite = overwrite
        self.last_delete_after_push = delete_after_push

        if self.should_raise:
            raise RuntimeError("fake push failure")

        return PushResult(
            uploaded=self.uploaded_count,
            skipped=self.skipped_count,
            failed=self.failed_count,
            total=self.uploaded_count + self.skipped_count + self.failed_count,
            uploaded_files=[e.rel_path for e in self.uploaded_entries],
            uploaded_entries=list(self.uploaded_entries),
        )
