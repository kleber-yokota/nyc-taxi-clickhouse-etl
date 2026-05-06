"""ETL orchestrator — coordinates extract and push pipeline.

The orchestrator is the authority on the push manifest:
- It creates, updates, and saves the manifest
- It delegates download to extract
- It delegates upload to push
- Push provides PushedEntry[], orchestrator builds the manifest
- When manifest is missing/outdated (incremental), orchestrator
  calls push.push_manifest.list_s3_objects() to rebuild it

It does NOT:
- Know how to communicate with S3 (delegate to push)
- Know how to download files (delegate to extract)
- Calculate checksums (push does that)
- Build S3 keys (push does that via S3Client.build_key)
- List S3 objects (push does that via S3Client.list_objects)
- Know manifest format (orchestrator adapts push's neutral data)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from dotenv import load_dotenv

from extract.downloader.downloader import run as extract_run
from push.core.push_manifest import list_s3_objects
from push.core.push_manifest import S3Object
from push.core.runner import upload_from_env
from push.core.state import PushResult
from push.core.state import UploadConfig

from .manifest import load, save, update_from_entries


class S3ListClient(Protocol):
    """Interface for S3 listing operations."""

    def list(self, bucket: str, prefix: str) -> list[S3Object]:
        """List S3 objects under a prefix.

        Args:
            bucket: S3 bucket name.
            prefix: Key prefix to filter.

        Returns:
            List of S3Object instances.
        """
        ...


class ExtractClient(Protocol):
    """Interface for extract operations."""

    def run(
        self,
        data_dir: str,
        types: list[str] | None = None,
        from_year: int = 2024,
        to_year: int = 2024,
        mode: str = "incremental",
        push_manifest: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Run extract operation.

        Args:
            data_dir: Data directory path.
            types: List of data types to extract.
            from_year: Starting year.
            to_year: Ending year.
            mode: Extract mode.
            push_manifest: Current push manifest.

        Returns:
            Dict with extracted file info.
        """
        ...


class PushClient(Protocol):
    """Interface for push operations."""

    def upload(
        self,
        data_dir: str,
        bucket: str = "",
        prefix: str = "",
        overwrite: bool = False,
        delete_after_push: bool = False,
    ) -> PushResult:
        """Run push operation.

        Args:
            data_dir: Data directory path.
            bucket: S3 bucket name.
            prefix: S3 key prefix.
            overwrite: Whether to overwrite existing files.
            delete_after_push: Whether to delete after upload.

        Returns:
            PushResult with upload details.
        """
        ...


@dataclass(frozen=True)
class ETLConfig:
    """Configuration for the ETL pipeline.

    Args:
        data_dir: Base directory for data files. Defaults to "data".
        bucket: S3 bucket name. Read from S3_BUCKET env if empty.
        prefix: S3 key prefix. Defaults to "data".
        types: Data types to extract. Defaults to all.
        from_year: Starting year (inclusive). Defaults to 2024.
        to_year: Ending year (inclusive). Defaults to 2024.
        mode: "incremental" or "full". Defaults to "incremental".
        delete_after_push: Delete local files after upload. Defaults to True.
    """
    data_dir: str = "data"
    bucket: str = ""
    prefix: str = "data"
    types: list[str] = field(
        default_factory=lambda: ["yellow", "green", "fhv", "fhvhv"]
    )
    from_year: int = 2024
    to_year: int = 2024
    mode: str = "incremental"
    delete_after_push: bool = True


def adapt_s3_to_manifest(
    s3_objects: list[S3Object],
    prefix: str,
) -> dict[str, Any]:
    """Adapt neutral S3Object[] into manifest dict for extract.

    Push returns S3Object[key] — neutral, no manifest awareness.
    Extract expects manifest as dict with rel_path as keys.

    Example:
        S3Object(key="data/yellow/file.parquet")
        -> manifest["yellow/file.parquet"] = {"s3_key": "data/yellow/file.parquet"}

    Args:
        s3_objects: List of S3Object from push.list_s3_objects().
        prefix: S3 key prefix (e.g. "data").

    Returns:
        Dict mapping relative paths to {s3_key}.
    """
    manifest = {}
    for obj in s3_objects:
        if obj.key.startswith(f"{prefix}/"):
            rel_path = obj.key[len(prefix) + 1:]
        else:
            rel_path = obj.key

        manifest[rel_path] = {"s3_key": obj.key}

    return manifest


class Orchestrator:
    """Coordinates extract -> push pipeline."""

    def __init__(self, config: ETLConfig | None = None) -> None:
        """Initialize the orchestrator.

        Args:
            config: ETL pipeline configuration. Uses defaults if None.
        """
        self.config = config or ETLConfig()

    def run(
        self,
        s3_list_client: S3ListClient | None = None,
        extract_client: ExtractClient | None = None,
        push_client: PushClient | None = None,
    ) -> dict[str, Any]:
        """Run the full ETL pipeline.

        1. Load environment variables
        2. Load existing manifest (or empty dict)
        3. If incremental + manifest missing: rebuild from S3
        4. Run extract (downloads missing files, uses manifest for skip)
        5. Run push (uploads files, returns PushedEntry[])
        6. Build manifest from push's PushedEntry[]
        7. Save manifest to disk

        Args:
            s3_list_client: Optional S3 list client for dependency injection.
            extract_client: Optional extract client for dependency injection.
            push_client: Optional push client for dependency injection.

        Returns:
            Dict with 'extract', 'push', and 'reconciled' result dicts.
        """
        load_dotenv()

        data_dir = self._ensure_data_dir()
        s3_list = s3_list_client or _RealS3ListClient()
        extractor = extract_client or _RealExtractClient()
        pusher = push_client or _RealPushClient()

        manifest = self._load_manifest(data_dir, s3_list)
        extract_result = self._run_extract(extractor, data_dir, manifest)
        push_result = self._run_push(pusher, data_dir)

        self._save_manifest(data_dir, manifest, push_result)
        return self._build_result(extract_result, push_result)

    def _load_manifest(
        self,
        data_dir: Path,
        s3_list: S3ListClient,
    ) -> dict[str, Any]:
        """Load manifest or rebuild from S3 in incremental mode.

        Args:
            data_dir: Data directory path.
            s3_list: S3 list client for dependency injection.

        Returns:
            Manifest dict.
        """
        manifest = load(data_dir)
        if self.config.mode == "incremental" and not manifest:
            manifest = self._rebuild_manifest(s3_list)
        return manifest

    def _save_manifest(
        self,
        data_dir: Path,
        manifest: dict[str, Any],
        push_result: PushResult,
    ) -> None:
        """Update manifest with push results and save to disk.

        Args:
            data_dir: Data directory path.
            manifest: Manifest dict to update.
            push_result: Result from push operation.
        """
        update_from_entries(manifest, push_result.uploaded_entries)
        save(data_dir, manifest)

    def _build_result(
        self,
        extract_result: dict[str, Any],
        push_result: PushResult,
    ) -> dict[str, Any]:
        """Build return dict with extract, push, and reconciled results.

        Args:
            extract_result: Result from extract operation.
            push_result: Result from push operation.

        Returns:
            Dict with all result sections.
        """
        return {
            "extract": extract_result,
            "push": push_result,
            "reconciled": {"rebuilt": 0, "recovered": 0},
        }

    def _ensure_data_dir(self) -> Path:
        """Create data directory if it doesn't exist.

        Returns:
            Path to the data directory.
        """
        data_dir = Path(self.config.data_dir)
        data_dir.mkdir(exist_ok=True)
        return data_dir

    def _run_extract(
        self,
        extractor: ExtractClient,
        data_dir: Path,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        """Run extract with manifest for skip logic.

        Args:
            extractor: Extract client for dependency injection.
            data_dir: Data directory path.
            manifest: Current push manifest dict.

        Returns:
            Dict with extracted file info.
        """
        return extractor.run(
            data_dir=str(data_dir),
            types=self.config.types,
            from_year=self.config.from_year,
            to_year=self.config.to_year,
            mode=self.config.mode,
            push_manifest=manifest,
        )

    def _run_push(
        self,
        pusher: PushClient,
        data_dir: Path,
    ) -> PushResult:
        """Run push with config-derived parameters.

        Args:
            pusher: Push client for dependency injection.
            data_dir: Data directory path.

        Returns:
            PushResult with upload details.
        """
        push_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_push=self.config.delete_after_push,
        )
        bucket = self.config.bucket or os.environ.get("S3_BUCKET", "")
        return pusher.upload(
            data_dir=str(data_dir),
            bucket=bucket,
            prefix=self.config.prefix,
            overwrite=push_config.overwrite,
            delete_after_push=push_config.delete_after_push,
        )

    def _rebuild_manifest(self, s3_list: S3ListClient) -> dict[str, Any]:
        """Rebuild manifest from S3 listing in incremental mode.

        Args:
            s3_list: S3 list client for dependency injection.

        Returns:
            Manifest dict adapted from S3 objects.
        """
        bucket = self.config.bucket or os.environ.get("S3_BUCKET", "")
        prefix = self.config.prefix
        s3_objects = s3_list.list(bucket=bucket, prefix=prefix)
        return adapt_s3_to_manifest(s3_objects, prefix)


class _RealS3ListClient:
    """Real S3 list client wrapper for dependency injection."""

    def list(self, bucket: str, prefix: str) -> list[S3Object]:
        """List S3 objects via real S3 API."""
        return list_s3_objects(bucket=bucket, prefix=prefix)


class _RealExtractClient:
    """Real extract client wrapper for dependency injection."""

    def run(
        self,
        data_dir: str,
        types: list[str] | None = None,
        from_year: int = 2024,
        to_year: int = 2024,
        mode: str = "incremental",
        push_manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run real extract operation."""
        return extract_run(
            data_dir=data_dir,
            types=types or ["yellow", "green", "fhv", "fhvhv"],
            from_year=from_year,
            to_year=to_year,
            mode=mode,
            push_manifest=push_manifest,
        )


class _RealPushClient:
    """Real push client wrapper for dependency injection."""

    def upload(
        self,
        data_dir: str,
        bucket: str = "",
        prefix: str = "",
        overwrite: bool = False,
        delete_after_push: bool = False,
    ) -> PushResult:
        """Run real push operation."""
        config = UploadConfig(
            overwrite=overwrite,
            delete_after_push=delete_after_push,
        )
        return upload_from_env(
            data_dir=data_dir,
            config=config,
            bucket=bucket,
            prefix=prefix,
        )
