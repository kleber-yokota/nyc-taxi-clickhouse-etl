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

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from extract.downloader.downloader import run as extract_run
from push.core.push_manifest import list_s3_objects
from push.core.push_manifest import S3Object
from push.core.runner import upload_from_env
from push.core.state import PushResult
from push.core.state import UploadConfig

from .manifest import load, save, update_from_entries

logger = logging.getLogger(__name__)


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


class Orchestrator:
    """Coordinates extract -> push pipeline.

    The orchestrator is the authority on the push manifest:
    - Creates, updates, and saves the manifest
    - Delegates download to extract
    - Delegates upload to push
    - Rebuilds manifest via push when missing/outdated (incremental)
    - Resolves divergences between extract and push results
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        """Initialize the orchestrator.

        Args:
            config: ETL pipeline configuration. Uses defaults if None.
        """
        self.config = config or ETLConfig()

    def run(self) -> dict:
        """Run the full ETL pipeline.

        1. Load environment variables
        2. Load existing manifest (or empty dict)
        3. If incremental + manifest missing/outdated:
           -> call push.list_s3_objects() to get neutral list
           -> adapt S3Object[] -> manifest dict for extract
        4. Run extract (downloads missing files, uses manifest for skip)
        5. Run push (uploads files, returns PushedEntry[])
        6. Build manifest from push's PushedEntry[]
        7. Save manifest to disk
        8. Reconcile divergences (missing files -> re-download, re-push)

        Returns:
            Dict with 'extract', 'push', and 'reconciled' result dicts.
        """
        load_dotenv()

        data_dir = Path(self.config.data_dir)
        data_dir.mkdir(exist_ok=True)

        # 1. Load existing manifest (extract reads it for skip)
        manifest = load(data_dir)

        # 2. Rebuild manifest from S3 if missing/outdated (incremental only)
        #    Push returns neutral S3Object[], orchestrator adapts to manifest dict
        if self.config.mode == "incremental" and not manifest:
            logger.info("Manifest missing, listing S3 via push")
            bucket = self.config.bucket or os.environ.get("S3_BUCKET", "")
            prefix = self.config.prefix
            s3_objects = list_s3_objects(bucket=bucket, prefix=prefix)
            manifest = self._adapt_s3_to_manifest(s3_objects, prefix)
            logger.info("Adapted %d S3 objects into manifest", len(manifest))

        # 3. Extract (reads manifest to skip already-pushed files)
        logger.info("=== Extract starting (mode=%s) ===", self.config.mode)
        extract_result = extract_run(
            data_dir=str(data_dir),
            types=self.config.types,
            from_year=self.config.from_year,
            to_year=self.config.to_year,
            mode=self.config.mode,
            push_manifest=manifest,
        )
        logger.info("Extract complete: %s", extract_result)

        # 4. Push (uploads files, returns PushResult with uploaded_entries)
        logger.info("=== Push starting ===")
        push_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_push=self.config.delete_after_push,
        )
        bucket = self.config.bucket or os.environ.get("S3_BUCKET", "")
        push_result = upload_from_env(
            data_dir=str(data_dir),
            config=push_config,
            bucket=bucket,
            prefix=self.config.prefix,
        )
        logger.info("Push complete: %s", push_result)

        # 5. Build manifest from push's uploaded_entries
        update_from_entries(manifest, push_result.uploaded_entries)

        # 6. Save manifest to disk
        save(data_dir, manifest)

        # 7. Reconcile divergences
        reconciled = self._reconcile(data_dir, manifest, push_result)

        return {
            "extract": extract_result,
            "push": push_result,
            "reconciled": reconciled,
        }

    def _adapt_s3_to_manifest(
        self,
        s3_objects: list[S3Object],
        prefix: str,
    ) -> dict:
        """Adapt neutral S3Object[] into manifest dict for extract.

        Push returns S3Object[key] — neutral, no manifest awareness.
        Extract expects manifest as dict with rel_path as keys.
        Orchestrator bridges the gap.

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

    def _reconcile(
        self,
        data_dir: Path,
        manifest: dict,
        push_result: PushResult,
    ) -> dict:
        """Detect and fix divergences between extract and push.

        Example: file was downloaded but not pushed (or deleted after push).
        In incremental mode, orchestrator re-downloads and re-pushes it.

        Args:
            data_dir: Base data directory.
            manifest: Current manifest dict.
            push_result: Result from push.upload_from_env().

        Returns:
            Dict with reconciliation details.
        """
        # TODO: implement divergence detection
        # - List all .parquet files in data_dir
        # - Subtract manifest.keys()
        # - For each missing entry: re-push if exists on disk, re-extract if not
        return {"rebuilt": 0, "recovered": 0}
