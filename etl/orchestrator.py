"""ETL orchestrator — coordinates extract and push pipeline.

Runs extract then push in sequence, using a shared push manifest so
extract knows what is already in S3 and push knows what has been sent.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from extract.downloader.downloader import run as extract_run
from push.core.runner import upload_from_env
from push.core.state import UploadConfig

from .manifest import add_entry, load, save

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    """Configuration for the ETL pipeline.

    Args:
        types: Data types to extract. Defaults to all.
        from_year: Starting year (inclusive). Defaults to 2024.
        to_year: Ending year (inclusive). Defaults to 2024.
        mode: "incremental" or "full". Defaults to "incremental".
        delete_after_push: Delete local files after upload. Defaults to True.
        s3_prefix: S3 key prefix. Defaults to "data".
    """
    types: list[str] = field(
        default_factory=lambda: ["yellow", "green", "fhv", "fhvhv"]
    )
    from_year: int = 2024
    to_year: int = 2024
    mode: str = "incremental"
    delete_after_push: bool = True
    s3_prefix: str = "data"


class Orchestrator:
    """Coordinates extract → push pipeline.

    Uses the shared push manifest so extract knows what is already in S3.
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or ETLConfig()

    def run(self) -> dict:
        """Run the full ETL pipeline.

        Loads environment, runs extract with push manifest,
        then push, then updates push manifest with uploaded files.

        Returns:
            Dict with 'extract' and 'push' result dicts.
        """
        load_dotenv()

        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)

        s3_prefix = os.environ.get("S3_PREFIX", self.config.s3_prefix)

        manifest = load(data_dir)

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

        logger.info("=== Push starting ===")
        push_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_push=self.config.delete_after_push,
        )
        push_result = upload_from_env(
            data_dir=str(data_dir),
            config=push_config,
        )
        logger.info("Push complete: %s", push_result)

        for rel_path in push_result.uploaded_files:
            s3_key = f"{s3_prefix}/{rel_path}"
            checksum = push_result.uploaded_checksums.get(rel_path, "")
            add_entry(manifest, rel_path, s3_key, checksum)

        save(data_dir, manifest)

        return {
            "extract": extract_result,
            "push": {
                "uploaded": push_result.uploaded,
                "skipped": push_result.skipped,
                "failed": push_result.failed,
                "total": push_result.total,
                "uploaded_files": push_result.uploaded_files,
            },
        }
