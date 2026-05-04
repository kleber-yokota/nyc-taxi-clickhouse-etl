"""ETL entry point — runs extract then push in a single script.

METRICS REPORT
CC: 2
LoC: 1
MI: 80
Mutation: N/A
Coverage: N/A
Side Effects: Loads .env, runs extract + push, prints summary
"""

from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv

from extract.core.downloader import run as extract_run
from push.core.runner import upload_from_env
from push.core.state import UploadConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run extract then push with auto-delete.

    Loads environment variables from .env, downloads parquet files
    from the NYC TLC CDN, then uploads them to Garage object storage
    and deletes the local files after successful upload.

    Returns:
        None
    """
    load_dotenv()

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    logger.info("=== Extracting NYC Taxi Data ===")
    extract_result = extract_run(
        data_dir=str(data_dir),
        types=["yellow", "green", "fhv", "fhvhv"],
        from_year=2024,
        to_year=2024,
        mode="incremental",
    )
    logger.info("Extract complete: %s", extract_result)

    logger.info("=== Pushing to Garage ===")
    upload_config = UploadConfig(delete_after_push=True)
    push_result = upload_from_env(
        data_dir=str(data_dir),
        config=upload_config,
    )
    logger.info("Push complete: %s", push_result)


if __name__ == "__main__":
    main()
