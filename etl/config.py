"""ETL pipeline configuration."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ETLConfig:
    """Configuration for the ETL orchestrator.

    Args:
        types: Data types to extract (e.g. ['yellow', 'green']). Defaults to all.
        from_year: Starting year (inclusive). Defaults to 2009.
        to_year: Ending year (inclusive). Defaults to current year.
        mode: 'incremental' or 'full'. Defaults to 'incremental'.
        delete_after_upload: Delete local files after successful upload.
    """
    types: set[str] | None = None
    from_year: int | None = None
    to_year: int | None = None
    mode: str = "incremental"
    delete_after_upload: bool = False
