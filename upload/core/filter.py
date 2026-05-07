"""File collection and filtering for the upload module."""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path

from .state import PARQUET_EXTENSION


def collect_files(
    data_dir: Path,
    include: set[str] | None,
    exclude: set[str] | None,
) -> list[Path]:
    """Collect files to upload from data directory.

    Args:
        data_dir: Path to the data directory.
        include: Patterns to include.
        exclude: Patterns to exclude.

    Returns:
        Sorted list of file paths.
    """
    include_patterns = include if include is not None else {f"*{PARQUET_EXTENSION}"}
    default_exclude = {".upload_state.json"}
    exclude_patterns = set(exclude) | default_exclude if exclude is not None else default_exclude
    return _collect_and_filter(data_dir, include_patterns, exclude_patterns)


def _collect_and_filter(
    data_dir: Path,
    include_patterns: set[str],
    exclude_patterns: set[str],
) -> list[Path]:
    """Collect files matching include patterns and filter out excluded ones.

    Args:
        data_dir: Path to the data directory.
        include_patterns: Set of glob patterns to include.
        exclude_patterns: Set of glob patterns to exclude.

    Returns:
        Sorted list of file paths.
    """
    collected: list[Path] = []
    for pattern in include_patterns:
        if pattern == "*":
            collected.extend(data_dir.rglob("*"))
        else:
            collected.extend(data_dir.rglob(pattern))

    result: list[Path] = []
    for f in collected:
        if not f.is_file():
            continue
        rel = str(f.relative_to(data_dir))
        if _matches_any(rel, exclude_patterns):
            continue
        result.append(f)

    return sorted(set(result))


def _matches_any(path: str, patterns: set[str]) -> bool:
    """Check if a path matches any of the exclusion patterns.

    Args:
        path: File path relative to data_dir.
        patterns: Set of glob patterns to exclude.

    Returns:
        True if path matches any exclusion pattern.
    """
    filename = os.path.basename(path)
    for pattern in patterns:
        if _matches_pattern(filename, path, pattern):
            return True
    return False


def _matches_pattern(filename: str, path: str, pattern: str) -> bool:
    """Check if a filename or path matches a single pattern.

    Args:
        filename: The basename of the file.
        path: The full relative path.
        pattern: The glob pattern to match.

    Returns:
        True if the pattern matches.
    """
    if pattern == "*":
        return True
    return fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(path, pattern)
