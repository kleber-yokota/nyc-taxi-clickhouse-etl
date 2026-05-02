"""Track URLs that returned 404 and should be skipped on future runs."""

from __future__ import annotations

from pathlib import Path
from typing import Set

from .state import KNOWN_MISSING_FILE


class KnownMissing:
    """Tracks URLs that returned 404 during download.

    When a URL is not found (404), it is recorded here so that
    subsequent runs skip it. This avoids re-attempting downloads
    of files that are genuinely missing.

    The file can be manually edited — removing a line will cause
    that URL to be attempted again on the next run.
    """

    def __init__(self, known_missing_path: str | Path | None = None) -> None:
        """Initialize with optional custom path.

        Args:
            known_missing_path: Path to the known missing file.
        """
        self._known_missing_path = Path(known_missing_path) if known_missing_path else Path(KNOWN_MISSING_FILE)
        self._urls: Set[str] = set()
        self._load()

    def _load(self) -> None:
        """Load known missing URLs from disk."""
        if self._known_missing_path.exists():
            try:
                content = self._known_missing_path.read_text().strip()
                if not content:
                    self._urls = set()
                    return
                self._urls = set(line for line in content.split("\n") if line and line.startswith("http"))
            except OSError:
                self._urls = set()

    def is_missing(self, url: str) -> bool:
        """Check if a URL is recorded as missing.

        Args:
            url: The URL to check.

        Returns:
            True if the URL was previously recorded as missing.
        """
        return url in self._urls

    def add(self, url: str) -> None:
        """Record a URL as missing.

        Args:
            url: The URL that returned 404.
        """
        self._urls.add(url)
        self._persist()

    def _persist(self) -> None:
        """Persist known missing URLs to disk."""
        parent = self._known_missing_path.parent
        if parent != Path("."):
            parent.mkdir(parents=True, exist_ok=True)
        lines = sorted(self._urls)
        self._known_missing_path.write_text("\n".join(lines) + "\n")

    def clear(self) -> None:
        """Clear all recorded missing URLs."""
        self._urls = set()
        self._persist()
