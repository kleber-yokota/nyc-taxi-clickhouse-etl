"""State management for download progress and error logging."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .state import ERRORS_DIR, ERRORS_LOG, STATE_FILE

logger = logging.getLogger(__name__)


class State:
    """Manages download state, checksums, and error logging."""

    def __init__(
        self,
        state_path: str | Path | None = None,
        errors_dir: str | Path | None = None,
    ) -> None:
        """Initialize State with optional custom paths.

        Args:
            state_path: Optional path to the state JSON file. Defaults to STATE_FILE.
            errors_dir: Optional path to the errors directory. Defaults to ERRORS_DIR.
        """
        self.state_path = Path(state_path) if state_path else STATE_FILE
        self.checksums: dict[str, str] = {}
        self._errors: list[dict[str, Any]] = []

        if errors_dir:
            self._errors_dir = Path(errors_dir)
        else:
            state_parent = self.state_path.parent
            if state_parent != Path("."):
                self._errors_dir = state_parent / "errors"
            else:
                self._errors_dir = Path(ERRORS_DIR)

        self._load()

    def save(self, url: str, checksum: str) -> None:
        """Save a URL-checksum pair to state.

        Args:
            url: The CDN URL of the downloaded file.
            checksum: SHA-256 hex digest of the file content.
        """
        self.checksums[url] = checksum
        self._persist()

    def get_checksum(self, url: str) -> str | None:
        """Get the stored checksum for a URL.

        Args:
            url: The CDN URL to look up.

        Returns:
            The checksum string if found, None otherwise.
        """
        return self.checksums.get(url)

    def is_downloaded(self, url: str) -> bool:
        """Check if a URL has been previously downloaded.

        Args:
            url: The CDN URL to check.

        Returns:
            True if the URL is in the state checksums, False otherwise.
        """
        return url in self.checksums

    def reset(self) -> None:
        """Clear all stored checksums and persist the empty state."""
        self.checksums = {}
        self._persist()

    def log_error(self, url: str, error_type: ErrorType, detail: str = "") -> None:
        """Log a download error to the errors log file.

        Args:
            url: The CDN URL that failed.
            error_type: The type of error that occurred.
            detail: Additional context about the error.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": error_type.value,
            "url": url,
            "detail": detail,
        }
        self._errors.append(entry)
        self._persist_errors()

    def _load(self) -> None:
        """Load existing state from disk, initializing if missing."""
        path = Path(self.state_path)
        if path.exists():
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                self.checksums = data.get("checksums", {})
            except (json.JSONDecodeError, KeyError):
                self.checksums = {}
        self._ensure_dirs()

    def _persist(self) -> None:
        """Persist current checksums to the state JSON file."""
        self._ensure_dirs()
        path = Path(self.state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump({"checksums": self.checksums}, f, indent=2)

    def _ensure_dirs(self) -> None:
        """Create the state file parent and errors directories if they don't exist."""
        Path(self.state_path).parent.mkdir(parents=True, exist_ok=True)
        self._errors_dir.mkdir(parents=True, exist_ok=True)

    def _persist_errors(self) -> None:
        """Write accumulated error entries to the errors log file."""
        self._errors_dir.mkdir(parents=True, exist_ok=True)
        log_path = self._errors_dir / "download_errors.log"
        with open(log_path, "a") as f:
            for entry in self._errors:
                f.write(json.dumps(entry) + "\n")
        self._errors = []
