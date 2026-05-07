"""Interruptible download with atomic file operations and signal handling."""

from __future__ import annotations

import logging
import signal
from pathlib import Path
from typing import Any

from .state import compute_sha256

logger = logging.getLogger(__name__)

# Module-level signal handlers set once to avoid conflicts with
# other code that calls signal.signal() (e.g. KeyboardInterrupt handlers).
# We save the previous handler so we can chain it.
_SIGNAL_SET = False
_OLD_SIGINT = None
_OLD_SIGTERM = None


def _ensure_signal_handler() -> None:
    """Register interrupt signal handlers exactly once at module level."""
    global _SIGNAL_SET, _OLD_SIGINT, _OLD_SIGTERM
    if _SIGNAL_SET:
        return

    def _handle_signal(signum: int, _frame: Any) -> None:
        """Handle interrupt signals by logging and deferring cleanup."""
        logger.info("Interrupt signal received (signal %d)", signum)

    _OLD_SIGINT = signal.signal(signal.SIGINT, _handle_signal)
    _OLD_SIGTERM = signal.signal(signal.SIGTERM, _handle_signal)
    _SIGNAL_SET = True


class InterruptibleDownload:
    """Handles atomic downloads with interrupt and signal cleanup."""

    def __init__(self, data_dir: Path) -> None:
        """Initialize with the root data directory.

        Args:
            data_dir: Base path where data will be stored.
        """
        self.data_dir = data_dir
        self._tmp_path: Path | None = None
        _ensure_signal_handler()

    def cleanup(self) -> None:
        """Remove any active temporary download file."""
        if self._tmp_path:
            if self._tmp_path.exists():
                logger.info("Cleaning up interrupted download: %s", self._tmp_path)
                self._tmp_path.unlink()
            self._tmp_path = None

    def __enter__(self) -> "InterruptibleDownload":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, cleaning up on exception."""
        if exc_type is not None:  # noqa: ARG002
            self.cleanup()
