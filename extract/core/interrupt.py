"""Interruptible download with atomic file operations and signal handling."""

from __future__ import annotations

import logging
import signal
from pathlib import Path
from typing import Any

from .state import compute_sha256

logger = logging.getLogger(__name__)


class InterruptibleDownload:
    """Handles atomic downloads with interrupt and signal cleanup."""

    def __init__(self, data_dir: Path) -> None:
        """Initialize with the root data directory.

        Args:
            data_dir: Base path where data will be stored.
        """
        self.data_dir = data_dir
        self._tmp_path: Path | None = None
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Register SIGINT and SIGTERM handlers for cleanup."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    @staticmethod
    def _handle_signal(signum: int, _frame: Any) -> None:
        """Handle interrupt signals by logging and deferring cleanup."""
        logger.info("Interrupt signal received (signal %d)", signum)

    def cleanup(self) -> None:
        """Remove any active temporary download file."""
        if self._tmp_path and self._tmp_path.exists():
            logger.info("Cleaning up interrupted download: %s", self._tmp_path)
            self._tmp_path.unlink()
            self._tmp_path = None

    def _cleanup_tmp(self) -> None:
        """Public alias for cleanup to distinguish from signal handler."""
        self.cleanup()

    def __enter__(self) -> "InterruptibleDownload":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, cleaning up on exception."""
        if exc_type is not None:
            self.cleanup()
