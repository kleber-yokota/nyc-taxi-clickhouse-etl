"""Retry policy — exponential backoff with configurable limits."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Exponential backoff retry policy.

    Args:
        max_retries: Maximum number of retry attempts (not counting the first).
        base_delay: Base delay in seconds for exponential backoff.
    """

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay

    def execute(self, stage_name: str, operation: Callable[[], object]) -> object:
        """Execute operation with exponential backoff retry.

        Args:
            stage_name: Name of the stage for logging.
            operation: Function to execute.

        Returns:
            Result of the operation.

        Raises:
            The last exception if all retries are exhausted.
        """
        for attempt in range(self.max_retries):
            try:
                return operation()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    self._log_retry(stage_name, attempt, e)
                    time.sleep(self.base_delay * (2 ** attempt))
                else:
                    logger.error("%s failed after %d attempts: %s", stage_name, self.max_retries, e)
                    raise
        raise RuntimeError(f"{stage_name} failed after retries")

    def _log_retry(self, stage: str, attempt: int, error: Exception) -> None:
        """Log retry attempt with exponential backoff."""
        backoff = self.base_delay * (2 ** attempt)
        logger.warning(
            "%s failed (attempt %d/%d), retrying in %ds: %s",
            stage, attempt + 1, self.max_retries, backoff, error,
        )
