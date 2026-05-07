"""Retry policy — exponential backoff using tenacity."""

from __future__ import annotations

import logging
from collections.abc import Callable

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Exponential backoff retry policy using tenacity.

    Args:
        max_retries: Maximum number of retry attempts.
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
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.base_delay, min=self.base_delay, max=60),
            retry=retry_if_exception_type(Exception),
            before_sleep=self._make_before_sleep(stage_name),
            after=self._make_after(stage_name),
        )
        def wrapped() -> object:
            return operation()

        return wrapped()

    def _make_before_sleep(self, stage_name: str):
        """Create before_sleep callback for retry."""
        def callback(retry_state):
            logger.warning(
                "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                stage_name,
                retry_state.attempt_number,
                self.max_retries,
                retry_state.next_action.sleep,
                retry_state.outcome.exception(),
            )
        return callback

    def _make_after(self, stage_name: str):
        """Create after callback for retry."""
        def callback(retry_state):
            if retry_state.attempt_number == self.max_retries:
                logger.error(
                    "%s failed after %d attempts: %s",
                    stage_name,
                    self.max_retries,
                    retry_state.outcome.exception(),
                )
        return callback
