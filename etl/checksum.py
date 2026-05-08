"""Checksum protocol — abstract interface for file checksum computation.

The etl module defines this protocol. Other modules (extract, upload) receive
a callable matching this protocol, so they don't depend on the etl module's
checksum implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class ChecksumProvider(Protocol):
    """Protocol for computing file checksums.

    Any class or callable that implements this protocol can be used
    wherever a ChecksumProvider is expected.
    """

    def compute(self, file_path: Path) -> str:
        """Compute checksum of a file.

        Args:
            file_path: Path to the file.

        Returns:
            Hex digest string of the hash.
        """
        ...
