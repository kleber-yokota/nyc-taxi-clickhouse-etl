"""Checksum provider — composes upload module's checksum.

Single source of truth: delegates to upload.core.checksum.compute_sha256.
Both extract and upload use orchestrator's checksum, ensuring consistency.
"""

from __future__ import annotations

from pathlib import Path


class Checksum:
    """Checksum provider — composes upload module's checksum.

    Delegates to upload.core.checksum.compute_sha256.
    Both extract and upload use orchestrator's checksum, ensuring consistency.
    """

    def compute_sha256(self, file_path: Path) -> str:
        """Compute SHA-256 checksum of a file.

        Args:
            file_path: Path to the file.

        Returns:
            Hex digest string of the SHA-256 hash.
        """
        from upload.core.checksum import compute_sha256 as _compute
        return _compute(file_path)
