"""Upload checksum implementation — delegates to upload module.

This is the concrete implementation of ChecksumProvider that wraps
upload.core.checksum.compute_sha256.
"""

from __future__ import annotations

from pathlib import Path

from .checksum import ChecksumProvider


class UploadChecksum(ChecksumProvider):
    """ChecksumProvider backed by upload.core.checksum.

    Both extract and upload stages use this shared implementation,
    ensuring consistency across the pipeline.
    """

    def compute(self, file_path: Path) -> str:
        """Compute SHA-256 checksum of a file.

        Args:
            file_path: Path to the file.

        Returns:
            Hex digest string of the SHA-256 hash.
        """
        from upload.core.checksum import compute_sha256 as _compute

        return _compute(file_path)
