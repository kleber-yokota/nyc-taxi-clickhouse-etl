"""FakeS3Client — in-memory S3 client for unit tests (no mocks needed).

Per AGENTS.md, fakes reduce the Mock/Assert Ratio and make tests more robust.
"""

from __future__ import annotations

from typing import BinaryIO


class FakeS3Client:
    """In-memory fake S3 client for testing — no mocks needed."""

    def __init__(self, bucket: str = "b", prefix: str = "") -> None:
        """Initialize with empty object store.

        Args:
            bucket: Fake bucket name.
            prefix: Fake key prefix.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict] = {}
        self.upload_count = 0

    def build_key(self, relative_path: str) -> str:
        """Build full S3 key from relative path.

        Args:
            relative_path: Path relative to the prefix.

        Returns:
            Full S3 key string with prefix.
        """
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return relative_path

    def put_object(self, key: str, body: bytes | BinaryIO, **kwargs: object) -> dict:
        """Store object in memory.

        Args:
            key: S3 object key.
            body: File content as bytes or file-like object.

        Returns:
            Response dict with ETag.
        """
        if isinstance(body, bytes):
            self.objects[key] = body
        else:
            self.objects[key] = body.read()
        return {"ETag": '"fake"'}

    def upload_fileobj(self, key: str, fileobj: BinaryIO, **kwargs: object) -> dict:
        """Upload file object to memory store.

        Args:
            key: S3 object key.
            fileobj: File-like object to upload.

        Returns:
            Response dict with ETag.
        """
        self.upload_count += 1
        self.objects[key] = fileobj.read()
        if "checksum" in kwargs and kwargs["checksum"]:
            self.metadata[key] = {"checksum": kwargs["checksum"]}
        return {"ETag": '"fake"'}

    def head_object(self, key: str) -> dict | None:
        """Check if object exists.

        Args:
            key: S3 object key.

        Returns:
            Object metadata dict if exists, None otherwise.
        """
        if key in self.objects:
            result: dict[str, object] = {
                "ContentLength": len(self.objects[key]),
                "ETag": '"fake"',
            }
            if key in self.metadata:
                result["Metadata"] = self.metadata[key]
            return result
        return None

    def list_objects(self, prefix: str = "") -> list[str]:
        """List keys matching prefix.

        Args:
            prefix: Prefix to filter keys.

        Returns:
            List of matching key strings.
        """
        return [k for k in self.objects if k.startswith(prefix)]

    def delete_object(self, key: str) -> None:
        """Delete object.

        Args:
            key: S3 object key.
        """
        self.objects.pop(key, None)

    def create_bucket(self) -> None:
        """No-op for fake."""

    def set_metadata(self, key: str, metadata: dict) -> None:
        """Set metadata for a key (for testing recovery with checksums).

        Args:
            key: S3 object key.
            metadata: Metadata dict to store.
        """
        self.metadata[key] = metadata
