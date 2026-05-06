"""S3 client — thin wrapper around boto3 for S3-compatible services.

Merges session management and transfer configuration into a single module.
"""

from __future__ import annotations

from typing import Any, BinaryIO

import boto3  # type: ignore[import-untyped]
from boto3.s3.transfer import TransferConfig  # type: ignore[import-untyped]
from botocore.exceptions import NoCredentialsError  # type: ignore[import-untyped]

from .ops import S3Ops
from .ops import (
    create_bucket as _create_bucket,
    delete_object as _delete_object,
    head_object as _head_object,
    list_objects as _list_objects,
    put_object as _put_object,
    upload_fileobj as _upload_fileobj,
)
from .errors import S3ClientError

DEFAULT_PART_SIZE = 5 * 1024 * 1024  # 5MB


def get_s3_client(endpoint_url: str | None = None) -> S3Ops:
    """Create and return a boto3 S3 client.

    Args:
        endpoint_url: S3 endpoint URL (e.g. for MinIO). If None, uses AWS.

    Returns:
        boto3 S3 client implementing S3Ops protocol.

    Raises:
        S3ClientError: If AWS credentials are not available.
    """
    try:
        config: dict[str, Any] = {}
        if endpoint_url:
            config["endpoint_url"] = endpoint_url
        session = boto3.session.Session()
        return session.client("s3", **config)  # type: ignore[no-any-return]
    except NoCredentialsError:
        raise S3ClientError(
            "AWS credentials not found. Set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY."
        )


def _make_transfer_config(part_size: int, multipart_threshold: int) -> TransferConfig:
    """Build a boto3 TransferConfig for multipart uploads.

    Args:
        part_size: Default part size in bytes for multipart uploads.
        multipart_threshold: Threshold in bytes for triggering multipart.

    Returns:
        Configured TransferConfig instance.
    """
    return TransferConfig(
        multipart_threshold=multipart_threshold,
        multipart_chunksize=part_size,
    )


class S3Client:
    """Thin wrapper around boto3 S3 client for S3-compatible services.

    Accepts an S3Ops-compatible client via dependency injection,
    enabling testing with mocks, fakes, or real boto3 clients.
    """

    def __init__(
        self,
        client: S3Ops,
        bucket: str,
        prefix: str = "",
        part_size: int = DEFAULT_PART_SIZE,
    ) -> None:
        """Initialize the S3 client.

        Args:
            client: S3-compatible client implementing S3Ops protocol.
            bucket: S3 bucket name.
            prefix: Key prefix to prepend to all objects.
            part_size: Minimum part size for multipart uploads in bytes.
        """
        self._client = client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.part_size = part_size
        self.endpoint_url: str | None = None

    @classmethod
    def from_env(
        cls,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        part_size: int = DEFAULT_PART_SIZE,
    ) -> "S3Client":
        """Create an S3Client by resolving credentials from environment.

        Args:
            bucket: S3 bucket name.
            prefix: Key prefix to prepend to all objects.
            endpoint_url: S3 endpoint URL (e.g. http://localhost:9000 for MinIO).
            part_size: Minimum part size for multipart uploads in bytes.

        Returns:
            Configured S3Client instance.

        Raises:
            S3ClientError: If AWS credentials are not available.
        """
        client = get_s3_client(endpoint_url)
        instance = cls(
            client=client,
            bucket=bucket,
            prefix=prefix,
            part_size=part_size,
        )
        instance.endpoint_url = endpoint_url
        return instance

    def put_object(
        self,
        key: str,
        body: bytes | BinaryIO,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """Upload a single object to S3.

        Args:
            key: S3 object key.
            body: File content as bytes or file-like object.
            content_type: MIME type of the content.

        Returns:
            Response dict from S3 with ETag.

        Raises:
            S3ClientError: On S3 API errors.
        """
        return _put_object(
            self._client, self.bucket, key, body, content_type,
        )

    def upload_fileobj(
        self,
        key: str,
        fileobj: BinaryIO,
        part_size: int | None = None,
    ) -> None:
        """Upload a file object using multipart upload if needed.

        Args:
            key: S3 object key.
            fileobj: File-like object to upload.
            part_size: Part size for multipart upload.

        Raises:
            S3ClientError: On S3 API errors.
        """
        cfg = _make_transfer_config(
            self.part_size,
            part_size or self.part_size
        )
        _upload_fileobj(self._client, self.bucket, key, fileobj, cfg)

    def head_object(self, key: str) -> dict[str, Any] | None:
        """Check if an object exists in S3.

        Args:
            key: S3 object key.

        Returns:
            Object metadata dict if exists, None otherwise.

        Raises:
            S3ClientError: On S3 API errors other than 404.
        """
        return _head_object(self._client, self.bucket, key)

    def list_objects(self, prefix: str = "") -> list[str]:
        """List object keys under a prefix.

        Args:
            prefix: Prefix to filter objects by.

        Returns:
            List of object key strings.

        Raises:
            S3ClientError: On S3 API errors.
        """
        full_prefix = f"{self.prefix}/{prefix}" if self.prefix else prefix
        return _list_objects(self._client, self.bucket, full_prefix)

    def delete_object(self, key: str) -> None:
        """Delete an object from S3.

        Args:
            key: S3 object key.

        Raises:
            S3ClientError: On S3 API errors.
        """
        _delete_object(self._client, self.bucket, key)

    def create_bucket(self) -> None:
        """Create the bucket if it doesn't exist.

        Raises:
            S3ClientError: On S3 API errors.
        """
        _create_bucket(self._client, self.bucket, self.endpoint_url)

    def build_key(self, relative_path: str) -> str:
        """Build a full S3 key by prepending the prefix.

        Args:
            relative_path: Path relative to the prefix.

        Returns:
            Full S3 key string with prefix.
        """
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return relative_path
