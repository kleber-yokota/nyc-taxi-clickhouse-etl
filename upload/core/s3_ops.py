"""S3 operation wrappers for the upload module."""

from __future__ import annotations

from typing import BinaryIO

from botocore.exceptions import ClientError

from .errors import S3ClientError
from .ops import S3Ops


def put_object(
    client: S3Ops,
    bucket: str,
    key: str,
    body: bytes | BinaryIO,
    content_type: str,
) -> dict:
    """Upload a single object to S3.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        key: S3 object key.
        body: File content as bytes or file-like object.
        content_type: MIME type of the content.

    Returns:
        Response dict from S3 with ETag metadata.

    Raises:
        S3ClientError: On S3 API errors.
    """
    try:
        return client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType=content_type,
        )
    except ClientError as e:
        raise S3ClientError(f"PutObject failed for {key}: {e}")


def upload_fileobj(
    client: S3Ops,
    bucket: str,
    key: str,
    fileobj: BinaryIO,
    config: object,
) -> None:
    """Upload a file object to S3 using multipart transfer if needed.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        key: S3 object key.
        fileobj: File-like object to upload.
        config: boto3 TransferConfig for multipart settings.

    Raises:
        S3ClientError: On S3 API errors.
    """
    try:
        client.upload_fileobj(Fileobj=fileobj, Bucket=bucket, Key=key, Config=config)
    except ClientError as e:
        raise S3ClientError(f"UploadFileObj failed for {key}: {e}")


def head_object(client: S3Ops, bucket: str, key: str) -> dict | None:
    """Check if an object exists in S3.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        Object metadata dict if exists, None if not found (404).

    Raises:
        S3ClientError: On S3 API errors other than 404.
    """
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return None
        raise S3ClientError(f"HeadObject failed for {key}: {e}")


def list_objects(
    client: S3Ops,
    bucket: str,
    prefix: str,
) -> list[str]:
    """List object keys under a prefix using pagination.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        prefix: Key prefix to filter objects.

    Returns:
        List of object key strings.

    Raises:
        S3ClientError: On S3 API errors.
    """
    try:
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
    except ClientError as e:
        raise S3ClientError(f"ListObjects failed for prefix {prefix}: {e}")


def delete_object(client: S3Ops, bucket: str, key: str) -> None:
    """Delete an object from S3.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        key: S3 object key.

    Raises:
        S3ClientError: On S3 API errors.
    """
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise S3ClientError(f"DeleteObject failed for {key}: {e}")


def create_bucket(client: S3Ops, bucket: str, endpoint_url: str | None) -> None:
    """Create the bucket if it doesn't exist.

    For S3-compatible endpoints (with custom URL), calls create_bucket directly.
    For native AWS, uses head_bucket to verify existence.
    Handles race conditions: 409 Conflict is silently ignored.

    Args:
        client: S3-compatible client implementing S3Ops protocol.
        bucket: S3 bucket name.
        endpoint_url: Custom endpoint URL. If provided, create_bucket is used.

    Raises:
        S3ClientError: On S3 API errors other than expected conflicts.
    """
    try:
        if endpoint_url:
            client.create_bucket(Bucket=bucket)
        else:
            client.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "BucketAlreadyOwnedByYou", "409"):
            return
        raise S3ClientError(f"CreateBucket failed for {bucket}: {e}")
