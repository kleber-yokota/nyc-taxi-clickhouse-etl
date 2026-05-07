"""Custom exceptions for the upload module."""

from __future__ import annotations


class S3ClientError(Exception):
    """Base exception for S3 client operations."""
