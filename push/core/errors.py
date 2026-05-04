"""Custom exceptions for the push module."""

from __future__ import annotations


class S3ClientError(Exception):
    """Base exception for S3 client operations."""
