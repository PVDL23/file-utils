"""S3 streaming operations using boto3."""

from __future__ import annotations

import os
from typing import Iterator
import boto3
from botocore.config import Config


def s3_client(endpoint_url: str | None = None, region: str | None = None, **kwargs) -> boto3.client:
    """
    Create an S3 client with configurable endpoint and region.
    
    Args:
        endpoint_url: S3 endpoint URL (overrides env vars)
        region: AWS region (overrides env vars)
        **kwargs: Additional arguments passed to boto3.client
        
    Returns:
        A boto3 S3 client
    """
    # Determine endpoint
    if endpoint_url is None:
        endpoint_url = os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    
    # Determine region
    if region is None:
        region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("S3_REGION")
    
    # Build client config
    config_kwargs = {}
    if endpoint_url:
        config_kwargs["endpoint_url"] = endpoint_url
    if region:
        config_kwargs["region_name"] = region
    
    # Create client
    client = boto3.client("s3", **config_kwargs, **kwargs)
    
    return client


def _parse_s3_url(s3_url: str) -> tuple[str, str]:
    """
    Parse an S3 URL into (bucket, key).
    
    Args:
        s3_url: S3 URL (must start with s3://)
        
    Returns:
        Tuple of (bucket, key)
        
    Raises:
        ValueError: If URL is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"Not a valid S3 URL: {s3_url}")
    
    # Remove scheme
    if "://" in s3_url:
        s3_url = s3_url.split("://", 1)[1]
    
    parts = s3_url.split("/", 1)
    if not parts[0]:
        raise ValueError(f"Empty bucket in S3 URL: {s3_url}")
    
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    return bucket, key


def upload(src: str, s3_url: str, client: boto3.client | None = None, *,
           threshold_mb: int = 64, chunk_mb: int = 64,
           max_concurrency: int = 8, extra_args: dict | None = None) -> str:
    """
    Upload a file to S3 with multipart support.
    
    Args:
        src: Source path (local or s3://)
        s3_url: Destination S3 URL
        client: Optional S3 client (created if None)
        threshold_mb: Minimum file size in MB for multipart upload
        chunk_mb: Chunk size in MB for multipart upload
        max_concurrency: Maximum number of concurrent uploads
        extra_args: Extra arguments passed to upload_fileobj
        
    Returns:
        The S3 URL
        
    Raises:
        ValueError: If s3_url is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"s3_url must start with s3://, got: {s3_url}")
    
    bucket, key = _parse_s3_url(s3_url)
    
    if client is None:
        client = s3_client()
    
    # Build transfer config
    transfer_config = Config(
        multipart_threshold=threshold_mb * 1024 * 1024,
        multipart_chunksize=chunk_mb * 1024 * 1024,
        max_concurrent_requests=max_concurrency,
    )
    
    # Open source file
    from file_utils.io import open_any
    file_obj = open_any(src, "rb")
    
    extra_args = extra_args or {}
    
    try:
        client.upload_fileobj(
            file_obj,
            bucket,
            key,
            Config=transfer_config,
            ExtraArgs=extra_args,
        )
    finally:
        file_obj.close()
    
    return s3_url


def download(s3_url: str, dst: str, client: boto3.client | None = None, *,
             threshold_mb: int = 64, chunk_mb: int = 64,
             max_concurrency: int = 8) -> str:
    """
    Download a file from S3.
    
    Args:
        s3_url: Source S3 URL
        dst: Destination path (local or s3://)
        client: Optional S3 client (created if None)
        threshold_mb: Minimum file size in MB for multipart download
        chunk_mb: Chunk size in MB for multipart download
        max_concurrency: Maximum number of concurrent downloads
        
    Returns:
        The destination path
        
    Raises:
        ValueError: If s3_url is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"s3_url must start with s3://, got: {s3_url}")
    
    bucket, key = _parse_s3_url(s3_url)
    
    if client is None:
        client = s3_client()
    
    # Build transfer config
    transfer_config = Config(
        multipart_threshold=threshold_mb * 1024 * 1024,
        multipart_chunksize=chunk_mb * 1024 * 1024,
        max_concurrent_requests=max_concurrency,
    )
    
    # Open destination file
    from file_utils.io import open_any
    file_obj = open_any(dst, "wb")
    
    try:
        client.download_fileobj(
            bucket,
            key,
            file_obj,
            Config=transfer_config,
        )
    finally:
        file_obj.close()
    
    return dst


def list_keys(s3_url: str, client: boto3.client | None = None) -> Iterator[str]:
    """
    List keys under a prefix in S3.
    
    Args:
        s3_url: S3 URL with optional prefix
        client: Optional S3 client (created if None)
        
    Yields:
        Key names
        
    Raises:
        ValueError: If s3_url is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"s3_url must start with s3://, got: {s3_url}")
    
    bucket, prefix = _parse_s3_url(s3_url)
    
    if client is None:
        client = s3_client()
    
    paginator = client.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def object_size(s3_url: str, client: boto3.client | None = None) -> int:
    """
    Get the size of an S3 object in bytes.
    
    Args:
        s3_url: S3 URL of the object
        client: Optional S3 client (created if None)
        
    Returns:
        Size in bytes
        
    Raises:
        ValueError: If s3_url is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"s3_url must start with s3://, got: {s3_url}")
    
    bucket, key = _parse_s3_url(s3_url)
    
    if client is None:
        client = s3_client()
    
    response = client.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


def delete(s3_url: str, client: boto3.client | None = None) -> None:
    """
    Delete an S3 object.
    
    Args:
        s3_url: S3 URL of the object to delete
        client: Optional S3 client (created if None)
        
    Raises:
        ValueError: If s3_url is not a valid S3 URL
    """
    if not s3_url.startswith("s3://") and not s3_url.startswith("s3a://") and not s3_url.startswith("s3n://"):
        raise ValueError(f"s3_url must start with s3://, got: {s3_url}")
    
    bucket, key = _parse_s3_url(s3_url)
    
    if client is None:
        client = s3_client()
    
    client.delete_object(Bucket=bucket, Key=key)
