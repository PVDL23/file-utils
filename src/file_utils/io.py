"""Uniform local and S3 file access using fsspec."""

from __future__ import annotations

from pathlib import Path
import fsspec


def is_s3(path: str | Path) -> bool:
    """Check if a path is an S3 URL (s3://, s3a://, s3n://)."""
    path_str = str(path).replace("\\", "/")  # Normalize path separators
    
    # Normalize single slash after scheme (e.g., s3:/ -> s3://)
    # Check for "s3:/" followed by non-slash or end of string
    if len(path_str) >= 4 and path_str[:4] == "s3:/" and (len(path_str) == 4 or path_str[4] != "/"):
        path_str = "s3://" + path_str[4:]
    elif len(path_str) >= 5 and path_str[:5] == "s3a:/" and (len(path_str) == 5 or path_str[5] != "/"):
        path_str = "s3a://" + path_str[5:]
    elif len(path_str) >= 5 and path_str[:5] == "s3n:/" and (len(path_str) == 5 or path_str[5] != "/"):
        path_str = "s3n://" + path_str[5:]
    
    return path_str.startswith("s3://") or path_str.startswith("s3a://") or path_str.startswith("s3n://")


def split_s3(path: str | Path) -> tuple[str, str]:
    """
    Split an S3 URL into (bucket, key).
    
    Args:
        path: An s3://bucket/key path
        
    Returns:
        A tuple of (bucket, key)
        
    Raises:
        ValueError: If not an S3 URL or bucket is empty
    """
    path_str = str(path).replace("\\", "/")
    
    # Normalize: on Linux, Path("s3://bucket") becomes "s3:/bucket"
    # So we need to fix single slash after scheme
    # Check for "s3:/" followed by non-slash (meaning there's only one slash)
    if len(path_str) >= 4 and path_str[:4] == "s3:/" and (len(path_str) == 4 or path_str[4] != "/"):
        path_str = "s3://" + path_str[4:]  # Replace "s3:/..." with "s3://..."
    elif len(path_str) >= 5 and path_str[:5] == "s3a:/" and (len(path_str) == 5 or path_str[5] != "/"):
        path_str = "s3a://" + path_str[5:]  # Replace "s3a:/..." with "s3a://..."
    elif len(path_str) >= 5 and path_str[:5] == "s3n:/" and (len(path_str) == 5 or path_str[5] != "/"):
        path_str = "s3n://" + path_str[5:]  # Replace "s3n:/..." with "s3n://..."
    
    if not is_s3(path_str):
        raise ValueError(f"Not an S3 URL: {path_str}")
    
    # Remove the scheme
    if "://" in path_str:
        path_str = path_str.split("://", 1)[1]
    
    parts = path_str.split("/", 1)
    if not parts[0]:
        raise ValueError(f"Empty bucket in S3 URL: {path}")
    
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    return bucket, key


def open_any(path: str | Path, mode: str = "rb", **kwargs) -> any:
    """
    Open a file (local or S3) using fsspec.
    
    Args:
        path: Local path or S3 URL
        mode: File mode ('rb', 'wb', 'rt', 'wt', etc.)
        **kwargs: Additional arguments passed to fsspec.open
        
    Returns:
        An open file-like object
    """
    path_str = str(path)
    
    # For S3 paths, we need to use the s3 filesystem
    if is_s3(path_str):
        fs = fsspec.filesystem("s3", anon=False)
        return fs.open(path_str, mode, **kwargs)
    else:
        # Local path
        return fsspec.open(path_str, mode, **kwargs).open()


def exists(path: str | Path) -> bool:
    """Check if a path (local or S3) exists."""
    path_str = str(path)
    if is_s3(path_str):
        fs = fsspec.filesystem("s3", anon=False)
        return fs.exists(path_str)
    else:
        fs = fsspec.filesystem("file")
        return fs.exists(path_str)


def file_size(path: str | Path) -> int:
    """Get the size in bytes of a path (local or S3)."""
    path_str = str(path)
    if is_s3(path_str):
        fs = fsspec.filesystem("s3", anon=False)
        return fs.size(path_str)
    else:
        fs = fsspec.filesystem("file")
        return fs.size(path_str)
