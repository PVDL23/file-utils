"""Tests for file_utils.io module."""

import pytest
from pathlib import Path

from file_utils.io import is_s3, split_s3, open_any, exists, file_size


class TestIsS3:
    """Tests for is_s3 function."""
    
    def test_s3_url_returns_true(self):
        assert is_s3("s3://bucket/key") is True
        assert is_s3("s3a://bucket/key") is True
        assert is_s3("s3n://bucket/key") is True
    
    def test_local_path_returns_false(self):
        assert is_s3("/path/to/file") is False
        assert is_s3("relative/path") is False
        assert is_s3("file.txt") is False
    
    def test_path_object(self):
        assert is_s3(Path("s3://bucket/key")) is True
        assert is_s3(Path("/path/to/file")) is False


class TestSplitS3:
    """Tests for split_s3 function."""
    
    def test_basic_split(self):
        bucket, key = split_s3("s3://bucket/a/b.parquet")
        assert bucket == "bucket"
        assert key == "a/b.parquet"
    
    def test_no_prefix(self):
        bucket, key = split_s3("s3://bucket/file.txt")
        assert bucket == "bucket"
        assert key == "file.txt"
    
    def test_root_key(self):
        bucket, key = split_s3("s3://bucket")
        assert bucket == "bucket"
        assert key == ""
    
    def test_not_s3_url_raises(self):
        with pytest.raises(ValueError, match="Not an S3 URL"):
            split_s3("bucket/key")
    
    def test_empty_bucket_raises(self):
        with pytest.raises(ValueError, match="Empty bucket"):
            split_s3("s3:///key")
    
    def test_path_object(self):
        bucket, key = split_s3(Path("s3://bucket/a/b.parquet"))
        assert bucket == "bucket"
        assert key == "a/b.parquet"


class TestOpenAny:
    """Tests for open_any function."""
    
    def test_local_binary_roundtrip(self, tmp_path):
        test_file = tmp_path / "test.bin"
        content = b"Hello, World!"
        test_file.write_bytes(content)
        
        with open_any(test_file, "rb") as f:
            assert f.read() == content
    
    def test_local_text_roundtrip(self, tmp_path):
        test_file = tmp_path / "test.txt"
        content = "Hello, World!\n"
        test_file.write_text(content, encoding="utf-8")
        
        with open_any(test_file, "rt", encoding="utf-8") as f:
            assert f.read() == content


class TestExists:
    """Tests for exists function."""
    
    def test_local_file_exists(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        assert exists(test_file) is True
    
    def test_local_file_not_exists(self, tmp_path):
        test_file = tmp_path / "nonexistent.txt"
        assert exists(test_file) is False


class TestFileSize:
    """Tests for file_size function."""
    
    def test_local_file_size(self, tmp_path):
        test_file = tmp_path / "test.txt"
        content = "Hello, World!"
        test_file.write_text(content, encoding="utf-8")
        assert file_size(test_file) == len(content.encode("utf-8"))
    
    def test_empty_file(self, tmp_path):
        test_file = tmp_path / "empty.txt"
        test_file.write_text("", encoding="utf-8")
        assert file_size(test_file) == 0
