"""Tests for file_utils.stream module."""

import pytest

from file_utils.stream import (
    s3_client,
    upload,
    s3_download,
    list_keys,
    object_size,
    delete,
)


class TestS3Client:
    """Tests for s3_client function."""
    
    def test_s3_client_default(self):
        """Test creating a client with no arguments."""
        client = s3_client()
        assert hasattr(client, "_service_model")
        assert client._service_model.service_name == "s3"
    
    def test_s3_client_with_endpoint(self):
        """Test creating a client with endpoint URL."""
        client = s3_client(endpoint_url="https://s3.fr-par.scw.cloud")
        assert hasattr(client, "_service_model")
    
    def test_s3_client_with_region(self):
        """Test creating a client with region."""
        client = s3_client(region="fr-par")
        assert hasattr(client, "_service_model")


class TestUpload:
    """Tests for upload function."""
    
    def test_non_s3_dst_raises(self, tmp_path):
        """Test that non-S3 destination raises ValueError."""
        src = tmp_path / "test.txt"
        src.write_text("content")
        
        with pytest.raises(ValueError, match="s3_url must start with s3://"):
            upload(str(src), "not-s3://bucket/key")


class TestDownload:
    """Tests for s3_download function."""
    
    def test_non_s3_src_raises(self, tmp_path):
        """Test that non-S3 source raises ValueError."""
        dst = tmp_path / "test.txt"
        
        with pytest.raises(ValueError, match="s3_url must start with s3://"):
            s3_download("not-s3://bucket/key", str(dst))


class TestListKeys:
    """Tests for list_keys function."""
    
    def test_non_s3_url_raises(self):
        """Test that non-S3 URL raises ValueError."""
        with pytest.raises(ValueError, match="s3_url must start with s3://"):
            list(list_keys("not-s3://bucket/prefix"))


class TestObjectSize:
    """Tests for object_size function."""
    
    def test_non_s3_url_raises(self):
        """Test that non-S3 URL raises ValueError."""
        with pytest.raises(ValueError, match="s3_url must start with s3://"):
            object_size("not-s3://bucket/key")


class TestDelete:
    """Tests for delete function."""
    
    def test_non_s3_url_raises(self):
        """Test that non-S3 URL raises ValueError."""
        with pytest.raises(ValueError, match="s3_url must start with s3://"):
            delete("not-s3://bucket/key")
