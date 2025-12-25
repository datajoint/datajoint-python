"""
Tests for content-addressed storage (content_registry.py).
"""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from datajoint.content_registry import (
    build_content_path,
    compute_content_hash,
    content_exists,
    delete_content,
    get_content,
    get_content_size,
    put_content,
)
from datajoint.errors import DataJointError


class TestComputeContentHash:
    """Tests for compute_content_hash function."""

    def test_computes_sha256(self):
        """Test that SHA256 hash is computed correctly."""
        data = b"Hello, World!"
        result = compute_content_hash(data)

        # Verify against known SHA256 hash
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected
        assert len(result) == 64  # SHA256 produces 64 hex chars

    def test_empty_bytes(self):
        """Test hashing empty bytes."""
        result = compute_content_hash(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected

    def test_different_content_different_hash(self):
        """Test that different content produces different hashes."""
        hash1 = compute_content_hash(b"content1")
        hash2 = compute_content_hash(b"content2")
        assert hash1 != hash2

    def test_same_content_same_hash(self):
        """Test that same content produces same hash."""
        data = b"identical content"
        hash1 = compute_content_hash(data)
        hash2 = compute_content_hash(data)
        assert hash1 == hash2


class TestBuildContentPath:
    """Tests for build_content_path function."""

    def test_builds_hierarchical_path(self):
        """Test that path is built with proper hierarchy."""
        # Example hash: abcdef...
        test_hash = "abcdef0123456789" * 4  # 64 chars
        result = build_content_path(test_hash)

        # Path should be _content/{hash[:2]}/{hash[2:4]}/{hash}
        assert result == f"_content/ab/cd/{test_hash}"

    def test_rejects_invalid_hash_length(self):
        """Test that invalid hash length raises error."""
        with pytest.raises(DataJointError, match="Invalid content hash length"):
            build_content_path("tooshort")

        with pytest.raises(DataJointError, match="Invalid content hash length"):
            build_content_path("a" * 65)  # Too long

    def test_real_hash_path(self):
        """Test path building with a real computed hash."""
        data = b"test content"
        content_hash = compute_content_hash(data)
        path = build_content_path(content_hash)

        # Verify structure
        parts = path.split("/")
        assert parts[0] == "_content"
        assert len(parts[1]) == 2
        assert len(parts[2]) == 2
        assert len(parts[3]) == 64
        assert parts[1] == content_hash[:2]
        assert parts[2] == content_hash[2:4]
        assert parts[3] == content_hash


class TestPutContent:
    """Tests for put_content function."""

    @patch("datajoint.content_registry.get_store_backend")
    def test_stores_new_content(self, mock_get_backend):
        """Test storing new content."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend

        data = b"new content"
        result = put_content(data, store_name="test_store")

        # Verify return value
        assert "hash" in result
        assert result["hash"] == compute_content_hash(data)
        assert result["store"] == "test_store"
        assert result["size"] == len(data)

        # Verify backend was called
        mock_backend.put_buffer.assert_called_once()

    @patch("datajoint.content_registry.get_store_backend")
    def test_deduplicates_existing_content(self, mock_get_backend):
        """Test that existing content is not re-uploaded."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True  # Content already exists
        mock_get_backend.return_value = mock_backend

        data = b"existing content"
        result = put_content(data, store_name="test_store")

        # Verify return value is still correct
        assert result["hash"] == compute_content_hash(data)
        assert result["size"] == len(data)

        # Verify put_buffer was NOT called (deduplication)
        mock_backend.put_buffer.assert_not_called()


class TestGetContent:
    """Tests for get_content function."""

    @patch("datajoint.content_registry.get_store_backend")
    def test_retrieves_content(self, mock_get_backend):
        """Test retrieving content by hash."""
        data = b"stored content"
        content_hash = compute_content_hash(data)

        mock_backend = MagicMock()
        mock_backend.get_buffer.return_value = data
        mock_get_backend.return_value = mock_backend

        result = get_content(content_hash, store_name="test_store")

        assert result == data

    @patch("datajoint.content_registry.get_store_backend")
    def test_verifies_hash(self, mock_get_backend):
        """Test that hash is verified on retrieval."""
        data = b"original content"
        content_hash = compute_content_hash(data)

        # Return corrupted data
        mock_backend = MagicMock()
        mock_backend.get_buffer.return_value = b"corrupted content"
        mock_get_backend.return_value = mock_backend

        with pytest.raises(DataJointError, match="Content hash mismatch"):
            get_content(content_hash, store_name="test_store")


class TestContentExists:
    """Tests for content_exists function."""

    @patch("datajoint.content_registry.get_store_backend")
    def test_returns_true_when_exists(self, mock_get_backend):
        """Test that True is returned when content exists."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True
        mock_get_backend.return_value = mock_backend

        content_hash = "a" * 64
        assert content_exists(content_hash, store_name="test_store") is True

    @patch("datajoint.content_registry.get_store_backend")
    def test_returns_false_when_not_exists(self, mock_get_backend):
        """Test that False is returned when content doesn't exist."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend

        content_hash = "a" * 64
        assert content_exists(content_hash, store_name="test_store") is False


class TestDeleteContent:
    """Tests for delete_content function."""

    @patch("datajoint.content_registry.get_store_backend")
    def test_deletes_existing_content(self, mock_get_backend):
        """Test deleting existing content."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True
        mock_get_backend.return_value = mock_backend

        content_hash = "a" * 64
        result = delete_content(content_hash, store_name="test_store")

        assert result is True
        mock_backend.remove.assert_called_once()

    @patch("datajoint.content_registry.get_store_backend")
    def test_returns_false_for_nonexistent(self, mock_get_backend):
        """Test that False is returned when content doesn't exist."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend

        content_hash = "a" * 64
        result = delete_content(content_hash, store_name="test_store")

        assert result is False
        mock_backend.remove.assert_not_called()


class TestGetContentSize:
    """Tests for get_content_size function."""

    @patch("datajoint.content_registry.get_store_backend")
    def test_returns_size(self, mock_get_backend):
        """Test getting content size."""
        mock_backend = MagicMock()
        mock_backend.size.return_value = 1024
        mock_get_backend.return_value = mock_backend

        content_hash = "a" * 64
        result = get_content_size(content_hash, store_name="test_store")

        assert result == 1024
