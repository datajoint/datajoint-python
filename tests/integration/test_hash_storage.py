"""
Tests for hash-addressed storage (hash_registry.py).
"""

import re
from unittest.mock import MagicMock, patch

import pytest

from datajoint.hash_registry import (
    build_hash_path,
    compute_hash,
    hash_exists,
    delete_path,
    delete_hash,
    get_hash,
    get_size,
    get_hash_size,
    put_hash,
)
from datajoint.errors import DataJointError


# Base32 pattern for validation (26 lowercase alphanumeric chars)
BASE32_PATTERN = re.compile(r"^[a-z2-7]{26}$")


class TestComputeHash:
    """Tests for compute_hash function."""

    def test_returns_base32_format(self):
        """Test that hash is returned as Base32 string."""
        data = b"Hello, World!"
        result = compute_hash(data)

        # Should be valid Base32 format (26 lowercase chars)
        assert len(result) == 26
        assert BASE32_PATTERN.match(result)

    def test_empty_bytes(self):
        """Test hashing empty bytes."""
        result = compute_hash(b"")
        assert BASE32_PATTERN.match(result)

    def test_different_content_different_hash(self):
        """Test that different content produces different hashes."""
        hash1 = compute_hash(b"content1")
        hash2 = compute_hash(b"content2")
        assert hash1 != hash2

    def test_same_content_same_hash(self):
        """Test that same content produces same hash."""
        data = b"identical content"
        hash1 = compute_hash(data)
        hash2 = compute_hash(data)
        assert hash1 == hash2


class TestBuildHashPath:
    """Tests for build_hash_path function."""

    def test_builds_flat_path(self):
        """Test that path is built as _hash/{schema}/{hash}."""
        test_hash = "abcdefghijklmnopqrstuvwxyz"[:26]  # 26 char base32
        result = build_hash_path(test_hash, "my_schema")

        assert result == f"_hash/my_schema/{test_hash}"

    def test_builds_subfolded_path(self):
        """Test path with subfolding."""
        test_hash = "abcdefghijklmnopqrstuvwxyz"[:26]
        result = build_hash_path(test_hash, "my_schema", subfolding=(2, 2))

        assert result == f"_hash/my_schema/ab/cd/{test_hash}"

    def test_rejects_invalid_hash(self):
        """Test that invalid hash raises error."""
        with pytest.raises(DataJointError, match="Invalid content hash"):
            build_hash_path("not-a-hash", "my_schema")

        with pytest.raises(DataJointError, match="Invalid content hash"):
            build_hash_path("a" * 64, "my_schema")  # Too long

        with pytest.raises(DataJointError, match="Invalid content hash"):
            build_hash_path("ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:26], "my_schema")  # Uppercase

    def test_real_hash_path(self):
        """Test path building with a real computed hash."""
        data = b"test content"
        content_hash = compute_hash(data)
        path = build_hash_path(content_hash, "test_schema")

        # Verify structure: _hash/{schema}/{hash}
        parts = path.split("/")
        assert len(parts) == 3
        assert parts[0] == "_hash"
        assert parts[1] == "test_schema"
        assert parts[2] == content_hash
        assert BASE32_PATTERN.match(parts[2])


class TestPutHash:
    """Tests for put_hash function."""

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_stores_new_content(self, mock_get_backend, mock_get_subfolding):
        """Test storing new content."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        data = b"new content"
        result = put_hash(data, schema_name="test_schema", store_name="test_store")

        # Verify return value includes hash and path
        assert "hash" in result
        assert "path" in result
        assert result["hash"] == compute_hash(data)
        assert result["path"] == f"_hash/test_schema/{result['hash']}"
        assert result["schema"] == "test_schema"
        assert result["store"] == "test_store"
        assert result["size"] == len(data)

        # Verify backend was called
        mock_backend.put_buffer.assert_called_once()

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_deduplicates_existing_content(self, mock_get_backend, mock_get_subfolding):
        """Test that existing content is not re-uploaded."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True  # Content already exists
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        data = b"existing content"
        result = put_hash(data, schema_name="test_schema", store_name="test_store")

        # Verify return value is still correct
        assert result["hash"] == compute_hash(data)
        assert "path" in result
        assert result["schema"] == "test_schema"
        assert result["size"] == len(data)

        # Verify put_buffer was NOT called (deduplication)
        mock_backend.put_buffer.assert_not_called()


class TestGetHash:
    """Tests for get_hash function."""

    @patch("datajoint.hash_registry.get_store_backend")
    def test_retrieves_content(self, mock_get_backend):
        """Test retrieving content using metadata."""
        data = b"stored content"
        content_hash = compute_hash(data)

        mock_backend = MagicMock()
        mock_backend.get_buffer.return_value = data
        mock_get_backend.return_value = mock_backend

        metadata = {
            "hash": content_hash,
            "path": f"_hash/test_schema/{content_hash}",
            "store": "test_store",
        }
        result = get_hash(metadata)

        assert result == data
        mock_backend.get_buffer.assert_called_once_with(metadata["path"])

    @patch("datajoint.hash_registry.get_store_backend")
    def test_verifies_hash(self, mock_get_backend):
        """Test that hash is verified on retrieval."""
        data = b"original content"
        content_hash = compute_hash(data)

        # Return corrupted data
        mock_backend = MagicMock()
        mock_backend.get_buffer.return_value = b"corrupted content"
        mock_get_backend.return_value = mock_backend

        metadata = {
            "hash": content_hash,
            "path": f"_hash/test_schema/{content_hash}",
            "store": "test_store",
        }

        with pytest.raises(DataJointError, match="Hash mismatch"):
            get_hash(metadata)


class TestHashExists:
    """Tests for hash_exists function."""

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_returns_true_when_exists(self, mock_get_backend, mock_get_subfolding):
        """Test that True is returned when content exists."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        content_hash = "abcdefghijklmnopqrstuvwxyz"[:26]  # Valid base32
        assert hash_exists(content_hash, schema_name="test_schema", store_name="test_store") is True

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_returns_false_when_not_exists(self, mock_get_backend, mock_get_subfolding):
        """Test that False is returned when content doesn't exist."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        content_hash = "abcdefghijklmnopqrstuvwxyz"[:26]  # Valid base32
        assert hash_exists(content_hash, schema_name="test_schema", store_name="test_store") is False


class TestDeletePath:
    """Tests for delete_path function."""

    @patch("datajoint.hash_registry.get_store_backend")
    def test_deletes_existing_content(self, mock_get_backend):
        """Test deleting existing content by path."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True
        mock_get_backend.return_value = mock_backend

        path = "_hash/test_schema/abcdefghijklmnopqrst"
        result = delete_path(path, store_name="test_store")

        assert result is True
        mock_backend.remove.assert_called_once_with(path)

    @patch("datajoint.hash_registry.get_store_backend")
    def test_returns_false_for_nonexistent(self, mock_get_backend):
        """Test that False is returned when content doesn't exist."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = False
        mock_get_backend.return_value = mock_backend

        path = "_hash/test_schema/abcdefghijklmnopqrst"
        result = delete_path(path, store_name="test_store")

        assert result is False
        mock_backend.remove.assert_not_called()


class TestDeleteHash:
    """Tests for delete_hash function (backward compatibility)."""

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_deletes_existing_content(self, mock_get_backend, mock_get_subfolding):
        """Test deleting existing content by hash."""
        mock_backend = MagicMock()
        mock_backend.exists.return_value = True
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        content_hash = "abcdefghijklmnopqrstuvwxyz"[:26]  # Valid base32
        result = delete_hash(content_hash, schema_name="test_schema", store_name="test_store")

        assert result is True
        mock_backend.remove.assert_called_once()


class TestGetSize:
    """Tests for get_size function."""

    @patch("datajoint.hash_registry.get_store_backend")
    def test_returns_size(self, mock_get_backend):
        """Test getting content size by path."""
        mock_backend = MagicMock()
        mock_backend.size.return_value = 1024
        mock_get_backend.return_value = mock_backend

        path = "_hash/test_schema/abcdefghijklmnopqrst"
        result = get_size(path, store_name="test_store")

        assert result == 1024
        mock_backend.size.assert_called_once_with(path)


class TestGetHashSize:
    """Tests for get_hash_size function (backward compatibility)."""

    @patch("datajoint.hash_registry.get_store_subfolding")
    @patch("datajoint.hash_registry.get_store_backend")
    def test_returns_size(self, mock_get_backend, mock_get_subfolding):
        """Test getting content size by hash."""
        mock_backend = MagicMock()
        mock_backend.size.return_value = 1024
        mock_get_backend.return_value = mock_backend
        mock_get_subfolding.return_value = None

        content_hash = "abcdefghijklmnopqrstuvwxyz"[:26]  # Valid base32
        result = get_hash_size(content_hash, schema_name="test_schema", store_name="test_store")

        assert result == 1024
