"""
Content-addressed storage registry for DataJoint.

This module provides content-addressed storage with deduplication for the <content>
AttributeType. Content is identified by its SHA256 hash and stored in a hierarchical
directory structure: _content/{hash[:2]}/{hash[2:4]}/{hash}

The ContentRegistry tracks stored content for garbage collection purposes.
"""

import hashlib
import logging
from typing import Any

from .errors import DataJointError
from .settings import config
from .storage import StorageBackend

logger = logging.getLogger(__name__.split(".")[0])


def compute_content_hash(data: bytes) -> str:
    """
    Compute SHA256 hash of content.

    Args:
        data: Content bytes

    Returns:
        Hex-encoded SHA256 hash (64 characters)
    """
    return hashlib.sha256(data).hexdigest()


def build_content_path(content_hash: str) -> str:
    """
    Build the storage path for content-addressed storage.

    Content is stored in a hierarchical structure to avoid too many files
    in a single directory: _content/{hash[:2]}/{hash[2:4]}/{hash}

    Args:
        content_hash: SHA256 hex hash (64 characters)

    Returns:
        Relative path within the store
    """
    if len(content_hash) != 64:
        raise DataJointError(f"Invalid content hash length: {len(content_hash)} (expected 64)")
    return f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"


def get_store_backend(store_name: str | None = None) -> StorageBackend:
    """
    Get a StorageBackend for content storage.

    Args:
        store_name: Name of the store to use. If None, uses the default object storage
            configuration or the configured default_store.

    Returns:
        StorageBackend instance
    """
    # If store_name is None, check for configured default_store
    if store_name is None and config.object_storage.default_store:
        store_name = config.object_storage.default_store

    # get_object_store_spec handles None by returning default object_storage config
    spec = config.get_object_store_spec(store_name)
    return StorageBackend(spec)


def put_content(data: bytes, store_name: str | None = None) -> dict[str, Any]:
    """
    Store content using content-addressed storage.

    If the content already exists (same hash), it is not re-uploaded.
    Returns metadata including the hash, store, and size.

    Args:
        data: Content bytes to store
        store_name: Name of the store. If None, uses default store.

    Returns:
        Metadata dict with keys: hash, store, size
    """
    content_hash = compute_content_hash(data)
    path = build_content_path(content_hash)

    backend = get_store_backend(store_name)

    # Check if content already exists (deduplication)
    if not backend.exists(path):
        backend.put_buffer(data, path)
        logger.debug(f"Stored new content: {content_hash[:16]}... ({len(data)} bytes)")
    else:
        logger.debug(f"Content already exists: {content_hash[:16]}...")

    return {
        "hash": content_hash,
        "store": store_name,
        "size": len(data),
    }


def get_content(content_hash: str, store_name: str | None = None) -> bytes:
    """
    Retrieve content by its hash.

    Args:
        content_hash: SHA256 hex hash of the content
        store_name: Name of the store. If None, uses default store.

    Returns:
        Content bytes

    Raises:
        MissingExternalFile: If content is not found
        DataJointError: If hash verification fails
    """
    path = build_content_path(content_hash)
    backend = get_store_backend(store_name)

    data = backend.get_buffer(path)

    # Verify hash (optional but recommended for integrity)
    actual_hash = compute_content_hash(data)
    if actual_hash != content_hash:
        raise DataJointError(f"Content hash mismatch: expected {content_hash[:16]}..., " f"got {actual_hash[:16]}...")

    return data


def content_exists(content_hash: str, store_name: str | None = None) -> bool:
    """
    Check if content exists in storage.

    Args:
        content_hash: SHA256 hex hash of the content
        store_name: Name of the store. If None, uses default store.

    Returns:
        True if content exists
    """
    path = build_content_path(content_hash)
    backend = get_store_backend(store_name)
    return backend.exists(path)


def delete_content(content_hash: str, store_name: str | None = None) -> bool:
    """
    Delete content from storage.

    WARNING: This should only be called after verifying no references exist.
    Use garbage collection to safely remove unreferenced content.

    Args:
        content_hash: SHA256 hex hash of the content
        store_name: Name of the store. If None, uses default store.

    Returns:
        True if content was deleted, False if it didn't exist
    """
    path = build_content_path(content_hash)
    backend = get_store_backend(store_name)

    if backend.exists(path):
        backend.remove(path)
        logger.debug(f"Deleted content: {content_hash[:16]}...")
        return True
    return False


def get_content_size(content_hash: str, store_name: str | None = None) -> int:
    """
    Get the size of stored content.

    Args:
        content_hash: SHA256 hex hash of the content
        store_name: Name of the store. If None, uses default store.

    Returns:
        Size in bytes
    """
    path = build_content_path(content_hash)
    backend = get_store_backend(store_name)
    return backend.size(path)
