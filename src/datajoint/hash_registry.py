"""
Hash-addressed storage registry for DataJoint.

This module provides hash-addressed storage with deduplication for the ``<hash>``
codec. Content is identified by a Base32-encoded MD5 hash and stored with
per-schema isolation::

    _hash/{schema}/{hash}

With optional subfolding (configured per-store)::

    _hash/{schema}/{fold1}/{fold2}/{hash}

Subfolding creates directory hierarchies to improve performance on filesystems
that struggle with large directories (ext3, FAT32, NFS). Modern filesystems
(ext4, XFS, ZFS, S3) handle flat directories efficiently.

**Storage Model:**

- **Hash** is used for content identification (deduplication, integrity verification)
- **Path** is always stored in metadata and used for all file operations

This design protects against configuration changes (e.g., subfolding) affecting
existing data. The path stored at insert time is always used for retrieval.

Hash-addressed storage is used by ``<hash@>``, ``<blob@>``, and ``<attach@>`` types.
Deduplication occurs within each schema. Deletion requires garbage collection
via ``dj.gc.collect()``.

See Also
--------
datajoint.gc : Garbage collection for orphaned storage items.
"""

import base64
import hashlib
import logging
from typing import Any

from .errors import DataJointError
from .settings import config
from .storage import StorageBackend

logger = logging.getLogger(__name__.split(".")[0])


def compute_hash(data: bytes) -> str:
    """
    Compute Base32-encoded MD5 hash of content.

    Parameters
    ----------
    data : bytes
        Content bytes.

    Returns
    -------
    str
        Base32-encoded hash (26 lowercase characters, no padding).
    """
    md5_digest = hashlib.md5(data).digest()
    # Base32 encode, remove padding, lowercase for filesystem compatibility
    return base64.b32encode(md5_digest).decode("ascii").rstrip("=").lower()


def _subfold(name: str, folds: tuple[int, ...]) -> tuple[str, ...]:
    """
    Create subfolding hierarchy from a hash string.

    Parameters
    ----------
    name : str
        Hash string to subfold.
    folds : tuple[int, ...]
        Lengths of each subfolder level.

    Returns
    -------
    tuple[str, ...]
        Subfolder names.

    Examples
    --------
    >>> _subfold("abcdefgh", (2, 3))
    ('ab', 'cde')
    """
    if not folds:
        return ()
    return (name[: folds[0]],) + _subfold(name[folds[0] :], folds[1:])


def build_hash_path(
    content_hash: str,
    schema_name: str,
    subfolding: tuple[int, ...] | None = None,
) -> str:
    """
    Build the storage path for hash-addressed storage.

    Path structure without subfolding::

        _hash/{schema}/{hash}

    Path structure with subfolding (e.g., (2, 2))::

        _hash/{schema}/{fold1}/{fold2}/{hash}

    Parameters
    ----------
    content_hash : str
        Base32-encoded hash (26 characters).
    schema_name : str
        Database/schema name for isolation.
    subfolding : tuple[int, ...], optional
        Subfolding pattern from store config. None means flat (no subfolding).

    Returns
    -------
    str
        Relative path within the store.
    """
    # Validate hash format (26 base32 chars, lowercase alphanumeric)
    if not (len(content_hash) == 26 and content_hash.isalnum() and content_hash.islower()):
        raise DataJointError(f"Invalid content hash (expected 26-char lowercase base32): {content_hash}")

    if subfolding:
        folds = _subfold(content_hash, subfolding)
        fold_path = "/".join(folds)
        return f"_hash/{schema_name}/{fold_path}/{content_hash}"
    else:
        return f"_hash/{schema_name}/{content_hash}"


def get_store_backend(store_name: str | None = None) -> StorageBackend:
    """
    Get a StorageBackend for hash-addressed storage.

    Parameters
    ----------
    store_name : str, optional
        Name of the store to use. If None, uses stores.default.

    Returns
    -------
    StorageBackend
        StorageBackend instance.
    """
    # get_store_spec handles None by using stores.default
    spec = config.get_store_spec(store_name)
    return StorageBackend(spec)


def get_store_subfolding(store_name: str | None = None) -> tuple[int, ...] | None:
    """
    Get the subfolding configuration for a store.

    Parameters
    ----------
    store_name : str, optional
        Name of the store. If None, uses stores.default.

    Returns
    -------
    tuple[int, ...] | None
        Subfolding pattern (e.g., (2, 2)) or None for flat storage.
    """
    spec = config.get_store_spec(store_name)
    subfolding = spec.get("subfolding")
    if subfolding is not None:
        return tuple(subfolding)
    return None


def put_hash(
    data: bytes,
    schema_name: str,
    store_name: str | None = None,
) -> dict[str, Any]:
    """
    Store content using hash-addressed storage.

    If the content already exists (same hash in same schema), it is not
    re-uploaded. Returns metadata including the hash, path, store, and size.

    The path is always stored in metadata and used for retrieval, protecting
    against configuration changes (e.g., subfolding) affecting existing data.

    Parameters
    ----------
    data : bytes
        Content bytes to store.
    schema_name : str
        Database/schema name for path isolation.
    store_name : str, optional
        Name of the store. If None, uses default store.

    Returns
    -------
    dict[str, Any]
        Metadata dict with keys: hash, path, schema, store, size.
    """
    content_hash = compute_hash(data)
    subfolding = get_store_subfolding(store_name)
    path = build_hash_path(content_hash, schema_name, subfolding)

    backend = get_store_backend(store_name)

    # Check if content already exists (deduplication within schema)
    if not backend.exists(path):
        backend.put_buffer(data, path)
        logger.debug(f"Stored new hash: {content_hash} ({len(data)} bytes)")
    else:
        logger.debug(f"Hash already exists: {content_hash}")

    return {
        "hash": content_hash,
        "path": path,  # Always stored for retrieval
        "schema": schema_name,
        "store": store_name,
        "size": len(data),
    }


def get_hash(metadata: dict[str, Any]) -> bytes:
    """
    Retrieve content using stored metadata.

    Uses the stored path directly (not derived from hash) to protect against
    configuration changes affecting existing data.

    Parameters
    ----------
    metadata : dict
        Metadata dict with keys: path, hash, store (optional).

    Returns
    -------
    bytes
        Content bytes.

    Raises
    ------
    MissingExternalFile
        If content is not found at the stored path.
    DataJointError
        If hash verification fails (data corruption).
    """
    path = metadata["path"]
    expected_hash = metadata["hash"]
    store_name = metadata.get("store")

    backend = get_store_backend(store_name)
    data = backend.get_buffer(path)

    # Verify hash for integrity
    actual_hash = compute_hash(data)
    if actual_hash != expected_hash:
        raise DataJointError(
            f"Hash mismatch: expected {expected_hash}, got {actual_hash}. " f"Data at {path} may be corrupted."
        )

    return data


def delete_path(
    path: str,
    store_name: str | None = None,
) -> bool:
    """
    Delete content at the specified path from storage.

    This should only be called after verifying no references exist.
    Use garbage collection to safely remove unreferenced content.

    Parameters
    ----------
    path : str
        Storage path (as stored in metadata).
    store_name : str, optional
        Name of the store. If None, uses default store.

    Returns
    -------
    bool
        True if content was deleted, False if it didn't exist.

    Warnings
    --------
    This permanently deletes content. Ensure no references exist first.
    """
    backend = get_store_backend(store_name)

    if backend.exists(path):
        backend.remove(path)
        logger.debug(f"Deleted: {path}")
        return True
    return False
