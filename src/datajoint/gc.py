"""
Garbage collection for object storage.

This module provides utilities to identify and remove orphaned items
from object storage. Storage items become orphaned when all database rows
referencing them are deleted.

DataJoint uses two object storage patterns:

Hash-addressed storage
    Types: ``<hash@>``, ``<blob@>``, ``<attach@>``
    Path: ``_hash/{schema}/{hash}`` (with optional subfolding)
    Deduplication: Per-schema (identical data within a schema shares storage)
    Deletion: Requires garbage collection

Schema-addressed storage
    Types: ``<object@>``, ``<npy@>``
    Path: ``{schema}/{table}/{pk}/{field}/``
    Deduplication: None (each entity has unique path)
    Deletion: Requires garbage collection

Usage::

    import datajoint as dj

    # Scan schemas and find orphaned items
    stats = dj.gc.scan(schema1, schema2, store_name='mystore')

    # Remove orphaned items (dry_run=False to actually delete)
    stats = dj.gc.collect(schema1, schema2, store_name='mystore', dry_run=True)

See Also
--------
datajoint.builtin_codecs : Codec implementations for object storage types.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from .hash_registry import delete_path, get_store_backend
from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import _Schema as Schema

logger = logging.getLogger(__name__.split(".")[0])


def _uses_hash_storage(attr) -> bool:
    """
    Check if an attribute uses hash-addressed storage.

    Hash-addressed types use content deduplication via MD5/Base32 hashing:

    - ``<hash@store>`` - raw hash storage
    - ``<blob@store>`` - chains to ``<hash>``
    - ``<attach@store>`` - chains to ``<hash>``

    Parameters
    ----------
    attr : Attribute
        Attribute from table heading.

    Returns
    -------
    bool
        True if the attribute uses hash-addressed storage.
    """
    if not attr.codec:
        return False

    codec_name = getattr(attr.codec, "name", "")
    store = getattr(attr, "store", None)

    # <hash> always uses hash-addressed storage (external only)
    if codec_name == "hash":
        return True

    # <blob@> and <attach@> use hash-addressed storage when external
    if codec_name in ("blob", "attach") and store is not None:
        return True

    return False


def _uses_schema_storage(attr) -> bool:
    """
    Check if an attribute uses schema-addressed storage.

    Schema-addressed types store data at paths derived from the schema structure:

    - ``<object@store>`` - arbitrary objects (pickled or native formats)
    - ``<npy@store>`` - NumPy arrays with lazy loading

    Parameters
    ----------
    attr : Attribute
        Attribute from table heading.

    Returns
    -------
    bool
        True if the attribute uses schema-addressed storage.
    """
    if not attr.codec:
        return False

    codec_name = getattr(attr.codec, "name", "")
    return codec_name in ("object", "npy")


def _extract_hash_refs(value: Any) -> list[tuple[str, str | None]]:
    """
    Extract path references from hash-addressed storage metadata.

    Hash-addressed storage stores metadata as JSON with ``path`` and ``hash`` keys.
    The path is used for file operations; the hash is for integrity verification.

    Parameters
    ----------
    value : Any
        The stored value (JSON string or dict).

    Returns
    -------
    list[tuple[str, str | None]]
        List of (path, store_name) tuples.
    """
    refs = []

    if value is None:
        return refs

    # Parse JSON if string
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return refs

    # Extract path from dict (path is required for new data, hash for legacy)
    if isinstance(value, dict) and "path" in value:
        refs.append((value["path"], value.get("store")))

    return refs


def _extract_schema_refs(value: Any) -> list[tuple[str, str | None]]:
    """
    Extract schema-addressed path references from a stored value.

    Schema-addressed storage stores metadata as JSON with a ``path`` key.

    Parameters
    ----------
    value : Any
        The stored value (JSON string or dict).

    Returns
    -------
    list[tuple[str, str | None]]
        List of (path, store_name) tuples.
    """
    refs = []

    if value is None:
        return refs

    # Parse JSON if string
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return refs

    # Extract path from dict
    if isinstance(value, dict) and "path" in value:
        refs.append((value["path"], value.get("store")))

    return refs


def scan_hash_references(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> set[str]:
    """
    Scan schemas for hash-addressed storage references.

    Examines all tables in the given schemas and extracts storage paths
    from columns that use hash-addressed storage (``<hash@>``, ``<blob@>``,
    ``<attach@>``).

    Parameters
    ----------
    *schemas : Schema
        Schema instances to scan.
    store_name : str, optional
        Only include references to this store (None = all stores).
    verbose : bool, optional
        Print progress information.

    Returns
    -------
    set[str]
        Set of storage paths that are referenced.
    """
    referenced: set[str] = set()

    for schema in schemas:
        if verbose:
            logger.info(f"Scanning schema: {schema.database}")

        # Get all tables in schema
        for table_name in schema.list_tables():
            try:
                # Get table class
                table = schema.get_table(table_name)

                # Check each attribute for hash-addressed storage
                for attr_name, attr in table.heading.attributes.items():
                    if not _uses_hash_storage(attr):
                        continue

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Fetch all values for this attribute
                    try:
                        values = table.to_arrays(attr_name)
                        for value in values:
                            for path, ref_store in _extract_hash_refs(value):
                                # Filter by store if specified
                                if store_name is None or ref_store == store_name:
                                    referenced.add(path)
                    except Exception as e:
                        logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")

            except Exception as e:
                logger.warning(f"Error accessing table {table_name}: {e}")

    return referenced


def scan_schema_references(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> set[str]:
    """
    Scan schemas for schema-addressed storage references.

    Examines all tables in the given schemas and extracts paths from columns
    that use schema-addressed storage (``<object@>``, ``<npy@>``).

    Parameters
    ----------
    *schemas : Schema
        Schema instances to scan.
    store_name : str, optional
        Only include references to this store (None = all stores).
    verbose : bool, optional
        Print progress information.

    Returns
    -------
    set[str]
        Set of storage paths that are referenced.
    """
    referenced: set[str] = set()

    for schema in schemas:
        if verbose:
            logger.info(f"Scanning schema for schema-addressed storage: {schema.database}")

        # Get all tables in schema
        for table_name in schema.list_tables():
            try:
                # Get table class
                table = schema.get_table(table_name)

                # Check each attribute for schema-addressed storage
                for attr_name, attr in table.heading.attributes.items():
                    if not _uses_schema_storage(attr):
                        continue

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Fetch all values for this attribute
                    try:
                        values = table.to_arrays(attr_name)
                        for value in values:
                            for path, ref_store in _extract_schema_refs(value):
                                # Filter by store if specified
                                if store_name is None or ref_store == store_name:
                                    referenced.add(path)
                    except Exception as e:
                        logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")

            except Exception as e:
                logger.warning(f"Error accessing table {table_name}: {e}")

    return referenced


def list_stored_hashes(store_name: str | None = None, config=None) -> dict[str, int]:
    """
    List all hash-addressed items in storage.

    Scans the ``_hash/`` directory in the specified store and returns
    all storage paths found. These correspond to ``<hash@>``, ``<blob@>``,
    and ``<attach@>`` types.

    Parameters
    ----------
    store_name : str, optional
        Store to scan (None = default store).
    config : Config, optional
        Config instance. If None, falls back to global settings.config.

    Returns
    -------
    dict[str, int]
        Dict mapping storage path to size in bytes.
    """
    import re

    backend = get_store_backend(store_name, config=config)
    stored: dict[str, int] = {}

    # Hash-addressed storage: _hash/{schema}/{subfolders...}/{hash}
    hash_prefix = "_hash/"
    # Base32 pattern: 26 lowercase alphanumeric chars
    base32_pattern = re.compile(r"^[a-z2-7]{26}$")

    try:
        full_prefix = backend._full_path(hash_prefix)

        for root, dirs, files in backend.fs.walk(full_prefix):
            for filename in files:
                # Skip manifest files
                if filename.endswith(".manifest.json"):
                    continue

                # The filename is the base32 hash
                content_hash = filename

                # Validate it looks like a base32 hash
                if base32_pattern.match(content_hash):
                    try:
                        file_path = f"{root}/{filename}"
                        size = backend.fs.size(file_path)
                        # Build relative path for comparison with stored metadata
                        # Path format: _hash/{schema}/{subfolders...}/{hash}
                        relative_path = file_path.replace(backend._full_path(""), "").lstrip("/")
                        stored[relative_path] = size
                    except Exception:
                        pass

    except FileNotFoundError:
        # No _hash/ directory exists yet
        pass
    except Exception as e:
        logger.warning(f"Error listing stored hashes: {e}")

    return stored


def list_schema_paths(store_name: str | None = None, config=None) -> dict[str, int]:
    """
    List all schema-addressed items in storage.

    Scans for directories matching the schema-addressed storage pattern:
    ``{schema}/{table}/{pk}/{field}/``

    Parameters
    ----------
    store_name : str, optional
        Store to scan (None = default store).
    config : Config, optional
        Config instance. If None, falls back to global settings.config.

    Returns
    -------
    dict[str, int]
        Dict mapping storage path to size in bytes.
    """
    backend = get_store_backend(store_name, config=config)
    stored: dict[str, int] = {}

    try:
        # Walk the storage looking for schema-addressed paths
        full_prefix = backend._full_path("")

        for root, dirs, files in backend.fs.walk(full_prefix):
            # Skip _hash directory (hash-addressed storage)
            if "_hash" in root:
                continue

            # Look for schema-addressed pattern (has files, not in _hash)
            # Schema-addressed paths: {schema}/{table}/{pk}/{field}/
            relative_path = root.replace(full_prefix, "").lstrip("/")

            # Skip empty paths and root-level directories
            if not relative_path or relative_path.count("/") < 2:
                continue

            # Calculate total size of this directory
            total_size = 0
            for file in files:
                try:
                    file_path = f"{root}/{file}"
                    total_size += backend.fs.size(file_path)
                except Exception:
                    pass

            # Only count directories with files (actual objects)
            if total_size > 0 or files:
                stored[relative_path] = total_size

    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"Error listing stored schemas: {e}")

    return stored


def delete_schema_path(path: str, store_name: str | None = None, config=None) -> bool:
    """
    Delete a schema-addressed directory from storage.

    Parameters
    ----------
    path : str
        Storage path (relative to store root).
    store_name : str, optional
        Store name (None = default store).
    config : Config, optional
        Config instance. If None, falls back to global settings.config.

    Returns
    -------
    bool
        True if deleted, False if not found.
    """
    backend = get_store_backend(store_name, config=config)

    try:
        full_path = backend._full_path(path)
        if backend.fs.exists(full_path):
            # Remove entire directory tree
            backend.fs.rm(full_path, recursive=True)
            logger.debug(f"Deleted schema path: {path}")
            return True
    except Exception as e:
        logger.warning(f"Error deleting schema path {path}: {e}")

    return False


def scan(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Scan for orphaned storage items without deleting.

    Scans both hash-addressed storage (for ``<hash@>``, ``<blob@>``, ``<attach@>``)
    and schema-addressed storage (for ``<object@>``, ``<npy@>``).

    Parameters
    ----------
    *schemas : Schema
        Schema instances to scan.
    store_name : str, optional
        Store to check (None = default store).
    verbose : bool, optional
        Print progress information.

    Returns
    -------
    dict[str, Any]
        Dict with scan statistics:

        - hash_referenced: Number of hash items referenced in database
        - hash_stored: Number of hash items in storage
        - hash_orphaned: Number of unreferenced hash items
        - hash_orphaned_bytes: Total size of orphaned hashes
        - orphaned_hashes: List of orphaned content hashes
        - schema_paths_referenced: Number of schema items referenced in database
        - schema_paths_stored: Number of schema items in storage
        - schema_paths_orphaned: Number of unreferenced schema items
        - schema_paths_orphaned_bytes: Total size of orphaned schema items
        - orphaned_paths: List of orphaned schema paths
    """
    if not schemas:
        raise DataJointError("At least one schema must be provided")

    # Extract config from the first schema's connection
    _config = schemas[0].connection._config if schemas else None

    # --- Hash-addressed storage ---
    hash_referenced = scan_hash_references(*schemas, store_name=store_name, verbose=verbose)
    hash_stored = list_stored_hashes(store_name, config=_config)
    orphaned_hashes = set(hash_stored.keys()) - hash_referenced
    hash_orphaned_bytes = sum(hash_stored.get(h, 0) for h in orphaned_hashes)

    # --- Schema-addressed storage ---
    schema_paths_referenced = scan_schema_references(*schemas, store_name=store_name, verbose=verbose)
    schema_paths_stored = list_schema_paths(store_name, config=_config)
    orphaned_paths = set(schema_paths_stored.keys()) - schema_paths_referenced
    schema_paths_orphaned_bytes = sum(schema_paths_stored.get(p, 0) for p in orphaned_paths)

    return {
        # Hash-addressed storage stats
        "hash_referenced": len(hash_referenced),
        "hash_stored": len(hash_stored),
        "hash_orphaned": len(orphaned_hashes),
        "hash_orphaned_bytes": hash_orphaned_bytes,
        "orphaned_hashes": sorted(orphaned_hashes),
        # Schema-addressed storage stats
        "schema_paths_referenced": len(schema_paths_referenced),
        "schema_paths_stored": len(schema_paths_stored),
        "schema_paths_orphaned": len(orphaned_paths),
        "schema_paths_orphaned_bytes": schema_paths_orphaned_bytes,
        "orphaned_paths": sorted(orphaned_paths),
        # Combined totals
        "referenced": len(hash_referenced) + len(schema_paths_referenced),
        "stored": len(hash_stored) + len(schema_paths_stored),
        "orphaned": len(orphaned_hashes) + len(orphaned_paths),
        "orphaned_bytes": hash_orphaned_bytes + schema_paths_orphaned_bytes,
    }


def collect(
    *schemas: "Schema",
    store_name: str | None = None,
    dry_run: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Remove orphaned storage items.

    Scans the given schemas for storage references, then removes any
    items that are not referenced.

    Parameters
    ----------
    *schemas : Schema
        Schema instances to scan.
    store_name : str, optional
        Store to clean (None = default store).
    dry_run : bool, optional
        If True, report what would be deleted without deleting. Default True.
    verbose : bool, optional
        Print progress information.

    Returns
    -------
    dict[str, Any]
        Dict with collection statistics:

        - referenced: Total items referenced in database
        - stored: Total items in storage
        - orphaned: Total unreferenced items
        - hash_deleted: Number of hash items deleted
        - schema_paths_deleted: Number of schema items deleted
        - deleted: Total items deleted (0 if dry_run)
        - bytes_freed: Bytes freed (0 if dry_run)
        - errors: Number of deletion errors
    """
    # First scan to find orphaned items
    stats = scan(*schemas, store_name=store_name, verbose=verbose)

    # Extract config from the first schema's connection
    _config = schemas[0].connection._config if schemas else None

    hash_deleted = 0
    schema_paths_deleted = 0
    bytes_freed = 0
    errors = 0

    if not dry_run:
        # Delete orphaned hashes
        if stats["hash_orphaned"] > 0:
            hash_stored = list_stored_hashes(store_name, config=_config)

            for path in stats["orphaned_hashes"]:
                try:
                    size = hash_stored.get(path, 0)
                    if delete_path(path, store_name, config=_config):
                        hash_deleted += 1
                        bytes_freed += size
                        if verbose:
                            logger.info(f"Deleted: {path} ({size} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete {path}: {e}")

        # Delete orphaned schema paths
        if stats["schema_paths_orphaned"] > 0:
            schema_paths_stored = list_schema_paths(store_name, config=_config)

            for path in stats["orphaned_paths"]:
                try:
                    size = schema_paths_stored.get(path, 0)
                    if delete_schema_path(path, store_name, config=_config):
                        schema_paths_deleted += 1
                        bytes_freed += size
                        if verbose:
                            logger.info(f"Deleted schema path: {path} ({size} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete schema path {path}: {e}")

    return {
        "referenced": stats["referenced"],
        "stored": stats["stored"],
        "orphaned": stats["orphaned"],
        "hash_deleted": hash_deleted,
        "schema_paths_deleted": schema_paths_deleted,
        "deleted": hash_deleted + schema_paths_deleted,
        "bytes_freed": bytes_freed,
        "errors": errors,
        "dry_run": dry_run,
        # Include detailed stats
        "hash_orphaned": stats["hash_orphaned"],
        "schema_paths_orphaned": stats["schema_paths_orphaned"],
    }


def format_stats(stats: dict[str, Any]) -> str:
    """
    Format GC statistics as a human-readable string.

    Parameters
    ----------
    stats : dict[str, Any]
        Statistics dict from scan() or collect().

    Returns
    -------
    str
        Formatted string.
    """
    lines = ["Object Storage Statistics:"]

    # Show hash-addressed storage stats if present
    if "hash_referenced" in stats:
        lines.append("")
        lines.append("Hash-Addressed Storage (<hash@>, <blob@>, <attach@>):")
        lines.append(f"  Referenced: {stats['hash_referenced']}")
        lines.append(f"  Stored:     {stats['hash_stored']}")
        lines.append(f"  Orphaned:   {stats['hash_orphaned']}")
        if "hash_orphaned_bytes" in stats:
            size_mb = stats["hash_orphaned_bytes"] / (1024 * 1024)
            lines.append(f"  Orphaned size: {size_mb:.2f} MB")

    # Show schema-addressed storage stats if present
    if "schema_paths_referenced" in stats:
        lines.append("")
        lines.append("Schema-Addressed Storage (<object@>, <npy@>):")
        lines.append(f"  Referenced: {stats['schema_paths_referenced']}")
        lines.append(f"  Stored:     {stats['schema_paths_stored']}")
        lines.append(f"  Orphaned:   {stats['schema_paths_orphaned']}")
        if "schema_paths_orphaned_bytes" in stats:
            size_mb = stats["schema_paths_orphaned_bytes"] / (1024 * 1024)
            lines.append(f"  Orphaned size: {size_mb:.2f} MB")

    # Show totals
    lines.append("")
    lines.append("Totals:")
    lines.append(f"  Referenced in database: {stats['referenced']}")
    lines.append(f"  Stored in backend:      {stats['stored']}")
    lines.append(f"  Orphaned (unreferenced): {stats['orphaned']}")

    if "orphaned_bytes" in stats:
        size_mb = stats["orphaned_bytes"] / (1024 * 1024)
        lines.append(f"  Orphaned size:          {size_mb:.2f} MB")

    # Show deletion results if this is from collect()
    if "deleted" in stats:
        lines.append("")
        if stats.get("dry_run", True):
            lines.append("  [DRY RUN - no changes made]")
        else:
            lines.append(f"  Deleted:     {stats['deleted']}")
            if "hash_deleted" in stats:
                lines.append(f"    Hash items:   {stats['hash_deleted']}")
            if "schema_paths_deleted" in stats:
                lines.append(f"    Schema paths: {stats['schema_paths_deleted']}")
            freed_mb = stats["bytes_freed"] / (1024 * 1024)
            lines.append(f"  Bytes freed: {freed_mb:.2f} MB")
            if stats.get("errors", 0) > 0:
                lines.append(f"  Errors:      {stats['errors']}")

    return "\n".join(lines)
