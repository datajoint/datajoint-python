"""
Garbage collection for external storage.

This module provides utilities to identify and remove orphaned content
from external storage. Content becomes orphaned when all database rows
referencing it are deleted.

Supports two storage patterns:
- Content-addressed storage: <hash@>, <blob@>, <attach@>
  Stored at: _content/{hash[:2]}/{hash[2:4]}/{hash}

- Path-addressed storage: <object@>
  Stored at: {schema}/{table}/objects/{pk}/{field}_{token}/

Usage:
    import datajoint as dj

    # Scan schemas and find orphaned content
    stats = dj.gc.scan(schema1, schema2, store_name='mystore')

    # Remove orphaned content (dry_run=False to actually delete)
    stats = dj.gc.collect(schema1, schema2, store_name='mystore', dry_run=True)
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from .content_registry import delete_content, get_store_backend
from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import Schema

logger = logging.getLogger(__name__.split(".")[0])


def _uses_content_storage(attr) -> bool:
    """
    Check if an attribute uses content-addressed storage.

    This includes types that chain to <hash> for external storage:
    - <hash@store> directly
    - <blob@store> (chains to <hash>)
    - <attach@store> (chains to <hash>)

    Args:
        attr: Attribute from table heading

    Returns:
        True if the attribute stores content hashes
    """
    if not attr.codec:
        return False

    # Check if this type uses content storage
    codec_name = getattr(attr.codec, "name", "")
    store = getattr(attr, "store", None)

    # <hash> always uses content storage (external only)
    if codec_name == "hash":
        return True

    # <blob@> and <attach@> use content storage when external (has store)
    if codec_name in ("blob", "attach") and store is not None:
        return True

    return False


def _uses_object_storage(attr) -> bool:
    """
    Check if an attribute uses path-addressed object storage.

    Args:
        attr: Attribute from table heading

    Returns:
        True if the attribute stores object paths
    """
    if not attr.codec:
        return False

    codec_name = getattr(attr.codec, "name", "")
    return codec_name == "object"


def _extract_content_refs(value: Any) -> list[tuple[str, str | None]]:
    """
    Extract content references from a stored value.

    Args:
        value: The stored value (could be JSON string or dict)

    Returns:
        List of (content_hash, store_name) tuples
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

    # Extract hash from dict
    if isinstance(value, dict) and "hash" in value:
        refs.append((value["hash"], value.get("store")))

    return refs


def _extract_object_refs(value: Any) -> list[tuple[str, str | None]]:
    """
    Extract object path references from a stored value.

    Args:
        value: The stored value (could be JSON string or dict)

    Returns:
        List of (path, store_name) tuples
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


def scan_references(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> set[str]:
    """
    Scan schemas for content references.

    Examines all tables in the given schemas and extracts content hashes
    from columns that use content-addressed storage (<hash@>, <blob@>, <attach@>).

    Args:
        *schemas: Schema instances to scan
        store_name: Only include references to this store (None = all stores)
        verbose: Print progress information

    Returns:
        Set of content hashes that are referenced
    """
    referenced: set[str] = set()

    for schema in schemas:
        if verbose:
            logger.info(f"Scanning schema: {schema.database}")

        # Get all tables in schema
        for table_name in schema.list_tables():
            try:
                # Get table class
                table = schema.spawn_table(table_name)

                # Check each attribute for content storage
                for attr_name, attr in table.heading.attributes.items():
                    if not _uses_content_storage(attr):
                        continue

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Fetch all values for this attribute
                    # Use to_arrays to get attribute values
                    try:
                        values = table.to_arrays(attr_name)
                        for value in values:
                            for content_hash, ref_store in _extract_content_refs(value):
                                # Filter by store if specified
                                if store_name is None or ref_store == store_name:
                                    referenced.add(content_hash)
                    except Exception as e:
                        logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")

            except Exception as e:
                logger.warning(f"Error accessing table {table_name}: {e}")

    return referenced


def scan_object_references(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> set[str]:
    """
    Scan schemas for object path references.

    Examines all tables in the given schemas and extracts object paths
    from columns that use path-addressed storage (<object>).

    Args:
        *schemas: Schema instances to scan
        store_name: Only include references to this store (None = all stores)
        verbose: Print progress information

    Returns:
        Set of object paths that are referenced
    """
    referenced: set[str] = set()

    for schema in schemas:
        if verbose:
            logger.info(f"Scanning schema for objects: {schema.database}")

        # Get all tables in schema
        for table_name in schema.list_tables():
            try:
                # Get table class
                table = schema.spawn_table(table_name)

                # Check each attribute for object storage
                for attr_name, attr in table.heading.attributes.items():
                    if not _uses_object_storage(attr):
                        continue

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Fetch all values for this attribute
                    try:
                        values = table.to_arrays(attr_name)
                        for value in values:
                            for path, ref_store in _extract_object_refs(value):
                                # Filter by store if specified
                                if store_name is None or ref_store == store_name:
                                    referenced.add(path)
                    except Exception as e:
                        logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")

            except Exception as e:
                logger.warning(f"Error accessing table {table_name}: {e}")

    return referenced


def list_stored_content(store_name: str | None = None) -> dict[str, int]:
    """
    List all content hashes in storage.

    Scans the _content/ directory in the specified store and returns
    all content hashes found.

    Args:
        store_name: Store to scan (None = default store)

    Returns:
        Dict mapping content_hash to size in bytes
    """
    backend = get_store_backend(store_name)
    stored: dict[str, int] = {}

    # Content is stored at _content/{hash[:2]}/{hash[2:4]}/{hash}
    content_prefix = "_content/"

    try:
        # List all files under _content/
        full_prefix = backend._full_path(content_prefix)

        for root, dirs, files in backend.fs.walk(full_prefix):
            for filename in files:
                # Skip manifest files
                if filename.endswith(".manifest.json"):
                    continue

                # The filename is the full hash
                content_hash = filename

                # Validate it looks like a hash (64 hex chars)
                if len(content_hash) == 64 and all(c in "0123456789abcdef" for c in content_hash):
                    try:
                        file_path = f"{root}/{filename}"
                        size = backend.fs.size(file_path)
                        stored[content_hash] = size
                    except Exception:
                        stored[content_hash] = 0

    except FileNotFoundError:
        # No _content/ directory exists yet
        pass
    except Exception as e:
        logger.warning(f"Error listing stored content: {e}")

    return stored


def list_stored_objects(store_name: str | None = None) -> dict[str, int]:
    """
    List all object paths in storage.

    Scans for directories matching the object storage pattern:
    {schema}/{table}/objects/{pk}/{field}_{token}/

    Args:
        store_name: Store to scan (None = default store)

    Returns:
        Dict mapping object_path to size in bytes
    """
    backend = get_store_backend(store_name)
    stored: dict[str, int] = {}

    try:
        # Walk the storage looking for /objects/ directories
        full_prefix = backend._full_path("")

        for root, dirs, files in backend.fs.walk(full_prefix):
            # Skip _content directory
            if "_content" in root:
                continue

            # Look for "objects" directory pattern
            if "/objects/" in root:
                # This could be an object storage path
                # Path pattern: {schema}/{table}/objects/{pk}/{field}_{token}
                relative_path = root.replace(full_prefix, "").lstrip("/")

                # Calculate total size of this object directory
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
        logger.warning(f"Error listing stored objects: {e}")

    return stored


def delete_object(path: str, store_name: str | None = None) -> bool:
    """
    Delete an object directory from storage.

    Args:
        path: Object path (relative to store root)
        store_name: Store name (None = default store)

    Returns:
        True if deleted, False if not found
    """
    backend = get_store_backend(store_name)

    try:
        full_path = backend._full_path(path)
        if backend.fs.exists(full_path):
            # Remove entire directory tree
            backend.fs.rm(full_path, recursive=True)
            logger.debug(f"Deleted object: {path}")
            return True
    except Exception as e:
        logger.warning(f"Error deleting object {path}: {e}")

    return False


def scan(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Scan for orphaned content and objects without deleting.

    Scans both content-addressed storage (for <hash@>, <blob@>, <attach@>)
    and path-addressed storage (for <object>).

    Args:
        *schemas: Schema instances to scan
        store_name: Store to check (None = default store)
        verbose: Print progress information

    Returns:
        Dict with scan statistics:
        - content_referenced: Number of content items referenced in database
        - content_stored: Number of content items in storage
        - content_orphaned: Number of unreferenced content items
        - content_orphaned_bytes: Total size of orphaned content
        - orphaned_hashes: List of orphaned content hashes
        - object_referenced: Number of objects referenced in database
        - object_stored: Number of objects in storage
        - object_orphaned: Number of unreferenced objects
        - object_orphaned_bytes: Total size of orphaned objects
        - orphaned_paths: List of orphaned object paths
    """
    if not schemas:
        raise DataJointError("At least one schema must be provided")

    # --- Content-addressed storage ---
    content_referenced = scan_references(*schemas, store_name=store_name, verbose=verbose)
    content_stored = list_stored_content(store_name)
    orphaned_hashes = set(content_stored.keys()) - content_referenced
    content_orphaned_bytes = sum(content_stored.get(h, 0) for h in orphaned_hashes)

    # --- Path-addressed storage (objects) ---
    object_referenced = scan_object_references(*schemas, store_name=store_name, verbose=verbose)
    object_stored = list_stored_objects(store_name)
    orphaned_paths = set(object_stored.keys()) - object_referenced
    object_orphaned_bytes = sum(object_stored.get(p, 0) for p in orphaned_paths)

    return {
        # Content-addressed storage stats
        "content_referenced": len(content_referenced),
        "content_stored": len(content_stored),
        "content_orphaned": len(orphaned_hashes),
        "content_orphaned_bytes": content_orphaned_bytes,
        "orphaned_hashes": sorted(orphaned_hashes),
        # Path-addressed storage stats
        "object_referenced": len(object_referenced),
        "object_stored": len(object_stored),
        "object_orphaned": len(orphaned_paths),
        "object_orphaned_bytes": object_orphaned_bytes,
        "orphaned_paths": sorted(orphaned_paths),
        # Combined totals
        "referenced": len(content_referenced) + len(object_referenced),
        "stored": len(content_stored) + len(object_stored),
        "orphaned": len(orphaned_hashes) + len(orphaned_paths),
        "orphaned_bytes": content_orphaned_bytes + object_orphaned_bytes,
    }


def collect(
    *schemas: "Schema",
    store_name: str | None = None,
    dry_run: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Remove orphaned content and objects from storage.

    Scans the given schemas for content and object references, then removes any
    storage items that are not referenced.

    Args:
        *schemas: Schema instances to scan
        store_name: Store to clean (None = default store)
        dry_run: If True, report what would be deleted without deleting
        verbose: Print progress information

    Returns:
        Dict with collection statistics:
        - referenced: Total items referenced in database
        - stored: Total items in storage
        - orphaned: Total unreferenced items
        - content_deleted: Number of content items deleted
        - object_deleted: Number of object items deleted
        - deleted: Total items deleted (0 if dry_run)
        - bytes_freed: Bytes freed (0 if dry_run)
        - errors: Number of deletion errors
    """
    # First scan to find orphaned content and objects
    stats = scan(*schemas, store_name=store_name, verbose=verbose)

    content_deleted = 0
    object_deleted = 0
    bytes_freed = 0
    errors = 0

    if not dry_run:
        # Delete orphaned content (hash-addressed)
        if stats["content_orphaned"] > 0:
            content_stored = list_stored_content(store_name)

            for content_hash in stats["orphaned_hashes"]:
                try:
                    size = content_stored.get(content_hash, 0)
                    if delete_content(content_hash, store_name):
                        content_deleted += 1
                        bytes_freed += size
                        if verbose:
                            logger.info(f"Deleted content: {content_hash[:16]}... ({size} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete content {content_hash[:16]}...: {e}")

        # Delete orphaned objects (path-addressed)
        if stats["object_orphaned"] > 0:
            object_stored = list_stored_objects(store_name)

            for path in stats["orphaned_paths"]:
                try:
                    size = object_stored.get(path, 0)
                    if delete_object(path, store_name):
                        object_deleted += 1
                        bytes_freed += size
                        if verbose:
                            logger.info(f"Deleted object: {path} ({size} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete object {path}: {e}")

    return {
        "referenced": stats["referenced"],
        "stored": stats["stored"],
        "orphaned": stats["orphaned"],
        "content_deleted": content_deleted,
        "object_deleted": object_deleted,
        "deleted": content_deleted + object_deleted,
        "bytes_freed": bytes_freed,
        "errors": errors,
        "dry_run": dry_run,
        # Include detailed stats
        "content_orphaned": stats["content_orphaned"],
        "object_orphaned": stats["object_orphaned"],
    }


def format_stats(stats: dict[str, Any]) -> str:
    """
    Format GC statistics as a human-readable string.

    Args:
        stats: Statistics dict from scan() or collect()

    Returns:
        Formatted string
    """
    lines = ["External Storage Statistics:"]

    # Show content-addressed storage stats if present
    if "content_referenced" in stats:
        lines.append("")
        lines.append("Content-Addressed Storage (<hash@>, <blob@>, <attach@>):")
        lines.append(f"  Referenced: {stats['content_referenced']}")
        lines.append(f"  Stored:     {stats['content_stored']}")
        lines.append(f"  Orphaned:   {stats['content_orphaned']}")
        if "content_orphaned_bytes" in stats:
            size_mb = stats["content_orphaned_bytes"] / (1024 * 1024)
            lines.append(f"  Orphaned size: {size_mb:.2f} MB")

    # Show path-addressed storage stats if present
    if "object_referenced" in stats:
        lines.append("")
        lines.append("Path-Addressed Storage (<object>):")
        lines.append(f"  Referenced: {stats['object_referenced']}")
        lines.append(f"  Stored:     {stats['object_stored']}")
        lines.append(f"  Orphaned:   {stats['object_orphaned']}")
        if "object_orphaned_bytes" in stats:
            size_mb = stats["object_orphaned_bytes"] / (1024 * 1024)
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
            if "content_deleted" in stats:
                lines.append(f"    Content: {stats['content_deleted']}")
            if "object_deleted" in stats:
                lines.append(f"    Objects: {stats['object_deleted']}")
            freed_mb = stats["bytes_freed"] / (1024 * 1024)
            lines.append(f"  Bytes freed: {freed_mb:.2f} MB")
            if stats.get("errors", 0) > 0:
                lines.append(f"  Errors:      {stats['errors']}")

    return "\n".join(lines)
