"""
Garbage collection for content-addressed storage.

This module provides utilities to identify and remove orphaned content
from external storage. Content becomes orphaned when all database rows
referencing it are deleted.

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

    This includes types that compose with <content>:
    - <content> directly
    - <xblob> (composes with <content>)
    - <xattach> (composes with <content>)

    Args:
        attr: Attribute from table heading

    Returns:
        True if the attribute stores content hashes
    """
    if not attr.adapter:
        return False

    # Check if this type or its composition chain uses content storage
    type_name = getattr(attr.adapter, "type_name", "")
    return type_name in ("content", "xblob", "xattach")


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


def scan_references(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> set[str]:
    """
    Scan schemas for content references.

    Examines all tables in the given schemas and extracts content hashes
    from columns that use content-addressed storage (<content>, <xblob>, <xattach>).

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
                    # Use raw fetch to get JSON strings
                    try:
                        values = table.fetch(attr_name)
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


def scan(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Scan for orphaned content without deleting.

    Args:
        *schemas: Schema instances to scan
        store_name: Store to check (None = default store)
        verbose: Print progress information

    Returns:
        Dict with scan statistics:
        - referenced: Number of content items referenced in database
        - stored: Number of content items in storage
        - orphaned: Number of unreferenced content items
        - orphaned_bytes: Total size of orphaned content
        - orphaned_hashes: List of orphaned content hashes
    """
    if not schemas:
        raise DataJointError("At least one schema must be provided")

    # Find all referenced content
    referenced = scan_references(*schemas, store_name=store_name, verbose=verbose)

    # Find all stored content
    stored = list_stored_content(store_name)

    # Find orphaned content
    orphaned_hashes = set(stored.keys()) - referenced
    orphaned_bytes = sum(stored.get(h, 0) for h in orphaned_hashes)

    return {
        "referenced": len(referenced),
        "stored": len(stored),
        "orphaned": len(orphaned_hashes),
        "orphaned_bytes": orphaned_bytes,
        "orphaned_hashes": sorted(orphaned_hashes),
    }


def collect(
    *schemas: "Schema",
    store_name: str | None = None,
    dry_run: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Remove orphaned content from storage.

    Scans the given schemas for content references, then removes any
    content in storage that is not referenced.

    Args:
        *schemas: Schema instances to scan
        store_name: Store to clean (None = default store)
        dry_run: If True, report what would be deleted without deleting
        verbose: Print progress information

    Returns:
        Dict with collection statistics:
        - referenced: Number of content items referenced in database
        - stored: Number of content items in storage
        - orphaned: Number of unreferenced content items
        - deleted: Number of items deleted (0 if dry_run)
        - bytes_freed: Bytes freed (0 if dry_run)
        - errors: Number of deletion errors
    """
    # First scan to find orphaned content
    stats = scan(*schemas, store_name=store_name, verbose=verbose)

    deleted = 0
    bytes_freed = 0
    errors = 0

    if not dry_run and stats["orphaned"] > 0:
        stored = list_stored_content(store_name)

        for content_hash in stats["orphaned_hashes"]:
            try:
                size = stored.get(content_hash, 0)
                if delete_content(content_hash, store_name):
                    deleted += 1
                    bytes_freed += size
                    if verbose:
                        logger.info(f"Deleted: {content_hash[:16]}... ({size} bytes)")
            except Exception as e:
                errors += 1
                logger.warning(f"Failed to delete {content_hash[:16]}...: {e}")

    return {
        "referenced": stats["referenced"],
        "stored": stats["stored"],
        "orphaned": stats["orphaned"],
        "deleted": deleted,
        "bytes_freed": bytes_freed,
        "errors": errors,
        "dry_run": dry_run,
    }


def format_stats(stats: dict[str, Any]) -> str:
    """
    Format GC statistics as a human-readable string.

    Args:
        stats: Statistics dict from scan() or collect()

    Returns:
        Formatted string
    """
    lines = [
        "Content Storage Statistics:",
        f"  Referenced in database: {stats['referenced']}",
        f"  Stored in backend:      {stats['stored']}",
        f"  Orphaned (unreferenced): {stats['orphaned']}",
    ]

    if "orphaned_bytes" in stats:
        size_mb = stats["orphaned_bytes"] / (1024 * 1024)
        lines.append(f"  Orphaned size:          {size_mb:.2f} MB")

    if "deleted" in stats:
        lines.append("")
        if stats.get("dry_run", True):
            lines.append("  [DRY RUN - no changes made]")
        else:
            lines.append(f"  Deleted:     {stats['deleted']}")
            freed_mb = stats["bytes_freed"] / (1024 * 1024)
            lines.append(f"  Bytes freed: {freed_mb:.2f} MB")
            if stats.get("errors", 0) > 0:
                lines.append(f"  Errors:      {stats['errors']}")

    return "\n".join(lines)
