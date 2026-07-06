"""
Garbage collection for object storage.

This module provides utilities to identify and remove orphaned items
from object storage. Storage items become orphaned when all database rows
referencing them are deleted.

DataJoint uses two object storage patterns:

Hash-addressed storage
    Types: ``<hash@>``, ``<blob@>``, ``<attach@>``
    Path: ``{hash_prefix}/{schema}/{hash}`` (with optional subfolding;
    ``hash_prefix`` defaults to ``_hash``)
    Deduplication: Per-schema (identical data within a schema shares storage)
    Deletion: Requires garbage collection

Schema-addressed storage
    Types: ``<object@>``, ``<npy@>``
    Path: ``{schema_prefix}/{schema}/{table}/{pk}/...`` (``schema_prefix``
    defaults to ``_schema``; pre-2.3.1 stores hold root-level ``{schema}/...`` paths)
    Deduplication: None (each entity has unique path)
    Deletion: Requires garbage collection

Usage::

    import datajoint as dj

    # Scan schemas and find orphaned items
    stats = dj.gc.scan(schema1, schema2, store_name='mystore')

    # Remove orphaned items (dry_run=False to actually delete)
    stats = dj.gc.collect(schema1, schema2, store_name='mystore', dry_run=True)

Both sections embed the schema name in every path, so garbage collection is
**per-schema**: each schema is scanned against its own subtree only. Orphan
detection is therefore confined to the schemas you pass — any subset of the
schemas sharing a store may be scanned/collected safely, and GC never touches
another schema's objects or user-managed content elsewhere in the store.

See Also
--------
datajoint.builtin_codecs : Codec implementations for object storage types.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .hash_registry import delete_path, get_store_backend
from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import _Schema as Schema

logger = logging.getLogger(__name__.split(".")[0])


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
        Full relative store paths referenced by live rows, exactly as recorded
        in each row's metadata (independent of the store's current prefixes).
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

                # Attributes stored in external hash-addressed storage
                # (classification lives on the heading — Heading.hash_objects).
                for attr_name in table.heading.hash_objects:
                    attr = table.heading.attributes[attr_name]

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Read raw JSON metadata via cursor — bypasses decode_attribute
                    # so we get the stored dict (PostgreSQL/JSONB) or JSON string
                    # (MySQL), not the decoded codec output. The codec's own
                    # referenced_paths() extracts the referenced paths and handles
                    # both shapes (codec-driven discovery, #1469).
                    try:
                        cursor = table.proj(attr_name).cursor(as_dict=True)
                        for row in cursor:
                            for path, ref_store in attr.codec.referenced_paths(row[attr_name]):
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
    that use schema-addressed storage — any ``SchemaCodec`` (built-in
    ``<object@>``, ``<npy@>`` and custom subclasses, recognized by type per
    #1469), not a fixed codec-name list.

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
        Full relative store paths referenced by live rows, exactly as recorded
        in each row's metadata. For a directory-valued object the recorded
        path is the directory prefix, not its individual files.
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

                # Attributes stored in schema-addressed storage
                # (classification lives on the heading — Heading.schema_objects).
                for attr_name in table.heading.schema_objects:
                    attr = table.heading.attributes[attr_name]

                    if verbose:
                        logger.info(f"  Scanning {table_name}.{attr_name}")

                    # Read raw JSON metadata via cursor — bypasses decode_attribute
                    # so we get the stored dict (PostgreSQL/JSONB) or JSON string
                    # (MySQL), not the decoded codec output. The codec's own
                    # referenced_paths() extracts the referenced paths and handles
                    # both shapes (codec-driven discovery, #1469).
                    try:
                        cursor = table.proj(attr_name).cursor(as_dict=True)
                        for row in cursor:
                            for path, ref_store in attr.codec.referenced_paths(row[attr_name]):
                                # Filter by store if specified
                                if store_name is None or ref_store == store_name:
                                    referenced.add(path)
                    except Exception as e:
                        logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")

            except Exception as e:
                logger.warning(f"Error accessing table {table_name}: {e}")

    return referenced


def list_hash_paths(store_name: str | None = None, config=None, *, schema_name: str) -> dict[str, int]:
    """
    List a schema's hash-addressed items in storage.

    Walks exactly one schema's subtree of the configured hash-addressed
    section — ``{hash_prefix}/{schema_name}/`` (``hash_prefix`` default
    ``_hash``) — and returns the stored objects (``<hash@>``, ``<blob@>``,
    ``<attach@>``). Every hash path embeds the schema and deduplication is
    per-schema, so this listing is complete for that schema and independent of
    every other schema on the store.

    Only the CURRENTLY configured prefix is walked: objects written under a
    former prefix are not listed here (they remain readable via their metadata
    paths). ``schema_name`` is required — GC is always per-schema.

    Parameters
    ----------
    store_name : str, optional
        Store to scan (None = default store).
    config : Config, optional
        Config instance. If None, falls back to global settings.config.
    schema_name : str
        Schema whose hash subtree to list (required, keyword-only).

    Returns
    -------
    dict[str, int]
        Dict mapping each object's full relative store path
        (``{hash_prefix}/{schema}/[{subfolders}/]{hash}``) to its size in bytes.
    """
    import re

    backend = get_store_backend(store_name, config=config)
    stored: dict[str, int] = {}

    if config is None:
        from .settings import config as _global_config

        config = _global_config
    # Hash-addressed storage: {hash_prefix}/{schema}/{subfolders...}/{hash}.
    # The prefix comes from the store's settings — the same value the writer
    # uses — so scanner and writer cannot drift.
    _spec = config.get_store_spec(store_name)
    hash_prefix = _spec["hash_prefix"].strip("/") + "/"  # settings applies the "_hash" default
    section = f"{hash_prefix}{schema_name}/"
    # Base32 pattern: 26 lowercase alphanumeric chars
    base32_pattern = re.compile(r"^[a-z2-7]{26}$")

    try:
        full_prefix = backend._full_path(section)

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
                        # Path format: {hash_prefix}/{schema}/{subfolders...}/{hash}
                        relative_path = file_path.replace(backend._full_path(""), "").lstrip("/")
                        stored[relative_path] = size
                    except Exception:
                        pass

    except FileNotFoundError:
        # The hash section does not exist yet
        pass
    except Exception as e:
        logger.warning(f"Error listing stored hashes: {e}")

    return stored


def list_schema_paths(store_name: str | None = None, config=None, *, schema_name: str) -> dict[str, int]:
    """
    List a schema's schema-addressed object files in storage.

    Walks exactly one schema's section — ``{schema_prefix}/{schema_name}/``
    (``schema_prefix`` default ``_schema``) — and enumerates the individual
    **files** written by schema-addressed codecs. A single-file object is one
    file at ``{schema_prefix}/{schema}/{table}/{pk}/{field}_{token}[.ext]``; a
    directory-valued object (e.g. a Zarr store) is many files under that path
    prefix, plus a ``{path}.manifest.json`` sidecar beside it — all are listed.
    Orphan detection matches these files against each row's referenced object
    path via :func:`_is_covered` (exact, ancestor-prefix, or manifest-sidecar),
    so superseded per-token versions are reclaimable while every file belonging
    to a live object — including its manifest — is kept.

    The walk is confined to the schema's own section, so it never enters the
    hash section or the filepath section (mutually-exclusive top-level
    prefixes, enforced by store validation) — GC's blast radius is exactly
    ``{schema_prefix}/{schema}/``, and user-managed ``<filepath@>`` content and
    other schemas' objects are structurally out of reach.

    ``schema_name`` is required — GC is always per-schema.

    Legacy layout: DataJoint 2.3.0 and earlier wrote schema-addressed objects
    at root level ``{schema}/...`` (``schema_prefix`` was ignored — see
    datajoint/datajoint-python#1487). Such a store should set
    ``schema_prefix=""`` so both writes and this walk use the root-level
    layout consistently; otherwise those objects remain readable (metadata
    records full paths) but are outside the scanned section.

    Parameters
    ----------
    store_name : str, optional
        Store to scan (None = default store).
    config : Config, optional
        Config instance. If None, falls back to global settings.config.
    schema_name : str
        Schema whose section to list (required, keyword-only).

    Returns
    -------
    dict[str, int]
        Dict mapping each object file's relative path to its size in bytes.
    """
    backend = get_store_backend(store_name, config=config)
    stored: dict[str, int] = {}

    if config is None:
        from .settings import config as _global_config

        config = _global_config
    _spec = config.get_store_spec(store_name)
    _sp = _spec["schema_prefix"].strip("/")  # settings applies the "_schema" default
    rel = f"{_sp}/{schema_name}" if _sp else schema_name
    full_root = backend._full_path("")

    try:
        for root, dirs, files in backend.fs.walk(backend._full_path(rel)):
            for filename in files:
                # Manifest sidecars ({object}.manifest.json) are INCLUDED: they
                # are owned by their object and must be reclaimed with it when
                # the object is orphaned. _is_covered() keeps a live object's
                # manifest out of the orphan set.
                file_path = f"{root}/{filename}"
                relative_path = file_path.replace(full_root, "").lstrip("/")
                try:
                    stored[relative_path] = backend.fs.size(file_path)
                except Exception:
                    stored[relative_path] = 0
    except FileNotFoundError:
        pass  # this schema's section does not exist yet
    except Exception as e:
        logger.warning(f"Error listing stored schemas: {e}")

    return stored


def delete_schema_path(path: str, store_name: str | None = None, config=None) -> bool:
    """
    Delete a schema-addressed object file from storage.

    ``path`` is an individual object file (as enumerated by
    :func:`list_schema_paths`), not a directory — so only the orphaned token
    file is removed, leaving other versions/fields under the same primary-key
    directory intact. Empty parent directories are pruned best-effort.

    Parameters
    ----------
    path : str
        Object file path (relative to store root).
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
            # Remove just this object file (not the whole PK directory)
            backend.fs.rm(full_path)
            logger.debug(f"Deleted schema object: {path}")

            # Best-effort: prune now-empty parent directories up to the store root.
            root = backend._full_path("").rstrip("/")
            parent = full_path.rsplit("/", 1)[0]
            while parent and parent != root and parent.startswith(root):
                try:
                    if backend.fs.ls(parent):
                        break  # not empty
                    backend.fs.rmdir(parent)
                except Exception:
                    break
                parent = parent.rsplit("/", 1)[0]

            return True
    except Exception as e:
        logger.warning(f"Error deleting schema object {path}: {e}")

    return False


def _is_covered(path: str, referenced: set[str]) -> bool:
    """
    Return True if a stored file is accounted for by a referenced object path.

    A stored file is covered when:

    - it IS a referenced path (single-file object, e.g. ``field_token.npy``);
    - it lies UNDER a referenced path (directory-valued object, e.g. a Zarr
      store ``field_token/`` whose stored form is many chunk files); or
    - it is the ``.manifest.json`` sidecar of a referenced object (written
      alongside folder-valued objects).

    Uses O(path-depth) ancestor-prefix lookups against the referenced set.
    """
    if path.endswith(".manifest.json"):
        path = path[: -len(".manifest.json")]
    if path in referenced:
        return True
    idx = path.rfind("/")
    while idx > 0:
        if path[:idx] in referenced:
            return True
        idx = path.rfind("/", 0, idx)
    return False


def scan(
    *schemas: "Schema",
    store_name: str | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Scan for orphaned storage items without deleting.

    Scans both hash-addressed storage (``<hash@>``, ``<blob@>``, ``<attach@>``)
    and schema-addressed storage (any ``SchemaCodec``: ``<object@>``, ``<npy@>``
    and custom subclasses). Section locations follow the store's ``hash_prefix``
    / ``schema_prefix`` settings.

    Per-schema, and safe for subsets. Every managed object embeds its schema
    in the path (both sections, plus the legacy root-level layout), and hash
    deduplication is per-schema — so each schema is scanned independently
    against ITS OWN subtree. Orphan detection is therefore confined to the
    schemas you pass: objects of other schemas sharing the store are never
    listed and never at risk. (This removes the former requirement to pass
    *every* schema on a store at once.)

    Each schema's two sections are walked in isolation
    (``{hash_prefix}/{schema}/`` and ``{schema_prefix}/{schema}/``), so GC's
    blast radius is exactly those folders: user ``<filepath@>`` content and
    other schemas' objects are structurally out of reach. Objects under a
    *former* prefix (after a prefix change on a populated store) stay readable
    via their metadata paths but are not reclamation candidates until the
    setting is restored.

    Parameters
    ----------
    *schemas : Schema
        Schema instances to scan. Each is scanned against its own subtree;
        any subset of the schemas on a store may be passed safely.
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
        - orphaned_hashes: List of orphaned hash items as full relative store
          paths (NOT bare hashes — passed as-is to the deleter)
        - schema_paths_referenced: Number of schema items referenced in database
        - schema_paths_stored: Number of schema files in storage
        - schema_paths_orphaned: Number of unreferenced schema files
        - schema_paths_orphaned_bytes: Total size of orphaned schema files
        - orphaned_paths: List of orphaned schema files as full relative store paths
    """
    if not schemas:
        raise DataJointError("At least one schema must be provided")

    # Extract config from the first schema's connection
    _config = schemas[0].connection._config if schemas else None

    hash_referenced: set[str] = set()
    hash_stored: dict[str, int] = {}
    orphaned_hashes: set[str] = set()
    schema_paths_referenced: set[str] = set()
    schema_paths_stored: dict[str, int] = {}
    orphaned_paths: set[str] = set()

    # Scan each schema against its own subtree only. Because both sections
    # embed the schema name and hash dedup is per-schema, a schema's orphans
    # are fully determined by its own references vs. its own stored files —
    # no other schema on the store can influence (or be endangered by) the
    # result. Stats below aggregate across the passed schemas.
    for schema in schemas:
        db = schema.database

        # --- Hash-addressed storage (this schema's subtree) ---
        h_ref = scan_hash_references(schema, store_name=store_name, verbose=verbose)
        h_stored = list_hash_paths(store_name, config=_config, schema_name=db)
        orphaned_hashes |= set(h_stored.keys()) - h_ref

        # --- Schema-addressed storage (this schema's section) ---
        s_ref = scan_schema_references(schema, store_name=store_name, verbose=verbose)
        s_stored = list_schema_paths(store_name, config=_config, schema_name=db)
        # Coverage, not exact set difference: a referenced path may be a
        # DIRECTORY-valued object (e.g. a Zarr store), whose stored form is
        # many files under that prefix, plus a `.manifest.json` sidecar. The
        # schema walk is confined to this schema's schema_prefix section, so it
        # can only contain this schema's schema-addressed objects — coverage
        # against s_ref alone is complete (no hash objects can appear here).
        orphaned_paths |= {p for p in s_stored if not _is_covered(p, s_ref)}

        hash_referenced |= h_ref
        hash_stored.update(h_stored)
        schema_paths_referenced |= s_ref
        schema_paths_stored.update(s_stored)

    hash_orphaned_bytes = sum(hash_stored.get(h, 0) for h in orphaned_hashes)
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

    Scans the given schemas (via :func:`scan`) for storage references, then
    removes items covered by neither the schema nor hash reference set. Orphan
    identity therefore comes entirely from :func:`scan`: live objects outside
    the current section (after a prefix change) and the store's declared
    ``filepath_prefix`` namespace are never deleted.

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
            # Size map for bytes_freed — built per schema (listers are
            # per-schema) and merged across the passed schemas.
            hash_stored: dict[str, int] = {}
            for schema in schemas:
                hash_stored.update(list_hash_paths(store_name, config=_config, schema_name=schema.database))

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
            schema_paths_stored: dict[str, int] = {}
            for schema in schemas:
                schema_paths_stored.update(list_schema_paths(store_name, config=_config, schema_name=schema.database))

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
