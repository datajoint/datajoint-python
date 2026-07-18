"""
Garbage collection for object storage.

This module identifies and removes orphaned items from object storage. Storage
items become orphaned when all database rows referencing them are deleted.

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

Garbage collection is **store-specific** and handles one store at a time. The
:class:`GarbageCollector` is bound to its schemas and store at construction;
neither is threaded through each call (the config defaults to the schemas')::

    import datajoint as dj

    collector = dj.gc.GarbageCollector(schema1, schema2, store="mystore")
    stats = collector.collect()              # read-only report (dry_run=True default)
    stats = collector.collect(dry_run=False) # actually delete the orphans

Both sections embed the schema name in every path, so garbage collection is
**per-schema**: each schema is scanned against its own subtree only. Orphan
detection is therefore confined to the collector's schemas — any subset of the
schemas sharing a store may be collected safely, and GC never touches another
schema's objects or user-managed content elsewhere in the store.

See Also
--------
datajoint.builtin_codecs : Codec implementations for object storage types.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from .errors import DataJointError
from .hash_registry import delete_path, get_store_backend

if TYPE_CHECKING:
    from .schemas import _Schema as Schema

logger = logging.getLogger(__name__.split(".")[0])

# Base32 content-hash filename: 26 lowercase alphanumeric chars.
_BASE32_HASH = re.compile(r"^[a-z2-7]{26}$")


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


class GarbageCollector:
    """
    Store-specific garbage collector — one store, a fixed set of schemas.

    The schemas to collect and the store are bound at construction, so neither
    is threaded through individual operations. The store is resolved eagerly
    (an unknown/misconfigured store raises immediately). Storage-side work (the
    stored-file listings, deletion) uses this store; database metadata is read
    through each schema's own connection.

    Parameters
    ----------
    *schemas : Schema
        The schemas to collect. At least one is required. Every managed path
        embeds its schema, so collection is per-schema and confined to these
        schemas: objects of any other schema sharing the store are never listed
        and never at risk — pass any subset safely.
    store : str, optional
        Store name (None = the default store), keyword-only.
    config : Config, optional
        Config that defines the store. Defaults to the first schema's
        connection config (``schemas[0].connection._config``).
    """

    def __init__(self, *schemas: "Schema", store: str | None = None, config=None) -> None:
        if not schemas:
            raise DataJointError("At least one schema must be provided")
        self.schemas = schemas
        self.store = store
        self.config = config if config is not None else schemas[0].connection._config
        # Resolve the store eagerly: validates it exists and pins the backend
        # and section prefixes for this collector's lifetime (one store only).
        self.backend = get_store_backend(store, config=self.config)
        spec = self.config.get_store_spec(store)
        self._hash_prefix = spec["hash_prefix"].strip("/")  # settings applies the "_hash" default
        self._schema_prefix = spec["schema_prefix"].strip("/")  # ... "_schema" default
        # Per-collect() scan errors (walk failures in list_*_paths). Non-empty
        # blocks destructive deletion in collect(dry_run=False). Reset at the
        # start of each collect() call.
        self._scan_errors: list[str] = []

    # ------------------------------------------------------------------ #
    # References — extracted from database metadata (per-schema connection)
    # ------------------------------------------------------------------ #
    def hash_references(self, verbose: bool = False) -> set[str]:
        """
        Full relative store paths of hash-addressed objects referenced by live
        rows across this collector's schemas.

        Reads columns classified by ``Heading.hash_objects`` (``<hash@>``, and
        external ``<blob@>``/``<attach@>``). Paths are taken verbatim from each
        row's metadata (independent of the store's current prefixes) and
        filtered to this collector's store.
        """
        return self._references("hash_objects", verbose)

    def schema_references(self, verbose: bool = False) -> set[str]:
        """
        Full relative store paths of schema-addressed objects referenced by live
        rows across this collector's schemas.

        Reads columns classified by ``Heading.schema_objects`` — any
        ``SchemaCodec`` (``<object@>``, ``<npy@>``, custom subclasses,
        recognized by type per #1469). For a directory-valued object the
        recorded path is the directory prefix, not its individual files.
        """
        return self._references("schema_objects", verbose)

    def _references(self, heading_attr: str, verbose: bool) -> set[str]:
        referenced: set[str] = set()
        for schema in self.schemas:
            if verbose:
                logger.info(f"Scanning schema {schema.database} for {heading_attr}")
            for table_name in schema.list_tables():
                try:
                    table = schema.get_table(table_name)
                    # Classification lives on the heading — one source of truth.
                    for attr_name in getattr(table.heading, heading_attr):
                        attr = table.heading.attributes[attr_name]
                        if verbose:
                            logger.info(f"  Scanning {table_name}.{attr_name}")
                        # Read raw JSON metadata via cursor — bypasses
                        # decode_attribute, so we get the stored dict
                        # (PostgreSQL/JSONB) or JSON string (MySQL). The codec's
                        # referenced_paths() extracts paths and handles both
                        # shapes (codec-driven discovery, #1469).
                        try:
                            for row in table.proj(attr_name).cursor(as_dict=True):
                                for path, ref_store in attr.codec.referenced_paths(row[attr_name]):
                                    if self.store is None or ref_store == self.store:
                                        referenced.add(path)
                        except Exception as e:
                            logger.warning(f"Error scanning {table_name}.{attr_name}: {e}")
                except Exception as e:
                    logger.warning(f"Error accessing table {table_name}: {e}")
        return referenced

    # ------------------------------------------------------------------ #
    # Stored listings — walk this store, one schema's subtree
    # ------------------------------------------------------------------ #
    def list_hash_paths(self, schema_name: str) -> dict[str, int]:
        """
        List one schema's hash-addressed items: ``{hash_prefix}/{schema}/...``.

        Every hash path embeds the schema and deduplication is per-schema, so
        this listing is complete for that schema and independent of every other
        schema on the store. Only the currently configured prefix is walked;
        objects under a former prefix are not listed (they remain readable via
        their metadata paths).

        Returns a dict mapping each object's full relative store path to size.
        """
        hp = self._hash_prefix
        section = f"{hp}/{schema_name}/" if hp else f"{schema_name}/"
        full_root = self.backend._full_path("")
        stored: dict[str, int] = {}
        try:
            for root, _dirs, files in self.backend.fs.walk(self.backend._full_path(section)):
                for filename in files:
                    if filename.endswith(".manifest.json"):
                        continue  # folder-object sidecar, not a hash file
                    if not _BASE32_HASH.match(filename):
                        continue
                    file_path = f"{root}/{filename}"
                    relative_path = file_path[len(full_root) :].lstrip("/")
                    try:
                        stored[relative_path] = self.backend.fs.size(file_path)
                    except Exception as e:
                        logger.warning(f"Could not size {file_path}: {e}")
                        stored[relative_path] = 0
        except FileNotFoundError:
            pass  # the hash section does not exist yet
        except Exception as e:
            logger.warning(f"Error listing stored hashes: {e}")
            self._scan_errors.append(f"list_hash_paths({schema_name}): {e}")
        return stored

    def list_schema_paths(self, schema_name: str) -> dict[str, int]:
        """
        List one schema's schema-addressed object files: ``{schema_prefix}/{schema}/...``.

        Enumerates individual files: a single-file object is one file at
        ``{schema_prefix}/{schema}/{table}/{pk}/{field}_{token}[.ext]``; a
        directory-valued object (e.g. a Zarr store) is many files under that
        prefix, plus a ``{path}.manifest.json`` sidecar — all listed. Orphan
        detection matches them against references via :func:`_is_covered`.

        The walk is confined to the schema's own section, so it never enters the
        hash or filepath sections (mutually-exclusive top-level prefixes,
        enforced by store validation) — GC's blast radius is exactly
        ``{schema_prefix}/{schema}/``; user ``<filepath@>`` content and other
        schemas' objects are structurally out of reach.

        Legacy layout: DataJoint 2.3.0 and earlier wrote schema-addressed
        objects at root level ``{schema}/...`` (``schema_prefix`` ignored, see
        datajoint/datajoint-python#1487). Such a store should set
        ``schema_prefix=""`` so writes and this walk use the root-level layout
        consistently; otherwise those objects remain readable but outside the
        scanned section.

        Returns a dict mapping each object file's relative path to size.
        """
        sp = self._schema_prefix
        rel = f"{sp}/{schema_name}" if sp else schema_name
        full_root = self.backend._full_path("")
        stored: dict[str, int] = {}
        try:
            for root, _dirs, files in self.backend.fs.walk(self.backend._full_path(rel)):
                for filename in files:
                    # Manifest sidecars are INCLUDED: owned by their object and
                    # reclaimed with it. _is_covered() keeps a live object's
                    # manifest out of the orphan set.
                    file_path = f"{root}/{filename}"
                    relative_path = file_path[len(full_root) :].lstrip("/")
                    try:
                        stored[relative_path] = self.backend.fs.size(file_path)
                    except Exception as e:
                        logger.warning(f"Could not size {file_path}: {e}")
                        stored[relative_path] = 0
        except FileNotFoundError:
            pass  # this schema's section does not exist yet
        except Exception as e:
            logger.warning(f"Error listing stored schemas: {e}")
            self._scan_errors.append(f"list_schema_paths({schema_name}): {e}")
        return stored

    # ------------------------------------------------------------------ #
    # Deletion
    # ------------------------------------------------------------------ #
    def delete_schema_path(self, path: str) -> bool:
        """
        Delete a single schema-addressed object file (not a directory).

        ``path`` is an individual file (as enumerated by
        :meth:`list_schema_paths`), so only the orphaned token file is removed,
        leaving other versions/fields under the same primary-key directory
        intact. Empty parent directories are pruned best-effort. Returns True if
        deleted, False if not found.
        """
        try:
            full_path = self.backend._full_path(path)
            if self.backend.fs.exists(full_path):
                self.backend.fs.rm(full_path)
                logger.debug(f"Deleted schema object: {path}")
                # Best-effort: prune now-empty parent directories to store root.
                root = self.backend._full_path("").rstrip("/")
                parent = full_path.rsplit("/", 1)[0]
                while parent and parent != root and parent.startswith(root):
                    try:
                        if self.backend.fs.ls(parent):
                            break  # not empty
                        self.backend.fs.rmdir(parent)
                    except Exception:
                        break
                    parent = parent.rsplit("/", 1)[0]
                return True
        except Exception as e:
            logger.warning(f"Error deleting schema object {path}: {e}")
        return False

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #
    def collect(self, dry_run: bool = True, verbose: bool = False) -> dict[str, Any]:
        """
        Report — and, unless ``dry_run``, remove — orphaned storage.

        ``dry_run=True`` (the default) is a **read-only scan**: it reports what
        would be removed and deletes nothing. ``dry_run=False`` additionally
        deletes the orphans. The report is the same either way, so the safe
        default doubles as the inspection call.

        Every managed path embeds its schema, so a schema's stored files can
        only match that schema's references — orphan detection is confined to
        this collector's schemas; objects of any other schema on the store are
        never listed, deleted, or at risk. Objects under a *former* prefix stay
        readable via their metadata paths but are not reclamation candidates
        until the setting is restored.

        Returns a dict with per-section stats and the deletion outcome:

        - hash_paths_referenced / hash_paths_stored / hash_paths_orphaned / hash_paths_orphaned_bytes
        - orphaned_hash_paths: orphaned hash items as full relative store paths
          (NOT bare hashes — passed as-is to the deleter)
        - schema_paths_referenced / schema_paths_stored / schema_paths_orphaned
          / schema_paths_orphaned_bytes
        - orphaned_schema_paths: orphaned schema files as full relative store paths
        - hash_paths_deleted / schema_paths_deleted / deleted / bytes_freed (0 when
          dry_run) / errors / dry_run
        """
        self._scan_errors = []  # reset per-call; populated by list_*_paths below
        # References can be gathered across all schemas at once: because paths
        # embed the schema, a file under schema X is only ever covered by an X
        # reference, so combined coverage equals per-schema coverage.
        hash_paths_referenced = self.hash_references(verbose=verbose)
        schema_paths_referenced = self.schema_references(verbose=verbose)

        hash_paths_stored: dict[str, int] = {}
        schema_paths_stored: dict[str, int] = {}
        for schema in self.schemas:
            hash_paths_stored.update(self.list_hash_paths(schema.database))
            schema_paths_stored.update(self.list_schema_paths(schema.database))

        orphaned_hash_paths = sorted(set(hash_paths_stored.keys()) - hash_paths_referenced)
        # Coverage, not exact set difference: a referenced path may be a
        # directory-valued object (many files + a manifest sidecar).
        orphaned_schema_paths = sorted(p for p in schema_paths_stored if not _is_covered(p, schema_paths_referenced))

        hash_paths_deleted = 0
        schema_paths_deleted = 0
        bytes_freed = 0
        errors = 0

        if not dry_run:
            if self._scan_errors:
                raise DataJointError(
                    f"Refusing to delete: {len(self._scan_errors)} scan error(s) — "
                    f"partial listing risks classifying live files as orphaned. "
                    f"Errors: {self._scan_errors}"
                )
            # The size maps from the scan above are reused for the byte tally —
            # no second listing pass.
            for path in orphaned_hash_paths:
                try:
                    if delete_path(path, self.store, config=self.config):
                        hash_paths_deleted += 1
                        bytes_freed += hash_paths_stored.get(path, 0)
                        if verbose:
                            logger.info(f"Deleted: {path} ({hash_paths_stored.get(path, 0)} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete {path}: {e}")

            for path in orphaned_schema_paths:
                try:
                    if self.delete_schema_path(path):
                        schema_paths_deleted += 1
                        bytes_freed += schema_paths_stored.get(path, 0)
                        if verbose:
                            logger.info(f"Deleted schema path: {path} ({schema_paths_stored.get(path, 0)} bytes)")
                except Exception as e:
                    errors += 1
                    logger.warning(f"Failed to delete schema path {path}: {e}")

        return {
            # Hash-addressed storage stats
            "hash_paths_referenced": len(hash_paths_referenced),
            "hash_paths_stored": len(hash_paths_stored),
            "hash_paths_orphaned": len(orphaned_hash_paths),
            "hash_paths_orphaned_bytes": sum(hash_paths_stored.get(h, 0) for h in orphaned_hash_paths),
            "orphaned_hash_paths": orphaned_hash_paths,
            # Schema-addressed storage stats
            "schema_paths_referenced": len(schema_paths_referenced),
            "schema_paths_stored": len(schema_paths_stored),
            "schema_paths_orphaned": len(orphaned_schema_paths),
            "schema_paths_orphaned_bytes": sum(schema_paths_stored.get(p, 0) for p in orphaned_schema_paths),
            "orphaned_schema_paths": orphaned_schema_paths,
            # Deletion outcome (all zero when dry_run)
            "hash_paths_deleted": hash_paths_deleted,
            "schema_paths_deleted": schema_paths_deleted,
            "deleted": hash_paths_deleted + schema_paths_deleted,
            "bytes_freed": bytes_freed,
            "errors": errors,
            "scan_errors": list(self._scan_errors),
            "dry_run": dry_run,
        }
