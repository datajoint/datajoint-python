"""
Staged insert context manager for direct object storage writes.

This module provides the StagedInsert class which allows writing directly
to object storage before finalizing the database insert.
"""

import json
import mimetypes
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import IO, Any

import fsspec

from .errors import DataJointError
from .settings import config
from .storage import StorageBackend, build_object_path


class StagedInsert:
    """
    Context manager for staged insert operations.

    Allows direct writes to object storage before finalizing the database insert.
    Used for large objects like Zarr arrays where copying from local storage
    is inefficient.

    Usage:
        with table.staged_insert1 as staged:
            staged.rec['subject_id'] = 123
            staged.rec['session_id'] = 45

            # Create object storage directly
            z = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(1000, 1000))
            z[:] = data

            # Assign to record
            staged.rec['raw_data'] = z

        # On successful exit: metadata computed, record inserted
        # On exception: storage cleaned up, no record inserted
    """

    def __init__(self, table):
        """
        Initialize a staged insert.

        Args:
            table: The Table instance to insert into
        """
        self._table = table
        self._rec: dict[str, Any] = {}
        self._staged_objects: dict[str, dict] = {}  # field -> {path, ext, token}
        self._backend: StorageBackend | None = None

    @property
    def rec(self) -> dict[str, Any]:
        """Record dict for setting attribute values."""
        return self._rec

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return fsspec filesystem for advanced operations."""
        self._ensure_backend()
        return self._backend.fs

    def _ensure_backend(self):
        """Ensure storage backend is initialized."""
        if self._backend is None:
            try:
                spec = config.get_object_storage_spec()
                self._backend = StorageBackend(spec)
            except DataJointError:
                raise DataJointError(
                    "Object storage is not configured. Set object_storage settings in datajoint.json "
                    "or DJ_OBJECT_STORAGE_* environment variables."
                )

    def _get_storage_path(self, field: str, ext: str = "") -> str:
        """
        Get or create the storage path for a field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".zarr")

        Returns:
            Full storage path
        """
        self._ensure_backend()

        if field in self._staged_objects:
            return self._staged_objects[field]["full_path"]

        # Validate field is an object attribute
        if field not in self._table.heading:
            raise DataJointError(f"Attribute '{field}' not found in table heading")

        attr = self._table.heading[field]
        # Check if this is an object AttributeType (has adapter with "object" in type_name)
        if not (attr.codec and hasattr(attr.codec, "type_name") and "object" in attr.codec.type_name):
            raise DataJointError(f"Attribute '{field}' is not an <object> type")

        # Extract primary key from rec
        primary_key = {k: self._rec[k] for k in self._table.primary_key if k in self._rec}
        if len(primary_key) != len(self._table.primary_key):
            raise DataJointError(
                "Primary key values must be set in staged.rec before calling store() or open(). "
                f"Missing: {set(self._table.primary_key) - set(primary_key)}"
            )

        # Get storage spec
        spec = config.get_object_storage_spec()
        partition_pattern = spec.get("partition_pattern")
        token_length = spec.get("token_length", 8)

        # Build storage path (relative - StorageBackend will add location prefix)
        relative_path, token = build_object_path(
            schema=self._table.database,
            table=self._table.class_name,
            field=field,
            primary_key=primary_key,
            ext=ext if ext else None,
            partition_pattern=partition_pattern,
            token_length=token_length,
        )

        # Store staged object info (all paths are relative, backend adds location)
        self._staged_objects[field] = {
            "relative_path": relative_path,
            "ext": ext if ext else None,
            "token": token,
        }

        return relative_path

    def store(self, field: str, ext: str = "") -> fsspec.FSMap:
        """
        Get an FSMap store for direct writes to an object field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".zarr", ".hdf5")

        Returns:
            fsspec.FSMap suitable for Zarr/xarray
        """
        path = self._get_storage_path(field, ext)
        return self._backend.get_fsmap(path)

    def open(self, field: str, ext: str = "", mode: str = "wb") -> IO:
        """
        Open a file for direct writes to an object field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".bin", ".dat")
            mode: File mode (default: "wb")

        Returns:
            File-like object for writing
        """
        path = self._get_storage_path(field, ext)
        return self._backend.open(path, mode)

    def _compute_metadata(self, field: str) -> dict:
        """
        Compute metadata for a staged object after writing is complete.

        Args:
            field: Name of the object attribute

        Returns:
            JSON-serializable metadata dict
        """
        info = self._staged_objects[field]
        relative_path = info["relative_path"]
        ext = info["ext"]

        # Check if it's a directory (multiple files) or single file
        # _full_path adds the location prefix
        full_remote_path = self._backend._full_path(relative_path)

        try:
            is_dir = self._backend.fs.isdir(full_remote_path)
        except Exception:
            is_dir = False

        if is_dir:
            # Calculate total size and file count
            total_size = 0
            item_count = 0
            files = []

            for root, dirs, filenames in self._backend.fs.walk(full_remote_path):
                for filename in filenames:
                    file_path = f"{root}/{filename}"
                    try:
                        file_size = self._backend.fs.size(file_path)
                        rel_path = file_path[len(full_remote_path) :].lstrip("/")
                        files.append({"path": rel_path, "size": file_size})
                        total_size += file_size
                        item_count += 1
                    except Exception:
                        pass

            # Create manifest
            manifest = {
                "files": files,
                "total_size": total_size,
                "item_count": item_count,
                "created": datetime.now(timezone.utc).isoformat(),
            }

            # Write manifest alongside folder
            manifest_path = f"{relative_path}.manifest.json"
            self._backend.put_buffer(json.dumps(manifest, indent=2).encode(), manifest_path)

            metadata = {
                "path": relative_path,
                "size": total_size,
                "hash": None,
                "ext": ext,
                "is_dir": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "item_count": item_count,
            }
        else:
            # Single file
            try:
                size = self._backend.size(relative_path)
            except Exception:
                size = 0

            metadata = {
                "path": relative_path,
                "size": size,
                "hash": None,
                "ext": ext,
                "is_dir": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # Add mime_type for files
            if ext:
                mime_type, _ = mimetypes.guess_type(f"file{ext}")
                if mime_type:
                    metadata["mime_type"] = mime_type

        return metadata

    def _finalize(self):
        """
        Finalize the staged insert by computing metadata and inserting the record.
        """
        # Process each staged object
        for field in list(self._staged_objects.keys()):
            metadata = self._compute_metadata(field)
            # Store metadata dict in the record (ObjectType.encode handles it)
            self._rec[field] = metadata

        # Insert the record
        self._table.insert1(self._rec)

    def _cleanup(self):
        """
        Clean up staged objects on failure.
        """
        if self._backend is None:
            return

        for field, info in self._staged_objects.items():
            relative_path = info["relative_path"]
            try:
                # Check if it's a directory
                full_remote_path = self._backend._full_path(relative_path)
                if self._backend.fs.exists(full_remote_path):
                    if self._backend.fs.isdir(full_remote_path):
                        self._backend.remove_folder(relative_path)
                    else:
                        self._backend.remove(relative_path)
            except Exception:
                pass  # Best effort cleanup


@contextmanager
def staged_insert1(table):
    """
    Context manager for staged insert operations.

    Args:
        table: The Table instance to insert into

    Yields:
        StagedInsert instance for setting record values and getting storage handles

    Example:
        with staged_insert1(Recording) as staged:
            staged.rec['subject_id'] = 123
            staged.rec['session_id'] = 45
            z = zarr.open(staged.store('raw_data', '.zarr'), mode='w')
            z[:] = data
            staged.rec['raw_data'] = z
    """
    staged = StagedInsert(table)
    try:
        yield staged
        staged._finalize()
    except Exception:
        staged._cleanup()
        raise
