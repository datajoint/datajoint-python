"""
Staged insert context manager for direct object storage writes.

This module provides the StagedInsert class which allows writing directly
to object storage before finalizing the database insert.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import IO, TYPE_CHECKING, Any

import fsspec

from .codecs import resolve_dtype
from .errors import DataJointError
from .hash_registry import get_store_backend
from .storage import build_object_path

if TYPE_CHECKING:
    from .storage import StorageBackend


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

            # Write directly to object storage
            z = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(1000, 1000))
            z[:] = data

        # On clean exit: metadata is computed and the row is inserted.
        #   The caller does NOT assign anything to staged.rec[<object field>] —
        #   the framework computes the metadata dict.
        # On exception: storage cleaned up, no row inserted.
    """

    def __init__(self, table):
        """
        Initialize a staged insert.

        Args:
            table: The Table instance to insert into
        """
        self._table = table
        self._rec: dict[str, Any] = {}
        self._staged_objects: dict[str, dict] = {}  # field -> {relative_path, ext, token, store_name}

    @property
    def rec(self) -> dict[str, Any]:
        """Record dict for setting attribute values."""
        return self._rec

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """
        Return fsspec filesystem for the default store, for advanced operations.

        For per-field access, prefer ``staged.store(field)`` or ``staged.open(field)`` —
        those route to the store resolved from the field's type spec.
        """
        return self._default_backend().fs

    def _default_backend(self):
        """Return the StorageBackend for the default store, or raise a clear error."""
        try:
            return get_store_backend(None, config=self._table.connection._config)
        except DataJointError:
            raise DataJointError("Storage is not configured. Set stores.default and stores.<name> settings in datajoint.json.")

    def _resolve_field(self, field: str, ext: str) -> tuple[str, "StorageBackend"]:
        """
        Resolve a field to its (relative_path, backend), caching on first call.

        Validates the field is an ``<object@>`` attribute and that the full
        primary key is set on ``staged.rec``.
        """
        if field in self._staged_objects:
            info = self._staged_objects[field]
            return info["relative_path"], self._field_backend(info["store_name"])

        if field not in self._table.heading:
            raise DataJointError(f"Attribute '{field}' not found in table heading")

        attr = self._table.heading[field]
        if not (attr.codec and attr.codec.name == "object"):
            raise DataJointError(f"Attribute '{field}' is not an <object> type")

        primary_key = {k: self._rec[k] for k in self._table.primary_key if k in self._rec}
        if len(primary_key) != len(self._table.primary_key):
            raise DataJointError(
                "Primary key values must be set in staged.rec before calling store() or open(). "
                f"Missing: {set(self._table.primary_key) - set(primary_key)}"
            )

        # Resolve the store name from the field's type spec (e.g., <object@local> -> "local")
        _, _, store_name = resolve_dtype(f"<{attr.codec.name}>", store_name=attr.store)

        config = self._table.connection._config
        try:
            spec = config.get_store_spec(store_name)
        except DataJointError:
            raise DataJointError("Storage is not configured. Set stores.default and stores.<name> settings in datajoint.json.")
        partition_pattern = spec.get("partition_pattern")
        token_length = spec.get("token_length", 8)

        relative_path, token = build_object_path(
            schema=self._table.database,
            table=self._table.class_name,
            field=field,
            primary_key=primary_key,
            ext=ext if ext else None,
            partition_pattern=partition_pattern,
            token_length=token_length,
        )

        self._staged_objects[field] = {
            "relative_path": relative_path,
            "ext": ext if ext else None,
            "token": token,
            "store_name": store_name,
        }

        return relative_path, self._field_backend(store_name)

    def _field_backend(self, store_name: str | None):
        """Return the StorageBackend for the named store."""
        try:
            return get_store_backend(store_name, config=self._table.connection._config)
        except DataJointError:
            raise DataJointError("Storage is not configured. Set stores.default and stores.<name> settings in datajoint.json.")

    def store(self, field: str, ext: str = "") -> fsspec.FSMap:
        """
        Get an FSMap for direct writes to an ``<object@>`` field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".zarr", ".hdf5")

        Returns:
            fsspec.FSMap suitable for Zarr/xarray
        """
        relative_path, backend = self._resolve_field(field, ext)
        return backend.get_fsmap(relative_path)

    def open(self, field: str, ext: str = "", mode: str = "wb") -> IO:
        """
        Open a file for direct writes to an ``<object@>`` field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".bin", ".dat")
            mode: File mode (default: "wb")

        Returns:
            File-like object for writing
        """
        relative_path, backend = self._resolve_field(field, ext)
        return backend.open(relative_path, mode)

    def _compute_metadata(self, field: str) -> dict:
        """
        Compute the canonical ``<object@>`` metadata dict for a staged write.

        The returned dict is structurally equal to what ``ObjectCodec.encode``
        would produce for the same content, modulo ``timestamp``.

        Returns
        -------
        dict
            ``{path, store, size, ext, is_dir, item_count, timestamp}``
        """
        info = self._staged_objects[field]
        relative_path = info["relative_path"]
        ext = info["ext"]
        store_name = info["store_name"]
        backend = self._field_backend(store_name)

        full_remote_path = backend._full_path(relative_path)

        try:
            is_dir = backend.fs.isdir(full_remote_path)
        except Exception:
            is_dir = False

        if is_dir:
            total_size = 0
            item_count = 0
            for root, _dirs, filenames in backend.fs.walk(full_remote_path):
                for filename in filenames:
                    try:
                        total_size += backend.fs.size(f"{root}/{filename}")
                        item_count += 1
                    except Exception:
                        pass
            size = total_size
        else:
            try:
                size = backend.size(relative_path)
            except Exception:
                size = 0
            item_count = None

        return {
            "path": relative_path,
            "store": store_name,
            "size": size,
            "ext": ext,
            "is_dir": is_dir,
            "item_count": item_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _finalize(self):
        """
        Compute metadata for each staged object and insert the row.
        """
        for field in list(self._staged_objects.keys()):
            self._rec[field] = self._compute_metadata(field)
        self._table.insert1(self._rec)

    def _cleanup(self):
        """
        Best-effort removal of staged objects on failure.
        """
        for field, info in self._staged_objects.items():
            relative_path = info["relative_path"]
            try:
                backend = self._field_backend(info["store_name"])
                full_remote_path = backend._full_path(relative_path)
                if backend.fs.exists(full_remote_path):
                    if backend.fs.isdir(full_remote_path):
                        backend.remove_folder(relative_path)
                    else:
                        backend.remove(relative_path)
            except Exception:
                pass  # Best-effort cleanup


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
            # Metadata for 'raw_data' is computed on clean exit; do not assign it here.
    """
    staged = StagedInsert(table)
    try:
        yield staged
        staged._finalize()
    except Exception:
        staged._cleanup()
        raise
