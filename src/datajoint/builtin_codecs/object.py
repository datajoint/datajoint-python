"""
Schema-addressed storage for files and folders.
"""

from __future__ import annotations

from typing import Any

from ..errors import DataJointError
from .schema import SchemaCodec


class ObjectCodec(SchemaCodec):
    """
    Schema-addressed storage for files and folders.

    The ``<object@>`` codec provides managed file/folder storage using
    schema-addressed paths: ``{schema}/{table}/{pk}/{field}/``. This creates
    a browsable organization in object storage that mirrors the database schema.

    Unlike hash-addressed storage (``<hash@>``), each row has its own unique path
    (no deduplication). Ideal for:

    - Zarr arrays (hierarchical chunked data)
    - HDF5 files
    - Complex multi-file outputs
    - Any content that shouldn't be deduplicated

    Store only - requires @ modifier.

    Example::

        @schema
        class Analysis(dj.Computed):
            definition = '''
            -> Recording
            ---
            results : <object@mystore>
            '''

        def make(self, key):
            # Store a file
            self.insert1({**key, 'results': '/path/to/results.zarr'})

        # Fetch returns ObjectRef for lazy access
        ref = (Analysis & key).fetch1('results')
        ref.path       # Storage path
        ref.read()     # Read file content
        ref.fsmap      # For zarr.open(ref.fsmap)

    Storage Structure:
        Objects are stored at::

            {store_root}/{schema}/{table}/{pk}/{field}/

    Deletion: Requires garbage collection via ``dj.gc.collect()``.

    Comparison with hash-addressed::

        | Aspect         | <object@>           | <hash@>             |
        |----------------|---------------------|---------------------|
        | Addressing     | Schema-addressed    | Hash-addressed      |
        | Deduplication  | No                  | Yes                 |
        | Deletion       | GC required         | GC required         |
        | Use case       | Zarr, HDF5          | Blobs, attachments  |

    See Also
    --------
    datajoint.gc : Garbage collection for orphaned storage.
    SchemaCodec : Base class for schema-addressed codecs.
    NpyCodec : Schema-addressed storage for numpy arrays.
    HashCodec : Hash-addressed storage with deduplication.
    """

    name = "object"

    def encode(
        self,
        value: Any,
        *,
        key: dict | None = None,
        store_name: str | None = None,
    ) -> dict:
        """
        Store content and return metadata.

        Parameters
        ----------
        value : bytes, str, or Path
            Content to store: bytes (raw data), or str/Path (file/folder to upload).
        key : dict, optional
            Context for path construction with keys ``_schema``, ``_table``,
            ``_field``, plus primary key values.
        store_name : str, optional
            Store to use. If None, uses default store.

        Returns
        -------
        dict
            Metadata dict suitable for ``ObjectRef.from_json()``.
        """
        from datetime import datetime, timezone
        from pathlib import Path

        # Extract context using inherited helper
        schema, table, field, primary_key = self._extract_context(key)
        config = (key or {}).get("_config")

        # Check for pre-computed metadata (from staged insert)
        if isinstance(value, dict) and "path" in value:
            # Already encoded, pass through
            return value

        # Determine content type and extension
        is_dir = False
        ext = None
        size = None
        item_count = None

        if isinstance(value, bytes):
            content = value
            size = len(content)
        elif isinstance(value, tuple) and len(value) == 2:
            # Tuple format: (extension, data) where data is bytes or file-like
            ext, data = value
            if hasattr(data, "read"):
                content = data.read()
            else:
                content = data
            size = len(content)
        elif isinstance(value, (str, Path)):
            source_path = Path(value)
            if not source_path.exists():
                raise DataJointError(f"Source path not found: {source_path}")
            is_dir = source_path.is_dir()
            ext = source_path.suffix if not is_dir else None
            if is_dir:
                # For directories, we'll upload later
                content = None
                # Count items in directory
                item_count = sum(1 for _ in source_path.rglob("*") if _.is_file())
            else:
                content = source_path.read_bytes()
                size = len(content)
        else:
            raise TypeError(f"<object> expects bytes or path, got {type(value).__name__}")

        # Build storage path using inherited helper
        path, token = self._build_path(schema, table, field, primary_key, ext=ext, store_name=store_name, config=config)

        # Get storage backend using inherited helper
        backend = self._get_backend(store_name, config=config)

        # Upload content
        if is_dir:
            # Upload directory recursively
            source_path = Path(value)
            backend.put_folder(str(source_path), path)
            # Compute size by summing all files
            size = sum(f.stat().st_size for f in source_path.rglob("*") if f.is_file())
        else:
            backend.put_buffer(content, path)

        # Build metadata
        timestamp = datetime.now(timezone.utc)
        metadata = {
            "path": path,
            "store": store_name,
            "size": size,
            "ext": ext,
            "is_dir": is_dir,
            "item_count": item_count,
            "timestamp": timestamp.isoformat(),
        }

        return metadata

    def decode(self, stored: dict, *, key: dict | None = None) -> Any:
        """
        Create ObjectRef handle for lazy access.

        Parameters
        ----------
        stored : dict
            Metadata dict from database.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        ObjectRef
            Handle for accessing the stored content.
        """
        from ..objectref import ObjectRef

        config = (key or {}).get("_config")
        backend = self._get_backend(stored.get("store"), config=config)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate value is bytes, path, dict metadata, or (ext, data) tuple."""
        from pathlib import Path

        if isinstance(value, bytes):
            return
        if isinstance(value, (str, Path)):
            # Could be a path or pre-encoded JSON string
            return
        if isinstance(value, tuple) and len(value) == 2:
            # Tuple format: (extension, data)
            return
        if isinstance(value, dict) and "path" in value:
            # Pre-computed metadata dict (from staged insert)
            return
        raise TypeError(f"<object> expects bytes or path, got {type(value).__name__}")
