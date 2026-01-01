"""
Built-in DataJoint codecs.

This module defines the standard codecs that ship with DataJoint.
These serve as both useful built-in codecs and as examples for users who
want to create their own custom codecs.

Built-in Codecs:
    - ``<blob>``: Serialize Python objects (internal) or external with dedup
    - ``<hash>``: Hash-addressed storage with MD5 deduplication
    - ``<object>``: Path-addressed storage for files/folders (Zarr, HDF5)
    - ``<attach>``: File attachment (internal) or external with dedup
    - ``<filepath@store>``: Reference to existing file in store

Example - Creating a Custom Codec:
    Here's how to define your own codec, modeled after the built-in codecs::

        import datajoint as dj
        import networkx as nx

        class GraphCodec(dj.Codec):
            '''Store NetworkX graphs as edge lists.'''

            name = "graph"  # Use as <graph> in definitions

            def get_dtype(self, is_external: bool) -> str:
                return "<blob>"  # Compose with blob for serialization

            def encode(self, graph, *, key=None, store_name=None):
                # Convert graph to a serializable format
                return {
                    'nodes': list(graph.nodes(data=True)),
                    'edges': list(graph.edges(data=True)),
                }

            def decode(self, stored, *, key=None):
                # Reconstruct graph from stored format
                G = nx.Graph()
                G.add_nodes_from(stored['nodes'])
                G.add_edges_from(stored['edges'])
                return G

            def validate(self, value):
                if not isinstance(value, nx.Graph):
                    raise TypeError(f"Expected nx.Graph, got {type(value).__name__}")

        # Now use in table definitions:
        @schema
        class Networks(dj.Manual):
            definition = '''
            network_id : int
            ---
            topology : <graph>
            '''
"""

from __future__ import annotations

from typing import Any

from .codecs import Codec
from .errors import DataJointError


# =============================================================================
# Blob Codec - DataJoint's native serialization
# =============================================================================


class BlobCodec(Codec):
    """
    Serialize Python objects using DataJoint's blob format.

    The ``<blob>`` codec handles serialization of arbitrary Python objects
    including NumPy arrays, dictionaries, lists, datetime objects, and UUIDs.

    Supports both internal and external storage:
    - ``<blob>``: Stored in database (bytes → LONGBLOB)
    - ``<blob@>``: Stored externally via ``<hash@>`` with deduplication
    - ``<blob@store>``: Stored in specific named store

    Format Features:
        - Protocol headers (``mYm`` for MATLAB-compatible, ``dj0`` for Python-native)
        - Optional zlib compression for data > 1KB
        - Support for nested structures

    Example::

        @schema
        class ProcessedData(dj.Manual):
            definition = '''
            data_id : int
            ---
            small_result : <blob>       # internal (in database)
            large_result : <blob@>      # external (default store)
            archive : <blob@cold>       # external (specific store)
            '''

        # Insert any serializable object
        table.insert1({'data_id': 1, 'small_result': {'scores': [0.9, 0.8]}})
    """

    name = "blob"

    def get_dtype(self, is_external: bool) -> str:
        """Return bytes for internal, <hash> for external storage."""
        return "<hash>" if is_external else "bytes"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """Serialize a Python object to DataJoint's blob format."""
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """Deserialize blob bytes back to a Python object."""
        from . import blob

        return blob.unpack(stored, squeeze=False)


# Note: DJBlobType is defined at end of file as DJBlobCodec (not BlobCodec)


# =============================================================================
# Hash-Addressed Storage Codec
# =============================================================================


class HashCodec(Codec):
    """
    Hash-addressed storage with MD5 deduplication.

    The ``<hash@>`` codec stores raw bytes using content-addressed storage.
    Data is identified by its MD5 hash and stored in a hierarchical directory:
    ``_hash/{hash[:2]}/{hash[2:4]}/{hash}``

    The database column stores JSON metadata: ``{hash, store, size}``.
    Duplicate content is automatically deduplicated.

    External only - requires @ modifier.

    Example::

        @schema
        class RawContent(dj.Manual):
            definition = '''
            content_id : int
            ---
            data : <hash@mystore>
            '''

        # Insert raw bytes
        table.insert1({'content_id': 1, 'data': b'raw binary content'})

    Note:
        This codec accepts only ``bytes``. For Python objects, use ``<blob@>``.
        Typically used indirectly via ``<blob@>`` or ``<attach@>`` rather than directly.
    """

    name = "hash"

    def get_dtype(self, is_external: bool) -> str:
        """Hash storage is external only."""
        if not is_external:
            raise DataJointError("<hash> requires @ (external storage only)")
        return "json"

    def encode(self, value: bytes, *, key: dict | None = None, store_name: str | None = None) -> dict:
        """
        Store content and return metadata.

        Args:
            value: Raw bytes to store.
            key: Primary key values (unused).
            store_name: Store to use. If None, uses default store.

        Returns:
            Metadata dict: {hash, store, size}
        """
        from .content_registry import put_content

        return put_content(value, store_name=store_name)

    def decode(self, stored: dict, *, key: dict | None = None) -> bytes:
        """
        Retrieve content by hash.

        Args:
            stored: Metadata dict with 'hash' and optionally 'store'.
            key: Primary key values (unused).

        Returns:
            Original bytes.
        """
        from .content_registry import get_content

        return get_content(stored["hash"], store_name=stored.get("store"))

    def validate(self, value: Any) -> None:
        """Validate that value is bytes."""
        if not isinstance(value, bytes):
            raise TypeError(f"<hash> expects bytes, got {type(value).__name__}")


# Note: ContentType is defined at end of file as ContentCodec (not HashCodec)


# =============================================================================
# Path-Addressed Storage Codec (OAS - Object-Augmented Schema)
# =============================================================================


class ObjectCodec(Codec):
    """
    Path-addressed storage for files and folders.

    The ``<object@>`` codec provides managed file/folder storage where the path
    is derived from the primary key: ``{schema}/{table}/{pk}/{field}/``

    Unlike ``<hash@>`` (hash-addressed), each row has its own storage path,
    and content is deleted when the row is deleted. This is ideal for:

    - Zarr arrays (hierarchical chunked data)
    - HDF5 files
    - Complex multi-file outputs
    - Any content that shouldn't be deduplicated

    External only - requires @ modifier.

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

    Comparison with ``<hash@>``::

        | Aspect         | <object@>         | <hash@>             |
        |----------------|-------------------|---------------------|
        | Addressing     | Path (by PK)      | Hash (by content)   |
        | Deduplication  | No                | Yes                 |
        | Deletion       | With row          | GC when unreferenced|
        | Use case       | Zarr, HDF5        | Blobs, attachments  |
    """

    name = "object"

    def get_dtype(self, is_external: bool) -> str:
        """Object storage is external only."""
        if not is_external:
            raise DataJointError("<object> requires @ (external storage only)")
        return "json"

    def encode(
        self,
        value: Any,
        *,
        key: dict | None = None,
        store_name: str | None = None,
    ) -> dict:
        """
        Store content and return metadata.

        Args:
            value: Content to store. Can be:
                - bytes: Raw bytes to store as file
                - str/Path: Path to local file or folder to upload
            key: Dict containing context for path construction:
                - _schema: Schema name
                - _table: Table name
                - _field: Field/attribute name
                - Other entries are primary key values
            store_name: Store to use. If None, uses default store.

        Returns:
            Metadata dict suitable for ObjectRef.from_json()
        """
        from datetime import datetime, timezone
        from pathlib import Path

        from .content_registry import get_store_backend
        from .storage import build_object_path

        # Extract context from key
        key = key or {}
        schema = key.pop("_schema", "unknown")
        table = key.pop("_table", "unknown")
        field = key.pop("_field", "data")
        primary_key = {k: v for k, v in key.items() if not k.startswith("_")}

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

        # Build storage path
        path, token = build_object_path(
            schema=schema,
            table=table,
            field=field,
            primary_key=primary_key,
            ext=ext,
        )

        # Get storage backend
        backend = get_store_backend(store_name)

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

        Args:
            stored: Metadata dict from database.
            key: Primary key values (unused).

        Returns:
            ObjectRef for accessing the stored content.
        """
        from .objectref import ObjectRef
        from .content_registry import get_store_backend

        store_name = stored.get("store")
        backend = get_store_backend(store_name)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate that value is bytes, path, dict metadata, or (extension, data) tuple."""
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


# Backward compatibility alias
ObjectType = ObjectCodec


# =============================================================================
# File Attachment Codecs
# =============================================================================


class AttachCodec(Codec):
    """
    File attachment with filename preserved.

    Supports both internal and external storage:
    - ``<attach>``: Stored in database (bytes → LONGBLOB)
    - ``<attach@>``: Stored externally via ``<hash@>`` with deduplication
    - ``<attach@store>``: Stored in specific named store

    The filename is preserved and the file is extracted to the configured
    download path on fetch.

    Example::

        @schema
        class Documents(dj.Manual):
            definition = '''
            doc_id : int
            ---
            config : <attach>           # internal (small file in DB)
            dataset : <attach@>         # external (default store)
            archive : <attach@cold>     # external (specific store)
            '''

        # Insert a file
        table.insert1({'doc_id': 1, 'config': '/path/to/config.json'})

        # Fetch extracts to download_path and returns local path
        local_path = (table & 'doc_id=1').fetch1('config')

    Storage Format (internal):
        The blob contains: ``filename\\0contents``
        - Filename (UTF-8 encoded) + null byte + raw file contents
    """

    name = "attach"

    def get_dtype(self, is_external: bool) -> str:
        """Return bytes for internal, <hash> for external storage."""
        return "<hash>" if is_external else "bytes"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """
        Read file and encode as filename + contents.

        Args:
            value: Path to file (str or Path).
            key: Primary key values (unused).
            store_name: Unused for internal storage.

        Returns:
            Bytes: filename (UTF-8) + null byte + file contents
        """
        from pathlib import Path

        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"Attachment file not found: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"<attach> does not support directories: {path}")

        filename = path.name
        contents = path.read_bytes()
        return filename.encode("utf-8") + b"\x00" + contents

    def decode(self, stored: bytes, *, key: dict | None = None) -> str:
        """
        Extract file to download path and return local path.

        Args:
            stored: Blob containing filename + null + contents.
            key: Primary key values (unused).

        Returns:
            Path to extracted file as string.
        """
        from pathlib import Path

        from .settings import config

        # Split on first null byte
        null_pos = stored.index(b"\x00")
        filename = stored[:null_pos].decode("utf-8")
        contents = stored[null_pos + 1 :]

        # Write to download path
        download_path = Path(config.get("download_path", "."))
        download_path.mkdir(parents=True, exist_ok=True)
        local_path = download_path / filename

        # Handle filename collision - if file exists with different content, add suffix
        if local_path.exists():
            existing_contents = local_path.read_bytes()
            if existing_contents != contents:
                # Find unique filename
                stem = local_path.stem
                suffix = local_path.suffix
                counter = 1
                while local_path.exists() and local_path.read_bytes() != contents:
                    local_path = download_path / f"{stem}_{counter}{suffix}"
                    counter += 1

        # Only write if file doesn't exist or has different content
        if not local_path.exists():
            local_path.write_bytes(contents)

        return str(local_path)

    def validate(self, value: Any) -> None:
        """Validate that value is a valid file path."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<attach> expects a file path, got {type(value).__name__}")


# Backward compatibility aliases
AttachType = AttachCodec
XAttachType = AttachCodec  # <attach@> is now just AttachCodec with external storage


# =============================================================================
# Filepath Reference Codec
# =============================================================================


class FilepathCodec(Codec):
    """
    Reference to existing file in configured store.

    The ``<filepath@store>`` codec stores a reference to a file that already
    exists in the storage backend. Unlike ``<attach>`` or ``<object@>``, no
    file copying occurs - only the path is recorded.

    External only - requires @store.

    This is useful when:
    - Files are managed externally (e.g., by acquisition software)
    - Files are too large to copy
    - You want to reference shared datasets

    Example::

        @schema
        class Recordings(dj.Manual):
            definition = '''
            recording_id : int
            ---
            raw_data : <filepath@acquisition>
            '''

        # Reference an existing file (no copy)
        table.insert1({'recording_id': 1, 'raw_data': 'subject01/session001/data.bin'})

        # Fetch returns ObjectRef for lazy access
        ref = (table & 'recording_id=1').fetch1('raw_data')
        ref.read()      # Read file content
        ref.download()  # Download to local path

    Storage Format:
        JSON metadata: ``{path, store}``

    Warning:
        The file must exist in the store at the specified path.
        DataJoint does not manage the lifecycle of referenced files.
    """

    name = "filepath"

    def get_dtype(self, is_external: bool) -> str:
        """Filepath is external only."""
        if not is_external:
            raise DataJointError("<filepath> requires @store")
        return "json"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> dict:
        """
        Store path reference as JSON metadata.

        Args:
            value: Relative path within the store (str).
            key: Primary key values (unused).
            store_name: Store where the file exists.

        Returns:
            Metadata dict: {path, store}
        """
        from datetime import datetime, timezone

        from .content_registry import get_store_backend

        path = str(value)

        # Optionally verify file exists
        backend = get_store_backend(store_name)
        if not backend.exists(path):
            raise FileNotFoundError(f"File not found in store '{store_name or 'default'}': {path}")

        # Get file info
        try:
            size = backend.size(path)
        except Exception:
            size = None

        return {
            "path": path,
            "store": store_name,
            "size": size,
            "is_dir": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def decode(self, stored: dict, *, key: dict | None = None) -> Any:
        """
        Create ObjectRef handle for lazy access.

        Args:
            stored: Metadata dict with path and store.
            key: Primary key values (unused).

        Returns:
            ObjectRef for accessing the file.
        """
        from .objectref import ObjectRef
        from .content_registry import get_store_backend

        store_name = stored.get("store")
        backend = get_store_backend(store_name)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate that value is a path string or Path object."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<filepath> expects a path string or Path, got {type(value).__name__}")


# Backward compatibility alias
FilepathType = FilepathCodec
