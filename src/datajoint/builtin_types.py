"""
Built-in DataJoint attribute types.

This module defines the standard AttributeTypes that ship with DataJoint.
These serve as both useful built-in types and as examples for users who
want to create their own custom types.

Built-in Types:
    - ``<djblob>``: Serialize Python objects to DataJoint's blob format (internal storage)
    - ``<content>``: Content-addressed storage with SHA256 deduplication
    - ``<xblob>``: External serialized blobs using content-addressed storage
    - ``<object>``: Path-addressed storage for files/folders (Zarr, HDF5)
    - ``<attach>``: Internal file attachment stored in database
    - ``<xattach>``: External file attachment with deduplication
    - ``<filepath@store>``: Reference to existing file in store

Example - Creating a Custom Type:
    Here's how to define your own AttributeType, modeled after the built-in types::

        import datajoint as dj
        import networkx as nx

        @dj.register_type
        class GraphType(dj.AttributeType):
            '''Store NetworkX graphs as edge lists.'''

            type_name = "graph"      # Use as <graph> in definitions
            dtype = "<djblob>"       # Compose with djblob for serialization

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

from .attribute_type import AttributeType, register_type


# =============================================================================
# DJBlob Types - DataJoint's native serialization
# =============================================================================


@register_type
class DJBlobType(AttributeType):
    """
    Serialize Python objects using DataJoint's blob format.

    The ``<djblob>`` type handles serialization of arbitrary Python objects
    including NumPy arrays, dictionaries, lists, datetime objects, and UUIDs.
    Data is stored in a MySQL ``LONGBLOB`` column.

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
            results : <djblob>      # Serialized Python objects
            '''

        # Insert any serializable object
        table.insert1({'data_id': 1, 'results': {'scores': [0.9, 0.8], 'labels': ['a', 'b']}})

    Note:
        Plain ``longblob`` columns store raw bytes without serialization.
        Use ``<djblob>`` when you need automatic serialization.
    """

    type_name = "djblob"
    dtype = "longblob"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """Serialize a Python object to DataJoint's blob format."""
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """Deserialize blob bytes back to a Python object."""
        from . import blob

        return blob.unpack(stored, squeeze=False)


# =============================================================================
# Content-Addressed Storage Types
# =============================================================================


@register_type
class ContentType(AttributeType):
    """
    Content-addressed storage with SHA256 deduplication.

    The ``<content>`` type stores raw bytes using content-addressed storage.
    Data is identified by its SHA256 hash and stored in a hierarchical directory:
    ``_content/{hash[:2]}/{hash[2:4]}/{hash}``

    The database column stores JSON metadata: ``{hash, store, size}``.
    Duplicate content is automatically deduplicated.

    Example::

        @schema
        class RawContent(dj.Manual):
            definition = '''
            content_id : int
            ---
            data : <content@mystore>
            '''

        # Insert raw bytes
        table.insert1({'content_id': 1, 'data': b'raw binary content'})

    Note:
        This type accepts only ``bytes``. For Python objects, use ``<xblob>``.
        A store must be specified (e.g., ``<content@store>``) unless a default
        store is configured.
    """

    type_name = "content"
    dtype = "json"

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
            raise TypeError(f"<content> expects bytes, got {type(value).__name__}")


@register_type
class XBlobType(AttributeType):
    """
    External serialized blobs with content-addressed storage.

    The ``<xblob>`` type combines DataJoint's blob serialization with
    content-addressed storage. Objects are serialized, then stored externally
    with automatic deduplication.

    This is ideal for large objects (NumPy arrays, DataFrames) that may be
    duplicated across rows.

    Example::

        @schema
        class LargeArrays(dj.Manual):
            definition = '''
            array_id : int
            ---
            data : <xblob@mystore>
            '''

        import numpy as np
        table.insert1({'array_id': 1, 'data': np.random.rand(1000, 1000)})

    Type Composition:
        ``<xblob>`` composes with ``<content>``::

            Insert: object → blob.pack() → put_content() → JSON metadata
            Fetch:  JSON → get_content() → blob.unpack() → object

    Note:
        - For internal storage, use ``<djblob>``
        - For raw bytes without serialization, use ``<content>``
    """

    type_name = "xblob"
    dtype = "<content>"  # Composition: uses ContentType

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """Serialize object to bytes (passed to ContentType)."""
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """Deserialize bytes back to Python object."""
        from . import blob

        return blob.unpack(stored, squeeze=False)


# =============================================================================
# Path-Addressed Storage Types (OAS - Object-Augmented Schema)
# =============================================================================


@register_type
class ObjectType(AttributeType):
    """
    Path-addressed storage for files and folders.

    The ``<object>`` type provides managed file/folder storage where the path
    is derived from the primary key: ``{schema}/{table}/objects/{pk}/{field}_{token}.{ext}``

    Unlike ``<content>`` (content-addressed), each row has its own storage path,
    and content is deleted when the row is deleted. This is ideal for:

    - Zarr arrays (hierarchical chunked data)
    - HDF5 files
    - Complex multi-file outputs
    - Any content that shouldn't be deduplicated

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

            {store_root}/{schema}/{table}/objects/{pk}/{field}_{token}.ext

        The token ensures uniqueness even if content is replaced.

    Comparison with ``<content>``::

        | Aspect         | <object>          | <content>           |
        |----------------|-------------------|---------------------|
        | Addressing     | Path (by PK)      | Hash (by content)   |
        | Deduplication  | No                | Yes                 |
        | Deletion       | With row          | GC when unreferenced|
        | Use case       | Zarr, HDF5        | Blobs, attachments  |

    Note:
        A store must be specified (``<object@store>``) unless a default store
        is configured. Returns ``ObjectRef`` on fetch for lazy access.
    """

    type_name = "object"
    dtype = "json"

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
        source_path = None

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
                from .errors import DataJointError

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
        from .content_registry import get_store_backend
        from .objectref import ObjectRef

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


# =============================================================================
# File Attachment Types
# =============================================================================


@register_type
class AttachType(AttributeType):
    """
    Internal file attachment stored in database.

    The ``<attach>`` type stores a file directly in the database as a ``LONGBLOB``.
    The filename is preserved and the file is extracted to the configured
    download path on fetch.

    Example::

        @schema
        class Documents(dj.Manual):
            definition = '''
            doc_id : int
            ---
            report : <attach>
            '''

        # Insert a file
        table.insert1({'doc_id': 1, 'report': '/path/to/report.pdf'})

        # Fetch extracts to download_path and returns local path
        local_path = (table & 'doc_id=1').fetch1('report')

    Storage Format:
        The blob contains: ``filename\\0contents``
        - Filename (UTF-8 encoded) + null byte + raw file contents

    Note:
        - For large files, use ``<xattach>`` (external storage with deduplication)
        - For files that shouldn't be copied, use ``<filepath@store>``
    """

    type_name = "attach"
    dtype = "longblob"

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


@register_type
class XAttachType(AttributeType):
    """
    External file attachment with content-addressed storage.

    The ``<xattach>`` type stores files externally using content-addressed
    storage. Like ``<attach>``, the filename is preserved and the file is
    extracted on fetch. Unlike ``<attach>``, files are stored externally
    with automatic deduplication.

    Example::

        @schema
        class LargeDocuments(dj.Manual):
            definition = '''
            doc_id : int
            ---
            dataset : <xattach@mystore>
            '''

        # Insert a large file
        table.insert1({'doc_id': 1, 'dataset': '/path/to/large_file.h5'})

        # Fetch downloads and returns local path
        local_path = (table & 'doc_id=1').fetch1('dataset')

    Type Composition:
        ``<xattach>`` composes with ``<content>``::

            Insert: file → read + encode filename → put_content() → JSON
            Fetch:  JSON → get_content() → extract → local path

    Comparison::

        | Type       | Storage  | Deduplication | Best for           |
        |------------|----------|---------------|---------------------|
        | <attach>   | Database | No            | Small files (<16MB) |
        | <xattach>  | External | Yes           | Large files         |
    """

    type_name = "xattach"
    dtype = "<content>"  # Composition: uses ContentType

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """
        Read file and encode as filename + contents.

        Args:
            value: Path to file (str or Path).
            key: Primary key values (unused).
            store_name: Passed to ContentType for storage.

        Returns:
            Bytes: filename (UTF-8) + null byte + file contents
        """
        from pathlib import Path

        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"Attachment file not found: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"<xattach> does not support directories: {path}")

        filename = path.name
        contents = path.read_bytes()
        return filename.encode("utf-8") + b"\x00" + contents

    def decode(self, stored: bytes, *, key: dict | None = None) -> str:
        """
        Extract file to download path and return local path.

        Args:
            stored: Bytes containing filename + null + contents.
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
            raise TypeError(f"<xattach> expects a file path, got {type(value).__name__}")


# =============================================================================
# Filepath Reference Type
# =============================================================================


@register_type
class FilepathType(AttributeType):
    """
    Reference to existing file in configured store.

    The ``<filepath@store>`` type stores a reference to a file that already
    exists in the storage backend. Unlike ``<attach>`` or ``<object>``, no
    file copying occurs - only the path is recorded.

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

    type_name = "filepath"
    dtype = "json"

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
        from .content_registry import get_store_backend
        from .objectref import ObjectRef

        store_name = stored.get("store")
        backend = get_store_backend(store_name)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate that value is a path string or Path object."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<filepath> expects a path string or Path, got {type(value).__name__}")
