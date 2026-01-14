"""
Built-in DataJoint codecs.

This module defines the standard codecs that ship with DataJoint.
These serve as both useful built-in codecs and as examples for users who
want to create their own custom codecs.

Built-in Codecs:
    - ``<blob>``: Serialize Python objects (internal) or external with dedup
    - ``<hash>``: Hash-addressed storage with SHA256 deduplication
    - ``<object>``: Schema-addressed storage for files/folders (Zarr, HDF5)
    - ``<attach>``: File attachment (internal) or external with dedup
    - ``<filepath@store>``: Reference to existing file in store
    - ``<npy@>``: Store numpy arrays as portable .npy files (external only)

Example - Creating a Custom Codec:
    Here's how to define your own codec, modeled after the built-in codecs::

        import datajoint as dj
        import networkx as nx

        class GraphCodec(dj.Codec):
            '''Store NetworkX graphs as edge lists.'''

            name = "graph"  # Use as <graph> in definitions

            def get_dtype(self, is_store: bool) -> str:
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

    def get_dtype(self, is_store: bool) -> str:
        """Return bytes for internal, <hash> for external storage."""
        return "<hash>" if is_store else "bytes"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """Serialize a Python object to DataJoint's blob format."""
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """Deserialize blob bytes back to a Python object."""
        from . import blob

        return blob.unpack(stored, squeeze=False)


# =============================================================================
# Hash-Addressed Storage Codec
# =============================================================================


class HashCodec(Codec):
    """
    Hash-addressed storage with SHA256 deduplication.

    The ``<hash@>`` codec stores raw bytes using hash-addressed storage.
    Data is identified by its SHA256 hash and stored in a hierarchical directory:
    ``_hash/{hash[:2]}/{hash[2:4]}/{hash}``

    The database column stores JSON metadata: ``{hash, store, size}``.
    Duplicate content is automatically deduplicated across all tables.

    Deletion: Requires garbage collection via ``dj.gc.collect()``.

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

    See Also
    --------
    datajoint.gc : Garbage collection for orphaned storage.
    """

    name = "hash"

    def get_dtype(self, is_store: bool) -> str:
        """Hash storage is external only."""
        if not is_store:
            raise DataJointError("<hash> requires @ (external storage only)")
        return "json"

    def encode(self, value: bytes, *, key: dict | None = None, store_name: str | None = None) -> dict:
        """
        Store content and return metadata.

        Parameters
        ----------
        value : bytes
            Raw bytes to store.
        key : dict, optional
            Context dict with ``_schema`` for path isolation.
        store_name : str, optional
            Store to use. If None, uses default store.

        Returns
        -------
        dict
            Metadata dict: ``{hash, path, schema, store, size}``.
        """
        from .hash_registry import put_hash

        schema_name = (key or {}).get("_schema", "unknown")
        return put_hash(value, schema_name=schema_name, store_name=store_name)

    def decode(self, stored: dict, *, key: dict | None = None) -> bytes:
        """
        Retrieve content using stored metadata.

        Parameters
        ----------
        stored : dict
            Metadata dict with ``'path'``, ``'hash'``, and optionally ``'store'``.
        key : dict, optional
            Context dict (unused - path is in metadata).

        Returns
        -------
        bytes
            Original bytes.
        """
        from .hash_registry import get_hash

        return get_hash(stored)

    def validate(self, value: Any) -> None:
        """Validate that value is bytes."""
        if not isinstance(value, bytes):
            raise TypeError(f"<hash> expects bytes, got {type(value).__name__}")


# =============================================================================
# Schema-Addressed Storage Base Class
# =============================================================================


class SchemaCodec(Codec, register=False):
    """
    Abstract base class for schema-addressed codecs.

    Schema-addressed storage is an OAS (Object-Augmented Schema) addressing
    scheme where paths mirror the database schema structure:
    ``{schema}/{table}/{pk}/{attribute}``. This creates a browsable
    organization in object storage that reflects the schema design.

    Subclasses must implement:
        - ``name``: Codec name for ``<name@>`` syntax
        - ``encode()``: Serialize and upload content
        - ``decode()``: Create lazy reference from metadata
        - ``validate()``: Validate input values

    Helper Methods:
        - ``_extract_context()``: Parse key dict into schema/table/field/pk
        - ``_build_path()``: Construct storage path from context
        - ``_get_backend()``: Get storage backend by name

    Comparison with Hash-addressed:
        - **Schema-addressed** (this): Path from schema structure, no dedup
        - **Hash-addressed**: Path from content hash, automatic dedup

    Example::

        class MyCodec(SchemaCodec):
            name = "my"

            def encode(self, value, *, key=None, store_name=None):
                schema, table, field, pk = self._extract_context(key)
                path, _ = self._build_path(schema, table, field, pk, ext=".dat")
                backend = self._get_backend(store_name)
                backend.put_buffer(serialize(value), path)
                return {"path": path, "store": store_name, ...}

            def decode(self, stored, *, key=None):
                backend = self._get_backend(stored.get("store"))
                return MyRef(stored, backend)

    See Also
    --------
    HashCodec : Hash-addressed storage with content deduplication.
    ObjectCodec : Schema-addressed storage for files/folders.
    NpyCodec : Schema-addressed storage for numpy arrays.
    """

    def get_dtype(self, is_store: bool) -> str:
        """
        Return storage dtype. Schema-addressed codecs require @ modifier.

        Parameters
        ----------
        is_store : bool
            Must be True for schema-addressed codecs.

        Returns
        -------
        str
            "json" for metadata storage.

        Raises
        ------
        DataJointError
            If is_store is False (@ modifier missing).
        """
        if not is_store:
            raise DataJointError(f"<{self.name}> requires @ (store only)")
        return "json"

    def _extract_context(self, key: dict | None) -> tuple[str, str, str, dict]:
        """
        Extract schema, table, field, and primary key from context dict.

        Parameters
        ----------
        key : dict or None
            Context dict with ``_schema``, ``_table``, ``_field``,
            and primary key values.

        Returns
        -------
        tuple[str, str, str, dict]
            ``(schema, table, field, primary_key)``
        """
        key = dict(key) if key else {}
        schema = key.pop("_schema", "unknown")
        table = key.pop("_table", "unknown")
        field = key.pop("_field", "data")
        primary_key = {k: v for k, v in key.items() if not k.startswith("_")}
        return schema, table, field, primary_key

    def _build_path(
        self,
        schema: str,
        table: str,
        field: str,
        primary_key: dict,
        ext: str | None = None,
        store_name: str | None = None,
    ) -> tuple[str, str]:
        """
        Build schema-addressed storage path.

        Constructs a path that mirrors the database schema structure:
        ``{schema}/{table}/{pk_values}/{field}{ext}``

        Supports partitioning if configured in the store.

        Parameters
        ----------
        schema : str
            Schema name.
        table : str
            Table name.
        field : str
            Field/attribute name.
        primary_key : dict
            Primary key values.
        ext : str, optional
            File extension (e.g., ".npy", ".zarr").
        store_name : str, optional
            Store name for retrieving partition configuration.

        Returns
        -------
        tuple[str, str]
            ``(path, token)`` where path is the storage path and token
            is a unique identifier.
        """
        from .storage import build_object_path
        from . import config

        # Get store configuration for partition_pattern and token_length
        spec = config.get_store_spec(store_name)
        partition_pattern = spec.get("partition_pattern")
        token_length = spec.get("token_length", 8)

        return build_object_path(
            schema=schema,
            table=table,
            field=field,
            primary_key=primary_key,
            ext=ext,
            partition_pattern=partition_pattern,
            token_length=token_length,
        )

    def _get_backend(self, store_name: str | None = None):
        """
        Get storage backend by name.

        Parameters
        ----------
        store_name : str, optional
            Store name. If None, returns default store.

        Returns
        -------
        StorageBackend
            Storage backend instance.
        """
        from .hash_registry import get_store_backend

        return get_store_backend(store_name)


# =============================================================================
# Object Codec (Schema-Addressed Files/Folders)
# =============================================================================


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
        path, token = self._build_path(schema, table, field, primary_key, ext=ext, store_name=store_name)

        # Get storage backend using inherited helper
        backend = self._get_backend(store_name)

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
        from .objectref import ObjectRef

        backend = self._get_backend(stored.get("store"))
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

    def get_dtype(self, is_store: bool) -> str:
        """Return bytes for internal, <hash> for external storage."""
        return "<hash>" if is_store else "bytes"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """
        Read file and encode as filename + contents.

        Parameters
        ----------
        value : str or Path
            Path to file.
        key : dict, optional
            Primary key values (unused).
        store_name : str, optional
            Unused for internal storage.

        Returns
        -------
        bytes
            Filename (UTF-8) + null byte + file contents.
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

        Parameters
        ----------
        stored : bytes
            Blob containing filename + null + contents.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        str
            Path to extracted file.
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

    This codec gives users maximum freedom in organizing their files while
    reusing DataJoint's store configuration. Files can be placed anywhere
    in the store EXCEPT the reserved ``_hash/`` and ``_schema/`` sections
    which are managed by DataJoint.

    This is useful when:
    - Files are managed externally (e.g., by acquisition software)
    - Files are too large to copy
    - You want to reference shared datasets
    - You need custom directory structures

    Example::

        @schema
        class Recordings(dj.Manual):
            definition = '''
            recording_id : int
            ---
            raw_data : <filepath@acquisition>
            '''

        # Reference an existing file (no copy)
        # Path is relative to store location
        table.insert1({'recording_id': 1, 'raw_data': 'subject01/session001/data.bin'})

        # Fetch returns ObjectRef for lazy access
        ref = (table & 'recording_id=1').fetch1('raw_data')
        ref.read()      # Read file content
        ref.download()  # Download to local path

    Storage Format:
        JSON metadata: ``{path, store, size, timestamp}``

    Reserved Sections:
        Paths cannot start with ``_hash/`` or ``_schema/`` - these are managed by DataJoint.

    Warning:
        The file must exist in the store at the specified path.
        DataJoint does not manage the lifecycle of referenced files.
    """

    name = "filepath"

    def get_dtype(self, is_store: bool) -> str:
        """Filepath is external only."""
        if not is_store:
            raise DataJointError(
                "<filepath> requires @ symbol. Use <filepath@> for default store " "or <filepath@store> to specify store."
            )
        return "json"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> dict:
        """
        Store path reference as JSON metadata.

        Parameters
        ----------
        value : str
            Relative path within the store. Cannot use reserved sections (_hash/, _schema/).
        key : dict, optional
            Primary key values (unused).
        store_name : str, optional
            Store where the file exists.

        Returns
        -------
        dict
            Metadata dict: ``{path, store}``.

        Raises
        ------
        ValueError
            If path uses reserved sections (_hash/ or _schema/).
        FileNotFoundError
            If file does not exist in the store.
        """
        from datetime import datetime, timezone

        from . import config
        from .hash_registry import get_store_backend

        path = str(value)

        # Get store spec to check prefix configuration
        # Use filepath_default if no store specified (filepath is not part of OAS)
        spec = config.get_store_spec(store_name, use_filepath_default=True)

        # Validate path doesn't use reserved sections (hash and schema)
        path_normalized = path.lstrip("/")
        reserved_prefixes = []

        hash_prefix = spec.get("hash_prefix")
        if hash_prefix:
            reserved_prefixes.append(("hash_prefix", hash_prefix))

        schema_prefix = spec.get("schema_prefix")
        if schema_prefix:
            reserved_prefixes.append(("schema_prefix", schema_prefix))

        # Check if path starts with any reserved prefix
        for prefix_name, prefix_value in reserved_prefixes:
            prefix_normalized = prefix_value.strip("/") + "/"
            if path_normalized.startswith(prefix_normalized):
                raise ValueError(
                    f"<filepath@> cannot use reserved section '{prefix_value}' ({prefix_name}). "
                    f"This section is managed by DataJoint. "
                    f"Got path: {path}"
                )

        # If filepath_prefix is configured, enforce it
        filepath_prefix = spec.get("filepath_prefix")
        if filepath_prefix:
            filepath_prefix_normalized = filepath_prefix.strip("/") + "/"
            if not path_normalized.startswith(filepath_prefix_normalized):
                raise ValueError(f"<filepath@> must use prefix '{filepath_prefix}' (filepath_prefix). " f"Got path: {path}")

        # Verify file exists
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

        Parameters
        ----------
        stored : dict
            Metadata dict with path and store.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        ObjectRef
            Handle for accessing the file.
        """
        from .objectref import ObjectRef
        from .hash_registry import get_store_backend

        store_name = stored.get("store")
        backend = get_store_backend(store_name)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate that value is a path string or Path object."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<filepath> expects a path string or Path, got {type(value).__name__}")


# =============================================================================
# NumPy Array Codec (.npy format)
# =============================================================================


class NpyRef:
    """
    Lazy reference to a numpy array stored as a .npy file.

    This class provides metadata access without I/O and transparent
    integration with numpy operations via the ``__array__`` protocol.

    Attributes
    ----------
    shape : tuple[int, ...]
        Array shape (from metadata, no I/O).
    dtype : numpy.dtype
        Array dtype (from metadata, no I/O).
    path : str
        Storage path within the store.
    store : str or None
        Store name (None for default).

    Examples
    --------
    Metadata access without download::

        ref = (Recording & key).fetch1('waveform')
        print(ref.shape)  # (1000, 32) - no download
        print(ref.dtype)  # float64 - no download

    Explicit loading::

        arr = ref.load()  # Downloads and returns np.ndarray

    Transparent numpy integration::

        # These all trigger automatic download via __array__
        result = ref + 1
        result = np.mean(ref)
        result = ref[0:100]  # Slicing works too
    """

    __slots__ = ("_meta", "_backend", "_cached")

    def __init__(self, metadata: dict, backend: Any):
        """
        Initialize NpyRef from metadata and storage backend.

        Parameters
        ----------
        metadata : dict
            JSON metadata containing path, store, dtype, shape.
        backend : StorageBackend
            Storage backend for file operations.
        """
        self._meta = metadata
        self._backend = backend
        self._cached = None

    @property
    def shape(self) -> tuple:
        """Array shape (no I/O required)."""
        return tuple(self._meta["shape"])

    @property
    def dtype(self):
        """Array dtype (no I/O required)."""
        import numpy as np

        return np.dtype(self._meta["dtype"])

    @property
    def ndim(self) -> int:
        """Number of dimensions (no I/O required)."""
        return len(self._meta["shape"])

    @property
    def size(self) -> int:
        """Total number of elements (no I/O required)."""
        import math

        return math.prod(self._meta["shape"])

    @property
    def nbytes(self) -> int:
        """Total bytes (estimated from shape and dtype, no I/O required)."""
        return self.size * self.dtype.itemsize

    @property
    def path(self) -> str:
        """Storage path within the store."""
        return self._meta["path"]

    @property
    def store(self) -> str | None:
        """Store name (None for default store)."""
        return self._meta.get("store")

    @property
    def is_loaded(self) -> bool:
        """True if array data has been downloaded and cached."""
        return self._cached is not None

    def load(self, mmap_mode=None):
        """
        Download and return the array.

        Parameters
        ----------
        mmap_mode : str, optional
            Memory-map mode for lazy, random-access loading of large arrays:

            - ``'r'``: Read-only
            - ``'r+'``: Read-write
            - ``'c'``: Copy-on-write (changes not saved to disk)

            If None (default), loads entire array into memory.

        Returns
        -------
        numpy.ndarray or numpy.memmap
            The array data. Returns ``numpy.memmap`` if mmap_mode is specified.

        Notes
        -----
        When ``mmap_mode`` is None, the array is cached after first load.

        For local filesystem stores, memory mapping accesses the file directly
        with no download. For remote stores (S3, etc.), the file is downloaded
        to a local cache (``{tempdir}/datajoint_mmap/``) before memory mapping.

        Examples
        --------
        Standard loading::

            arr = ref.load()  # Loads entire array into memory

        Memory-mapped for random access to large arrays::

            arr = ref.load(mmap_mode='r')
            slice = arr[1000:2000]  # Only reads the needed portion from disk
        """
        import io

        import numpy as np

        if mmap_mode is None:
            # Standard loading with caching
            if self._cached is None:
                buffer = self._backend.get_buffer(self.path)
                self._cached = np.load(io.BytesIO(buffer), allow_pickle=False)
            return self._cached
        else:
            # Memory-mapped loading
            if self._backend.protocol == "file":
                # Local filesystem - mmap directly, no download needed
                local_path = self._backend._full_path(self.path)
                return np.load(local_path, mmap_mode=mmap_mode, allow_pickle=False)
            else:
                # Remote storage - download to local cache first
                import hashlib
                import tempfile
                from pathlib import Path

                path_hash = hashlib.md5(self.path.encode()).hexdigest()[:12]
                cache_dir = Path(tempfile.gettempdir()) / "datajoint_mmap"
                cache_dir.mkdir(exist_ok=True)
                cache_path = cache_dir / f"{path_hash}.npy"

                if not cache_path.exists():
                    buffer = self._backend.get_buffer(self.path)
                    cache_path.write_bytes(buffer)

                return np.load(str(cache_path), mmap_mode=mmap_mode, allow_pickle=False)

    def __array__(self, dtype=None):
        """
        NumPy array protocol for transparent integration.

        This method is called automatically when the NpyRef is used
        in numpy operations (arithmetic, ufuncs, etc.).

        Parameters
        ----------
        dtype : numpy.dtype, optional
            Desired output dtype.

        Returns
        -------
        numpy.ndarray
            The array data, optionally cast to dtype.
        """
        arr = self.load()
        if dtype is not None:
            return arr.astype(dtype)
        return arr

    def __getitem__(self, key):
        """Support indexing/slicing by loading then indexing."""
        return self.load()[key]

    def __len__(self) -> int:
        """Length of first dimension."""
        if not self._meta["shape"]:
            raise TypeError("len() of 0-dimensional array")
        return self._meta["shape"][0]

    def __repr__(self) -> str:
        status = "loaded" if self.is_loaded else "not loaded"
        return f"NpyRef(shape={self.shape}, dtype={self.dtype}, {status})"

    def __str__(self) -> str:
        return repr(self)


class NpyCodec(SchemaCodec):
    """
    Schema-addressed storage for numpy arrays as .npy files.

    The ``<npy@>`` codec stores numpy arrays as standard ``.npy`` files
    using schema-addressed paths: ``{schema}/{table}/{pk}/{attribute}.npy``.
    Arrays are fetched lazily via ``NpyRef``, which provides metadata access
    without I/O and transparent numpy integration via ``__array__``.

    Store only - requires ``@`` modifier.

    Key Features:
        - **Portable**: Standard .npy format readable by numpy, MATLAB, etc.
        - **Lazy loading**: Metadata (shape, dtype) available without download
        - **Transparent**: Use in numpy operations triggers automatic download
        - **Safe bulk fetch**: Fetching many rows doesn't download until needed
        - **Schema-addressed**: Browsable paths that mirror database structure

    Example::

        @schema
        class Recording(dj.Manual):
            definition = '''
            recording_id : int
            ---
            waveform : <npy@>           # default store
            spectrogram : <npy@archive>  # specific store
            '''

        # Insert - just pass the array
        Recording.insert1({
            'recording_id': 1,
            'waveform': np.random.randn(1000, 32),
        })

        # Fetch - returns NpyRef (lazy)
        ref = (Recording & 'recording_id=1').fetch1('waveform')
        ref.shape   # (1000, 32) - no download
        ref.dtype   # float64 - no download

        # Use in numpy ops - downloads automatically
        result = np.mean(ref, axis=0)

        # Or load explicitly
        arr = ref.load()

    Storage Details:
        - File format: NumPy .npy (version 1.0 or 2.0)
        - Path: ``{schema}/{table}/{pk}/{attribute}.npy``
        - Database column: JSON with ``{path, store, dtype, shape}``

    Deletion: Requires garbage collection via ``dj.gc.collect()``.

    See Also
    --------
    datajoint.gc : Garbage collection for orphaned storage.
    NpyRef : The lazy array reference returned on fetch.
    SchemaCodec : Base class for schema-addressed codecs.
    ObjectCodec : Schema-addressed storage for files/folders.
    """

    name = "npy"

    def validate(self, value: Any) -> None:
        """
        Validate that value is a numpy array suitable for .npy storage.

        Parameters
        ----------
        value : Any
            Value to validate.

        Raises
        ------
        DataJointError
            If value is not a numpy array or has object dtype.
        """
        import numpy as np

        if not isinstance(value, np.ndarray):
            raise DataJointError(f"<npy> requires numpy.ndarray, got {type(value).__name__}")
        if value.dtype == object:
            raise DataJointError("<npy> does not support object dtype arrays")

    def encode(
        self,
        value: Any,
        *,
        key: dict | None = None,
        store_name: str | None = None,
    ) -> dict:
        """
        Serialize array to .npy and upload to storage.

        Parameters
        ----------
        value : numpy.ndarray
            Array to store.
        key : dict, optional
            Context dict with ``_schema``, ``_table``, ``_field``,
            and primary key values for path construction.
        store_name : str, optional
            Target store. If None, uses default store.

        Returns
        -------
        dict
            JSON metadata: ``{path, store, dtype, shape}``.
        """
        import io

        import numpy as np

        # Extract context using inherited helper
        schema, table, field, primary_key = self._extract_context(key)

        # Build schema-addressed storage path
        path, _ = self._build_path(schema, table, field, primary_key, ext=".npy", store_name=store_name)

        # Serialize to .npy format
        buffer = io.BytesIO()
        np.save(buffer, value, allow_pickle=False)
        npy_bytes = buffer.getvalue()

        # Upload to storage using inherited helper
        backend = self._get_backend(store_name)
        backend.put_buffer(npy_bytes, path)

        # Return metadata (includes numpy-specific shape/dtype)
        return {
            "path": path,
            "store": store_name,
            "dtype": str(value.dtype),
            "shape": list(value.shape),
        }

    def decode(self, stored: dict, *, key: dict | None = None) -> NpyRef:
        """
        Create lazy NpyRef from stored metadata.

        Parameters
        ----------
        stored : dict
            JSON metadata from database.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        NpyRef
            Lazy array reference with metadata access and numpy integration.
        """
        backend = self._get_backend(stored.get("store"))
        return NpyRef(stored, backend)
