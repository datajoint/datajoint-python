"""
NumPy array codec using .npy format.
"""

from __future__ import annotations

from typing import Any

from ..errors import DataJointError
from .schema import SchemaCodec


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
        config = (key or {}).get("_config")

        # Build schema-addressed storage path
        path, _ = self._build_path(schema, table, field, primary_key, ext=".npy", store_name=store_name, config=config)

        # Serialize to .npy format
        buffer = io.BytesIO()
        np.save(buffer, value, allow_pickle=False)
        npy_bytes = buffer.getvalue()

        # Upload to storage using inherited helper
        backend = self._get_backend(store_name, config=config)
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
        config = (key or {}).get("_config")
        backend = self._get_backend(stored.get("store"), config=config)
        return NpyRef(stored, backend)
