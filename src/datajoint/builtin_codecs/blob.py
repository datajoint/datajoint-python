"""
Blob codec for Python object serialization.
"""

from __future__ import annotations

from typing import Any

from ..codecs import Codec


class BlobCodec(Codec):
    """
    Serialize Python objects using DataJoint's blob format.

    The ``<blob>`` codec handles serialization of arbitrary Python objects
    including NumPy arrays, dictionaries, lists, datetime objects, and UUIDs.

    Supports both in-table and in-store storage:
    - ``<blob>``: Stored in database (bytes → LONGBLOB)
    - ``<blob@>``: Stored in object store via ``<hash@>`` with deduplication
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
            small_result : <blob>       # in-table (in database)
            large_result : <blob@>      # in-store (default store)
            archive : <blob@cold>       # in-store (specific store)
            '''

        # Insert any serializable object
        table.insert1({'data_id': 1, 'small_result': {'scores': [0.9, 0.8]}})
    """

    name = "blob"

    def get_dtype(self, is_store: bool) -> str:
        """Return bytes for in-table, <hash> for in-store storage."""
        return "<hash>" if is_store else "bytes"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
        """Serialize a Python object to DataJoint's blob format."""
        from .. import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None, context: dict | None = None) -> Any:
        """
        Deserialize blob bytes back to a Python object.

        A compressed blob whose recorded uncompressed size exceeds
        ``blob.max_decompressed_size`` is refused before it is decompressed.
        """
        from .. import blob

        config = self._codec_config(key, context)
        if config is None:
            from ..settings import config  # type: ignore[assignment]
        assert config is not None
        return blob.unpack(stored, squeeze=False, max_decompressed_size=config["blob.max_decompressed_size"])
