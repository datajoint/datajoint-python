"""
Hash-addressed storage codec with MD5-based deduplication.
"""

from __future__ import annotations

from typing import Any

from ..codecs import Codec
from ..errors import DataJointError


class HashCodec(Codec):
    """
    Hash-addressed storage with content-addressed deduplication.

    The ``<hash@>`` codec stores raw bytes using hash-addressed storage.
    Data is identified by a 26-character Base32-encoded MD5 digest of the
    content, and stored at ``{hash_prefix}/{schema}/{hash}`` (``hash_prefix``
    defaults to ``_hash``). Stores with subfolding configured insert
    additional path segments: ``{hash_prefix}/{schema}/{fold1}/{fold2}/{hash}``.

    The database column stores JSON metadata: ``{hash, store, size}``.
    Duplicate content is automatically deduplicated across all tables.

    Deletion: Requires garbage collection via ``dj.gc.GarbageCollector``.

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
        """Hash storage is in-store only."""
        if not is_store:
            raise DataJointError("<hash> requires @ (in-store storage only)")
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
        from ..hash_registry import put_hash

        schema_name = (key or {}).get("_schema", "unknown")
        config = (key or {}).get("_config")
        return put_hash(value, schema_name=schema_name, store_name=store_name, config=config)

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
        from ..hash_registry import get_hash

        config = (key or {}).get("_config")
        return get_hash(stored, config=config)

    def validate(self, value: Any) -> None:
        """Validate that value is bytes."""
        if not isinstance(value, bytes):
            raise TypeError(f"<hash> expects bytes, got {type(value).__name__}")
