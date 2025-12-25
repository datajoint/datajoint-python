"""
Built-in DataJoint attribute types.

This module defines the standard AttributeTypes that ship with DataJoint.
These serve as both useful built-in types and as examples for users who
want to create their own custom types.

Built-in Types:
    - ``<djblob>``: Serialize Python objects to DataJoint's blob format (internal storage)
    - ``<content>``: Content-addressed storage with SHA256 deduplication
    - ``<xblob>``: External serialized blobs using content-addressed storage

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
