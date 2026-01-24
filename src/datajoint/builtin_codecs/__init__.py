"""
Built-in DataJoint codecs.

This package defines the standard codecs that ship with DataJoint.
These serve as both useful built-in codecs and as examples for users who
want to create their own custom codecs.

Built-in Codecs:
    - ``<blob>``: Serialize Python objects (in-table storage)
    - ``<blob@>``: Serialize Python objects (external with hash-addressed dedup)
    - ``<attach>``: File attachment (in-table storage)
    - ``<attach@>``: File attachment (external with hash-addressed dedup)
    - ``<hash@>``: Hash-addressed storage with MD5 deduplication (external only)
    - ``<object@>``: Schema-addressed storage for files/folders (external only)
    - ``<npy@>``: Store numpy arrays as portable .npy files (external only)
    - ``<filepath@store>``: Reference to existing file in store (external only)

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

from .attach import AttachCodec
from .blob import BlobCodec
from .filepath import FilepathCodec
from .hash import HashCodec
from .npy import NpyCodec, NpyRef
from .object import ObjectCodec
from .schema import SchemaCodec

__all__ = [
    "BlobCodec",
    "HashCodec",
    "SchemaCodec",
    "ObjectCodec",
    "AttachCodec",
    "FilepathCodec",
    "NpyCodec",
    "NpyRef",
]
