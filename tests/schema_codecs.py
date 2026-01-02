import inspect

import networkx as nx

import datajoint as dj


class GraphCodec(dj.Codec):
    """Custom codec for storing NetworkX graphs as edge lists."""

    name = "graph"

    def get_dtype(self, is_external: bool) -> str:
        """Chain to blob for serialization."""
        return "<blob>"

    def encode(self, obj, *, key=None, store_name=None):
        """Convert graph object into an edge list."""
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)

    def decode(self, stored, *, key=None):
        """Convert edge list into a graph."""
        return nx.Graph(stored)


class LayoutCodec(dj.Codec):
    """Custom codec that saves a graph layout as serialized blob."""

    name = "layout"

    def get_dtype(self, is_external: bool) -> str:
        """Chain to blob for serialization."""
        return "<blob>"

    def encode(self, layout, *, key=None, store_name=None):
        """Serialize layout dict."""
        return layout  # blob handles serialization

    def decode(self, stored, *, key=None):
        """Deserialize layout dict."""
        return stored  # blob handles deserialization


class Connectivity(dj.Manual):
    definition = """
    connid : int
    ---
    conn_graph = null : <graph>
    """


class Layout(dj.Manual):
    definition = """
    # stores graph layout
    -> Connectivity
    ---
    layout: <layout>
    """


LOCALS_CODECS = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_CODECS)
