import inspect

import networkx as nx

import datajoint as dj


class GraphType(dj.Codec):
    """Custom codec for storing NetworkX graphs as edge lists."""

    name = "graph"

    def get_dtype(self, is_external: bool) -> str:
        """Chain to djblob for serialization."""
        return "<blob>"

    def encode(self, obj, *, key=None, store_name=None):
        """Convert graph object into an edge list."""
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)

    def decode(self, stored, *, key=None):
        """Convert edge list into a graph."""
        return nx.Graph(stored)


class LayoutToFilepathType(dj.Codec):
    """Custom codec that saves a graph layout as serialized JSON blob."""

    name = "layout_to_filepath"

    def get_dtype(self, is_external: bool) -> str:
        """Chain to djblob for serialization."""
        return "<blob>"

    def encode(self, layout, *, key=None, store_name=None):
        """Serialize layout dict."""
        return layout  # djblob handles serialization

    def decode(self, stored, *, key=None):
        """Deserialize layout dict."""
        return stored  # djblob handles deserialization


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
    layout: <layout_to_filepath>
    """


LOCALS_ADAPTED = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_ADAPTED)
