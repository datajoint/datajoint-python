import inspect

import networkx as nx

import datajoint as dj


@dj.register_type
class GraphType(dj.AttributeType):
    """Custom type for storing NetworkX graphs as edge lists."""

    type_name = "graph"
    dtype = "<djblob>"  # Use djblob for proper serialization

    def encode(self, obj, *, key=None):
        """Convert graph object into an edge list."""
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)

    def decode(self, stored, *, key=None):
        """Convert edge list into a graph."""
        return nx.Graph(stored)


@dj.register_type
class LayoutToFilepathType(dj.AttributeType):
    """Custom type that saves a graph layout as serialized JSON blob."""

    type_name = "layout_to_filepath"
    dtype = "<djblob>"  # Use djblob for serialization

    def encode(self, layout, *, key=None):
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
