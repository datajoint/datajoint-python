import inspect
import json
from pathlib import Path

import networkx as nx

import datajoint as dj


@dj.register_type
class GraphType(dj.AttributeType):
    """Custom type for storing NetworkX graphs as edge lists."""

    type_name = "graph"
    dtype = "longblob"

    def encode(self, obj, *, key=None):
        """Convert graph object into an edge list."""
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)

    def decode(self, stored, *, key=None):
        """Convert edge list into a graph."""
        return nx.Graph(stored)


@dj.register_type
class LayoutToFilepathType(dj.AttributeType):
    """Custom type that saves a graph layout to a filepath."""

    type_name = "layout_to_filepath"
    dtype = "filepath@repo-s3"

    def encode(self, layout, *, key=None):
        """Save layout to file and return path."""
        path = Path(dj.config["stores"]["repo-s3"]["stage"], "layout.json")
        with open(str(path), "w") as f:
            json.dump(layout, f)
        return path

    def decode(self, path, *, key=None):
        """Load layout from file."""
        with open(path, "r") as f:
            return json.load(f)


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
