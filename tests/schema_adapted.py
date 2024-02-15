import datajoint as dj
import inspect
import networkx as nx
import json
from pathlib import Path
import tempfile


class GraphAdapter(dj.AttributeAdapter):
    attribute_type = "longblob"  # this is how the attribute will be declared

    @staticmethod
    def get(obj):
        # convert edge list into a graph
        return nx.Graph(obj)

    @staticmethod
    def put(obj):
        # convert graph object into an edge list
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)


class LayoutToFilepath(dj.AttributeAdapter):
    """
    An adapted data type that saves a graph layout into fixed filepath
    """

    attribute_type = "filepath@repo-s3"

    @staticmethod
    def get(path):
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def put(layout):
        path = Path(dj.config["stores"]["repo-s3"]["stage"], "layout.json")
        with open(str(path), "w") as f:
            json.dump(layout, f)
        return path


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
