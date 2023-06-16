import pytest, datajoint as dj
from datajoint import errors
import tempfile, networkx as nx, json
from pathlib import Path
from .. import PREFIX, S3_CONN_INFO, bucket, connection_test


@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + "_test_custom_datatype"), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def graph():
    class GraphAdapter(dj.AttributeAdapter):
        attribute_type = "longblob"

        @staticmethod
        def get(obj):
            return nx.Graph(obj)

        @staticmethod
        def put(obj):
            assert isinstance(obj, nx.Graph)
            return list(obj.edges)

    graph = GraphAdapter()
    yield graph


@pytest.fixture
def Connectivity(schema, graph):
    errors._switch_adapted_types(True)

    @schema
    class Connectivity(dj.Manual):
        definition = """
        connid : int
        ---
        conn_graph = null : <graph>
        """

    yield Connectivity
    Connectivity.drop()
    errors._switch_adapted_types(False)


@pytest.fixture
def stores():
    dj.config["stores"] = dict()
    yield dj.config["stores"]
    del dj.config["stores"]


@pytest.fixture
def store(schema, bucket, stores):
    with tempfile.TemporaryDirectory() as stage_dir:
        store_name = "repo-s3"
        dj.config["stores"][store_name] = dict(
            S3_CONN_INFO, protocol="s3", location="adapted/repo", stage=stage_dir
        )
        yield store_name
        schema.external[store_name].delete(delete_external_files=True)
        schema.connection.query(
            f"DROP TABLE IF EXISTS `{schema.database}`.`~external_{store_name}`"
        )
        del dj.config["stores"][store_name]


@pytest.fixture
def Layout(schema, store, Connectivity):
    errors._switch_adapted_types(True)
    errors._switch_filepath_types(True)

    class LayoutToFilepath(dj.AttributeAdapter):
        __doc__ = """
        An adapted data type that saves a graph layout into fixed filepath
        """
        attribute_type = f"filepath@{store}"

        @staticmethod
        def get(path):
            with open(path, "r") as f:
                return json.load(f)

        @staticmethod
        def put(layout):
            path = Path(dj.config["stores"][store]["stage"], "layout.json")
            with open(str(path), "w") as f:
                json.dump(layout, f)
            return path

    layout_to_filepath = LayoutToFilepath()

    @schema
    class Layout(dj.Manual):
        definition = """
        # stores graph layout
        -> Connectivity
        ---
        layout: <layout_to_filepath>
        """

    yield Layout
    Layout.drop()
    errors._switch_adapted_types(False)
    errors._switch_filepath_types(False)
