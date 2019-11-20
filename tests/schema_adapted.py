import datajoint as dj
import networkx as nx
from pathlib import Path
import tempfile
from datajoint import errors

from . import PREFIX, CONN_INFO, S3_CONN_INFO

stores_config = {
    'repo_s3': dict(
        S3_CONN_INFO,
        protocol='s3',
        location='adapted/repo',
        stage=tempfile.mkdtemp())
}
dj.config['stores'] = stores_config

schema_name = PREFIX + '_test_custom_datatype'
schema = dj.schema(schema_name, connection=dj.conn(**CONN_INFO))


errors._switch_adapted_types(True)  # enable adapted types for testing only


class GraphAdapter(dj.AttributeAdapter):

    attribute_type = 'longblob'  # this is how the attribute will be declared

    @staticmethod
    def get(obj):
        # convert edge list into a graph
        return nx.Graph(obj)

    @staticmethod
    def put(obj):
        # convert graph object into an edge list
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)


# instantiate for use as a datajoint type
graph = GraphAdapter()


@schema
class Connectivity(dj.Manual):
    definition = """
    connid : int
    ---
    conn_graph = null : <graph>
    """

errors._switch_filepath_types(True)


class Filepath2GraphAdapter(dj.AttributeAdapter):

    attribute_type = 'filepath@repo_s3'

    @staticmethod
    def get(obj):
        s = open(obj, "r").read()
        return nx.spring_layout(
            nx.lollipop_graph(4, 2), seed=int(s))

    @staticmethod
    def put(obj):
        path = Path(
            dj.config['stores']['repo_s3']['stage'], 'sample.txt')

        f = open(path, "w")
        f.write(str(obj*obj))
        f.close()

        return path


file2graph = Filepath2GraphAdapter()


@schema
class Position(dj.Manual):
    definition = """
    pos_id : int
    ---
    seed_root: <file2graph>
    """

errors._switch_filepath_types(False)
errors._switch_adapted_types(False)  # disable again
