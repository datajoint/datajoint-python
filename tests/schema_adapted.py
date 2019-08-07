import datajoint as dj
import networkx as nx

from . import PREFIX, CONN_INFO

schema_name = PREFIX + '_test_custom_datatype'
schema = dj.schema(schema_name, connection=dj.conn(**CONN_INFO))


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
    conn_graph : <graph>
    """
