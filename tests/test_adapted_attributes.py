import datajoint as dj
import networkx as nx
from nose.tools import assert_true, assert_equal
from . import schema_adapted as adapted
from .schema_adapted import graph


def test_adapted_type():
    c = adapted.Connectivity()
    graphs = [nx.lollipop_graph(4, 2), nx.star_graph(5), nx.barbell_graph(3, 1), nx.cycle_graph(5)]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch('conn_graph', order_by='connid')
    for g1, g2 in zip(graphs, returned_graphs):
        assert_true(isinstance(g2, nx.Graph))
        assert_equal(len(g1.edges), len(g2.edges))
        assert_true(0 == len(nx.symmetric_difference(g1 ,g2).edges))
    c.delete()


# test spawned classes
local_schema = dj.schema(adapted.schema_name)
local_schema.spawn_missing_classes()


def test_adapted_module():
    c = Connectivity()
    graphs = [nx.lollipop_graph(4, 2), nx.star_graph(5), nx.barbell_graph(3, 1), nx.cycle_graph(5)]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch('conn_graph', order_by='connid')
    for g1, g2 in zip(graphs, returned_graphs):
        assert_true(isinstance(g2, nx.Graph))
        assert_equal(len(g1.edges), len(g2.edges))
        assert_true(0 == len(nx.symmetric_difference(g1 ,g2).edges))
    c.delete()
