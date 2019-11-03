import datajoint as dj
import networkx as nx
from itertools import zip_longest
from nose.tools import assert_true, assert_equal
from . import schema_adapted as adapted
from .schema_adapted import graph, file2graph


def test_adapted_type():
    dj.errors._switch_adapted_types(True)
    c = adapted.Connectivity()
    graphs = [nx.lollipop_graph(4, 2), nx.star_graph(5), nx.barbell_graph(3, 1), nx.cycle_graph(5)]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch('conn_graph', order_by='connid')
    for g1, g2 in zip(graphs, returned_graphs):
        assert_true(isinstance(g2, nx.Graph))
        assert_equal(len(g1.edges), len(g2.edges))
        assert_true(0 == len(nx.symmetric_difference(g1, g2).edges))
    c.delete()
    dj.errors._switch_adapted_types(False)


def test_adapted_filepath_type():
    # https://github.com/datajoint/datajoint-python/issues/684
    dj.errors._switch_adapted_types(True)
    dj.errors._switch_filepath_types(True)
    c = adapted.Position()
    Position.insert([{'pos_id': 0, 'seed_root': 3}])
    result = (Position & 'pos_id=0').fetch1('seed_root')

    assert_true(isinstance(result, dict))
    assert_equal(0.3761992090175474, result[1][0])
    assert_true(6 == len(result))

    c.delete()
    dj.errors._switch_filepath_types(False)
    dj.errors._switch_adapted_types(False)

# test spawned classes
local_schema = dj.schema(adapted.schema_name)
local_schema.spawn_missing_classes()


def test_adapted_spawned():
    dj.errors._switch_adapted_types(True)
    c = Connectivity()  # a spawned class
    graphs = [nx.lollipop_graph(4, 2), nx.star_graph(5), nx.barbell_graph(3, 1), nx.cycle_graph(5)]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch('conn_graph', order_by='connid')
    for g1, g2 in zip(graphs, returned_graphs):
        assert_true(isinstance(g2, nx.Graph))
        assert_equal(len(g1.edges), len(g2.edges))
        assert_true(0 == len(nx.symmetric_difference(g1, g2).edges))
    c.delete()
    dj.errors._switch_adapted_types(False)


# test with virtual module
virtual_module = dj.create_virtual_module('virtual_module', adapted.schema_name, add_objects={'graph': graph})


def test_adapted_virtual():
    dj.errors._switch_adapted_types(True)
    c = virtual_module.Connectivity()
    graphs = [nx.lollipop_graph(4, 2), nx.star_graph(5), nx.barbell_graph(3, 1), nx.cycle_graph(5)]
    c.insert((i, g) for i, g in enumerate(graphs))
    c.insert1({'connid': 100})  # test work with NULLs
    returned_graphs = c.fetch('conn_graph', order_by='connid')
    for g1, g2 in zip_longest(graphs, returned_graphs):
        if g1 is None:
            assert_true(g2 is None)
        else:
            assert_true(isinstance(g2, nx.Graph))
            assert_equal(len(g1.edges), len(g2.edges))
            assert_true(0 == len(nx.symmetric_difference(g1, g2).edges))
    c.delete()
    dj.errors._switch_adapted_types(False)



