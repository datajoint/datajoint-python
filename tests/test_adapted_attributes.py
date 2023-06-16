import datajoint as dj, networkx as nx
from itertools import zip_longest
from .schemas import connection_root, connection_test, bucket
from schemas.adapted import schema, stores, store, graph, Connectivity, Layout


def test_adapted_type(Connectivity):
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    Connectivity.insert(((i, g) for i, g in enumerate(graphs)))
    returned_graphs = Connectivity().fetch("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)


def test_adapted_filepath_type(Connectivity, Layout):
    # https://github.com/datajoint/datajoint-python/issues/684

    # Connectivity.delete()
    Connectivity.insert1((0, nx.lollipop_graph(4, 2)))
    layout = nx.spring_layout(Connectivity().fetch1("conn_graph"))
    # make json friendly
    layout = {str(k): [round(r, ndigits=4) for r in v] for k, v in layout.items()}
    Layout.insert1((0, layout))
    result = Layout().fetch1("layout")
    assert result == layout


def test_adapted_spawned(schema, Connectivity):
    context = locals()
    schema.spawn_missing_classes(context=context)
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    context["Connectivity"].insert(((i, g) for i, g in enumerate(graphs)))
    returned_graphs = context["Connectivity"]().fetch("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)


def test_adapted_virtual(schema, Connectivity, graph):
    virtual_module = dj.VirtualModule(
        "virtual_module", (schema.database), add_objects={"graph": graph}
    )
    c = virtual_module.Connectivity()
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert(((i, g) for i, g in enumerate(graphs)))
    c.insert1({"connid": 100})
    returned_graphs = c.fetch("conn_graph", order_by="connid")
    for g1, g2 in zip_longest(graphs, returned_graphs):
        if g1 is None:
            assert g2 is None
        else:
            assert isinstance(g2, nx.Graph)
            assert len(g1.edges) == len(g2.edges)
            assert 0 == len(nx.symmetric_difference(g1, g2).edges)
