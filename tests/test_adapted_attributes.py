import os
import pytest
import tempfile
import datajoint as dj
from datajoint.errors import ADAPTED_TYPE_SWITCH
import networkx as nx
from itertools import zip_longest
from . import schema_adapted
from .schema_adapted import Connectivity, Layout
from . import PREFIX, S3_CONN_INFO


@pytest.fixture
def schema_ad(monkeypatch, connection_test, adapted_graph_instance, enable_adapted_types, enable_filepath_feature):
    assert os.environ.get(ADAPTED_TYPE_SWITCH) == 'TRUE', 'must have adapted types enabled in environment'
    stores_config = {
        "repo-s3": dict(
            S3_CONN_INFO, protocol="s3", location="adapted/repo", stage=tempfile.mkdtemp()
        )
    }
    dj.config["stores"] = stores_config
    schema_name = PREFIX + "_test_custom_datatype"
    layout_to_filepath = schema_adapted.LayoutToFilepath()
    context = {
        **schema_adapted.LOCALS_ADAPTED,
        'graph': adapted_graph_instance,
        'layout_to_filepath': layout_to_filepath,
    }
    schema = dj.schema(schema_name, context=context, connection=connection_test)


    # instantiate for use as a datajoint type
    # TODO: remove?
    graph = adapted_graph_instance

    schema(schema_adapted.Connectivity)
    # errors._switch_filepath_types(True)
    schema(schema_adapted.Layout)
    yield schema
    # errors._switch_filepath_types(False)
    schema.drop()

def test_adapted_type(schema_ad):
    assert os.environ[dj.errors.ADAPTED_TYPE_SWITCH] == 'TRUE'
    c = Connectivity()
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()


# adapted_graph_instance?
# @pytest.mark.skip(reason='misconfigured s3 fixtures')
def test_adapted_filepath_type(schema_ad):
    # https://github.com/datajoint/datajoint-python/issues/684

    # dj.errors._switch_adapted_types(True)
    # dj.errors._switch_filepath_types(True)

    c = Connectivity()
    c.delete()
    c.insert1((0, nx.lollipop_graph(4, 2)))

    layout = nx.spring_layout(c.fetch1("conn_graph"))
    # make json friendly
    layout = {str(k): [round(r, ndigits=4) for r in v] for k, v in layout.items()}
    t = Layout()
    t.insert1((0, layout))
    result = t.fetch1("layout")
    # TODO: may fail, used to be assert_dict_equal
    assert result == layout

    t.delete()
    c.delete()

    # dj.errors._switch_filepath_types(False)
    # dj.errors._switch_adapted_types(False)


# test spawned classes
# TODO: separate fixture
# local_schema = dj.Schema(adapted.schema_name)
# local_schema.spawn_missing_classes()

@pytest.mark.skip(reason='temp')
def test_adapted_spawned():
    dj.errors._switch_adapted_types(True)
    c = Connectivity()  # a spawned class
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.fetch("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()
    dj.errors._switch_adapted_types(False)


# test with virtual module
# TODO: separate fixture
# virtual_module = dj.VirtualModule(
#     "virtual_module", adapted.schema_name, add_objects={"graph": graph}
# )


@pytest.mark.skip(reason='temp')
def test_adapted_virtual():
    dj.errors._switch_adapted_types(True)
    c = virtual_module.Connectivity()
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    c.insert1({"connid": 100})  # test work with NULLs
    returned_graphs = c.fetch("conn_graph", order_by="connid")
    for g1, g2 in zip_longest(graphs, returned_graphs):
        if g1 is None:
            assert g2 is None
        else:
            assert isinstance(g2, nx.Graph)
            assert len(g1.edges) == len(g2.edges)
            assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()
    dj.errors._switch_adapted_types(False)
