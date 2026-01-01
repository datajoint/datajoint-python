"""
Tests for adapted/custom attribute types.

These tests verify the Codec system for custom data types.
"""

from itertools import zip_longest

import networkx as nx
import pytest

import datajoint as dj

from tests import schema_adapted
from tests.schema_adapted import Connectivity, Layout


@pytest.fixture
def schema_name(prefix):
    return prefix + "_test_custom_datatype"


@pytest.fixture
def schema_ad(
    connection_test,
    enable_filepath_feature,
    s3_creds,
    tmpdir,
    schema_name,
):
    dj.config["stores"] = {"repo-s3": dict(s3_creds, protocol="s3", location="adapted/repo", stage=str(tmpdir))}
    # Codecs are auto-registered via __init_subclass__ in schema_adapted
    context = {**schema_adapted.LOCALS_ADAPTED}
    schema = dj.schema(schema_name, context=context, connection=connection_test)
    schema(schema_adapted.Connectivity)
    schema(schema_adapted.Layout)
    yield schema
    schema.drop()


@pytest.fixture
def local_schema(schema_ad, schema_name):
    """Fixture for testing spawned classes"""
    local_schema = dj.Schema(schema_name, connection=schema_ad.connection)
    local_schema.spawn_missing_classes()
    yield local_schema
    # Don't drop - schema_ad fixture handles cleanup


@pytest.fixture
def schema_virtual_module(schema_ad, schema_name):
    """Fixture for testing virtual modules"""
    # Types are registered globally, no need to add_objects for codecs
    schema_virtual_module = dj.VirtualModule("virtual_module", schema_name, connection=schema_ad.connection)
    return schema_virtual_module


def test_adapted_type(schema_ad):
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


def test_adapted_filepath_type(schema_ad, minio_client):
    """https://github.com/datajoint/datajoint-python/issues/684"""
    c = Connectivity()
    c.delete()
    c.insert1((0, nx.lollipop_graph(4, 2)))

    layout = nx.spring_layout(c.fetch1("conn_graph"))
    # make json friendly
    layout = {str(k): [round(r, ndigits=4) for r in v] for k, v in layout.items()}
    t = Layout()
    t.insert1((0, layout))
    result = t.fetch1("layout")
    assert result == layout
    t.delete()
    c.delete()


def test_adapted_spawned(local_schema):
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


def test_adapted_virtual(schema_virtual_module):
    c = schema_virtual_module.Connectivity()
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
