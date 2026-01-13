"""
Tests for custom codecs.

These tests verify the Codec system for custom data types.
"""

from itertools import zip_longest

import networkx as nx
import pytest

import datajoint as dj

from tests import schema_codecs
from tests.schema_codecs import Connectivity, Layout


@pytest.fixture
def schema_name(prefix):
    return prefix + "_test_codecs"


@pytest.fixture
def schema_codec(
    connection_test,
    s3_creds,
    tmpdir,
    schema_name,
):
    dj.config["stores"] = {"repo-s3": dict(s3_creds, protocol="s3", location="codecs/repo", stage=str(tmpdir))}
    # Codecs are auto-registered via __init_subclass__ in schema_codecs
    context = {**schema_codecs.LOCALS_CODECS}
    schema = dj.schema(schema_name, context=context, connection=connection_test)
    schema(schema_codecs.Connectivity)
    schema(schema_codecs.Layout)
    yield schema
    schema.drop()


@pytest.fixture
def local_schema(schema_codec, schema_name):
    """Fixture for testing generated classes"""
    local_schema = dj.Schema(schema_name, connection=schema_codec.connection)
    local_schema.make_classes()
    yield local_schema
    # Don't drop - schema_codec fixture handles cleanup


@pytest.fixture
def schema_virtual_module(schema_codec, schema_name):
    """Fixture for testing virtual modules"""
    # Codecs are registered globally, no need to add_objects
    schema_virtual_module = dj.VirtualModule("virtual_module", schema_name, connection=schema_codec.connection)
    return schema_virtual_module


def test_codec_graph(schema_codec):
    """Test basic codec encode/decode with graph type."""
    c = Connectivity()
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.to_arrays("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()


def test_codec_chained(schema_codec, minio_client):
    """Test codec chaining (layout -> blob)."""
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


def test_codec_spawned(local_schema):
    """Test codecs work with spawned classes."""
    c = Connectivity()  # a spawned class
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    returned_graphs = c.to_arrays("conn_graph", order_by="connid")
    for g1, g2 in zip(graphs, returned_graphs):
        assert isinstance(g2, nx.Graph)
        assert len(g1.edges) == len(g2.edges)
        assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()


def test_codec_virtual_module(schema_virtual_module):
    """Test codecs work with virtual modules."""
    c = schema_virtual_module.Connectivity()
    graphs = [
        nx.lollipop_graph(4, 2),
        nx.star_graph(5),
        nx.barbell_graph(3, 1),
        nx.cycle_graph(5),
    ]
    c.insert((i, g) for i, g in enumerate(graphs))
    c.insert1({"connid": 100})  # test work with NULLs
    returned_graphs = c.to_arrays("conn_graph", order_by="connid")
    for g1, g2 in zip_longest(graphs, returned_graphs):
        if g1 is None:
            assert g2 is None
        else:
            assert isinstance(g2, nx.Graph)
            assert len(g1.edges) == len(g2.edges)
            assert 0 == len(nx.symmetric_difference(g1, g2).edges)
    c.delete()
