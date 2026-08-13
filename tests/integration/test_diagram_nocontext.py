"""
Regression test for #1535: dj.Diagram must render even when class-name
resolution fails and nodes fall back to raw (quoted) table names — previously a
KeyError on PostgreSQL, where `"schema"."table"` node keys were over-stripped.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_noctx_{backend}_{test_id}"[:64]
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


def test_diagram_renders_without_context(schema_by_backend):
    """With an empty context, nodes keep raw table names; rendering must still
    succeed (no KeyError) on both backends."""
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")

    @schema_by_backend
    class Master(dj.Manual):
        definition = "master_id : int32"

        class Part(dj.Part):
            definition = "-> master\npart_id : int32"

    # Empty context => class-name resolution fails => raw table-name nodes.
    dot = dj.Diagram(schema_by_backend, context={}).make_dot()
    names = [n.get_name() for n in dot.get_nodes()]
    # The master's table appears as a node (by raw table name).
    assert any("master" in n.lower() for n in names), f"master node missing; nodes={names}"
