"""
Part tables in schemas whose names hold non-word characters, such as hyphens.

DataJoint finds a part table's master from its full table name. These tests
declare a master with a part in a hyphenated schema and check that
part_integrity, part-to-master cascading, drop and part ordering behave as
they do in a schema whose name holds only word characters.
"""

import time

import pytest

import datajoint as dj
from datajoint.dependencies import extract_master
from datajoint.errors import DataJointError


@pytest.fixture(
    scope="function",
    params=["djtest-parts-{backend}-{tid}", "djtest_parts_{backend}_{tid}"],
    ids=["hyphenated", "word-only"],
)
def parts_schema(connection_by_backend, db_creds_by_backend, request):
    backend = db_creds_by_backend["backend"]
    schema_name = request.param.format(backend=backend, tid=str(int(time.time() * 1000))[-8:])
    # DROP DATABASE on MySQL, DROP SCHEMA ... CASCADE on PostgreSQL
    drop_sql = connection_by_backend.adapter.drop_schema_sql(schema_name)
    connection_by_backend.query(drop_sql)
    schema = dj.Schema(schema_name, connection=connection_by_backend)

    @schema
    class Monitor(dj.Lookup):
        definition = """
        monitor_id : int32
        """
        contents = [{"monitor_id": 1}]

    @schema
    class Stimulus(dj.Lookup):
        definition = """
        stim_id : int32
        ---
        -> Monitor
        """
        contents = [{"stim_id": 1, "monitor_id": 1}, {"stim_id": 2, "monitor_id": 1}]

    @schema
    class Session(dj.Manual):
        definition = """
        session_id : int32
        """

        class Trial(dj.Part):
            definition = """
            -> master
            trial_id : int32
            ---
            -> Stimulus
            """

    Session.insert([{"session_id": 1}, {"session_id": 2}])
    Session.Trial.insert(
        [
            {"session_id": 1, "trial_id": 1, "stim_id": 1},
            {"session_id": 2, "trial_id": 1, "stim_id": 2},
        ]
    )
    yield schema, Stimulus, Session
    connection_by_backend.query(drop_sql)


def test_master_is_found(parts_schema):
    _, _, Session = parts_schema
    assert extract_master(Session.Trial.full_table_name) == Session.full_table_name
    assert Session.connection.adapter.get_master_table_name(Session.Trial.full_table_name) == Session.full_table_name


def test_topo_sort_keeps_part_with_master(parts_schema):
    """The part's other parent comes before the master, and the part right after it.

    Stimulus depends on Monitor, so plain topological order places Stimulus after
    Session and between Session and Session.Trial. Only a recognized part moves
    its parents ahead of its master and itself up to the master.
    """
    _, Stimulus, Session = parts_schema
    deps = Session.connection.dependencies
    deps.load()
    order = deps.topo_sort()
    master, part = order.index(Session.full_table_name), order.index(Session.Trial.full_table_name)
    assert order.index(Stimulus.full_table_name) < master < part
    assert part == master + 1


def test_delete_enforce_refuses_part_without_master(parts_schema):
    _, Stimulus, Session = parts_schema
    with pytest.raises(DataJointError, match="before its master"):
        (Stimulus & {"stim_id": 1}).delete(prompt=False)
    assert len(Stimulus()) == 2
    assert len(Session()) == 2
    assert len(Session.Trial()) == 2


def test_delete_cascade_reaches_master(parts_schema):
    _, Stimulus, Session = parts_schema
    (Stimulus & {"stim_id": 2}).delete(prompt=False, part_integrity="cascade")
    assert Session.to_arrays("session_id").tolist() == [1]
    assert Session.Trial.to_arrays("session_id").tolist() == [1]


def test_drop_refuses_part_without_master(parts_schema):
    _, Stimulus, Session = parts_schema
    with pytest.raises(DataJointError, match="before its master"):
        Stimulus.drop(prompt=False)
    assert Stimulus.is_declared
    assert Session.Trial.is_declared
