"""
dj.U(...).aggr aggregates over the rows the group returns (#1661).

When the group is a union, an aggregation, or has computed or renamed attributes (a
projection, or a join with such a projection), the group is a subquery, so its UNION,
its own GROUP BY and HAVING, and its computed attributes apply before dj.U's GROUP BY.
Tables, joins of tables, and their restrictions keep the plain SQL with no subquery.
All tests run on MySQL and PostgreSQL.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_uaggr_{backend}_{test_id}"[:64]
    drop = f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass


@pytest.fixture(scope="function")
def tables(schema_by_backend):
    schema = schema_by_backend

    @schema
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        ---
        label : varchar(8)
        """

    @schema
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        ---
        color : varchar(8)
        """

    @schema
    class ParentInfo(dj.Manual):
        definition = """
        -> Parent
        ---
        info : varchar(8)
        """

    Parent.insert([dict(parent_id=1, label="a"), dict(parent_id=2, label="b"), dict(parent_id=3, label="a")])
    Child.insert(
        [
            dict(parent_id=1, child_id=1, color="red"),
            dict(parent_id=1, child_id=2, color="blue"),
            dict(parent_id=2, child_id=1, color="blue"),
        ]
    )
    ParentInfo.insert([dict(parent_id=1, info="x"), dict(parent_id=2, info="y"), dict(parent_id=3, info="z")])
    return Parent, Child, ParentInfo


def as_dict(query, key, value):
    return dict(zip(*query.to_arrays(key, value)))


def test_group_is_union(tables):
    Parent, Child, ParentInfo = tables
    union = (Parent.proj() & "parent_id=1") + (ParentInfo.proj() & "parent_id > 1")
    assert len(union) == 3
    assert dj.U().aggr(union, n="count(*)").fetch1("n") == 3
    assert as_dict(dj.U("parent_id").aggr(union, n="count(*)"), "parent_id", "n") == {1: 1, 2: 1, 3: 1}


def test_group_is_union_with_secondary_attributes(tables):
    Parent, Child, ParentInfo = tables
    union = (Parent & "parent_id < 3") + (ParentInfo & "parent_id > 1")
    assert len(union) == 3
    assert dj.U().aggr(union, n="count(*)").fetch1("n") == 3
    assert as_dict(dj.U("label").aggr(union, n="count(*)"), "label", "n") == {"a": 1, "b": 1, None: 1}
    # info comes only from the second operand
    assert as_dict(dj.U("info").aggr(union, n="count(*)"), "info", "n") == {"y": 1, "z": 1, None: 1}


def test_group_is_aggregation(tables):
    Parent, Child, ParentInfo = tables
    per_parent = Parent.aggr(Child, n="count(child_id)")  # 1 -> 2, 2 -> 1, 3 -> 0
    assert dj.U().aggr(per_parent, total="sum(n)").fetch1("total") == 3
    assert dj.U().aggr(per_parent, k="count(*)").fetch1("k") == 3
    restricted_first = (Parent & "parent_id < 3").aggr(Child, n="count(child_id)")
    assert dj.U().aggr(restricted_first, k="count(*)").fetch1("k") == 2
    assert dj.U().aggr(per_parent & "n > 0", k="count(*)").fetch1("k") == 2


def test_group_is_aggregation_grouped_by_attribute(tables):
    Parent, Child, ParentInfo = tables
    per_parent = Parent.aggr(Child, "label", n="count(child_id)")
    assert as_dict(dj.U("label").aggr(per_parent, total="sum(n)"), "label", "total") == {"a": 2, "b": 1}


def test_group_has_computed_attribute(tables):
    Parent, Child, ParentInfo = tables
    scores = Child.proj(score="child_id * 10")
    assert as_dict(dj.U("parent_id").aggr(scores, m="max(score)"), "parent_id", "m") == {1: 20, 2: 10}
    assert dj.U().aggr(scores, m="max(score)").fetch1("m") == 20
    assert as_dict(dj.U("score").aggr(scores, n="count(*)"), "score", "n") == {10: 2, 20: 1}
    assert as_dict(dj.U("parent_id").aggr(scores, n="count(*)"), "parent_id", "n") == {1: 2, 2: 1}
    renamed = Child.proj(pid="parent_id")
    assert as_dict(dj.U("pid").aggr(renamed, n="count(*)"), "pid", "n") == {1: 2, 2: 1}
    renamed = Child.proj("color", pid="parent_id")
    assert as_dict(dj.U("color").aggr(renamed, m="max(pid)"), "color", "m") == {"blue": 2, "red": 1}
    assert as_dict(dj.U("color").aggr(renamed, n="count(*)"), "color", "n") == {"blue": 2, "red": 1}


def test_join_with_computed_operand(tables):
    Parent, Child, ParentInfo = tables
    joined = Parent * Child.proj(score="child_id * 10")
    assert as_dict(dj.U("label").aggr(joined, m="max(score)"), "label", "m") == {"a": 20, "b": 10}
    assert as_dict(dj.U("label").aggr(joined, n="count(*)"), "label", "n") == {"a": 2, "b": 1}


def test_table_and_join_groups_need_no_subquery(tables):
    Parent, Child, ParentInfo = tables
    for group in (Child(), Child & "color = 'blue'", Parent * Child, (Parent * Child) & "label = 'a'"):
        query = dj.U("parent_id").aggr(group, n="count(*)")
        assert "SELECT" not in query.make_sql()[len("SELECT") :], query.make_sql()
    assert as_dict(dj.U("color").aggr(Child, n="count(*)"), "color", "n") == {"blue": 2, "red": 1}
    assert as_dict(dj.U("label").aggr(Parent * Child, n="count(*)"), "label", "n") == {"a": 2, "b": 1}
