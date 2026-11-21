"""
SQL generated for dj.U(...).aggr, built without a database (#1661).

A group that is a union, an aggregation, or a projection with computed attributes
becomes a subquery. A table, a join, or a restriction of these does not. The
integration tests in tests/integration/test_universal_aggr_group.py check the
results on MySQL and PostgreSQL.
"""

import importlib
from types import SimpleNamespace

import pytest

BACKENDS = ["mysql", "postgresql"]


def U(*attributes):
    return importlib.import_module("datajoint").U(*attributes)


def make_table(connection, name, primary_key, secondary, lineage):
    """A table expression with a fixed heading, standing in for a declared table."""
    # Resolve the classes at call time: another unit test reimports datajoint, and
    # the query code checks isinstance against the classes currently imported.
    expression = importlib.import_module("datajoint.expression")
    heading = importlib.import_module("datajoint.heading")
    expr = expression.QueryExpression()
    expr._connection = connection
    expr._support = [connection.adapter.quote_identifier(name)]
    specs = [
        dict(heading.default_attribute_properties, name=n, type="int", in_key=n in primary_key, lineage=lineage.get(n))
        for n in (*primary_key, *secondary)
    ]
    expr._heading = heading.Heading(specs)
    return expr


@pytest.fixture(params=BACKENDS)
def tables(request):
    adapters = importlib.import_module("datajoint.adapters")
    connection = SimpleNamespace(adapter=adapters.get_adapter(request.param))
    lineage = {"parent_id": "s.parent.parent_id", "child_id": "s.child.child_id"}
    parent = make_table(connection, "parent", ["parent_id"], ["label"], lineage)
    child = make_table(connection, "child", ["parent_id", "child_id"], ["color"], lineage)
    info = make_table(connection, "parent_info", ["parent_id"], ["info"], lineage)
    return parent, child, info


def subquery_count(sql):
    return sql.count("(SELECT ")


def test_union_group_keeps_both_operands(tables):
    parent, _, info = tables
    union = (parent.proj() & "parent_id=1") + (info.proj() & "parent_id > 1")
    sql = U().aggr(union, n="count(*)").make_sql()
    assert "UNION" in sql
    assert "parent_info" in sql


def test_aggregation_group_keeps_its_group_by_and_having(tables):
    parent, child, _ = tables
    per_parent = parent.aggr(child, n="count(child_id)")
    sql = U().aggr(per_parent & "n > 0", k="count(*)").make_sql()
    assert sql.count("GROUP BY") == 1  # the inner one; dj.U() adds none
    assert subquery_count(sql) >= 1
    sql = U("label").aggr(parent.aggr(child, "label", n="count(child_id)"), total="sum(n)").make_sql()
    assert sql.count("GROUP BY") == 2


def test_computed_group_is_subquery(tables):
    _, child, _ = tables
    sql = U("parent_id").aggr(child.proj(score="child_id * 10"), m="max(score)").make_sql()
    # The SELECT that computes score is the group's own, without dj.U's GROUP BY.
    computing_select = sql[sql.index("child_id * 10") :].split(")", 1)[0]
    assert "GROUP BY" not in computing_select, sql


@pytest.mark.parametrize("make_group", [lambda p, c: c, lambda p, c: c & "color = 'blue'", lambda p, c: p * c])
def test_table_and_join_groups_have_no_subquery(tables, make_group):
    parent, child, _ = tables
    sql = U("parent_id").aggr(make_group(parent, child), n="count(*)").make_sql()
    assert subquery_count(sql) == 0, sql
