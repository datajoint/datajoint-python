"""
SQL generated for ``dj.U(...) & expr`` operands, built without a database.

``dj.U(...) & expr`` deduplicates with ``DISTINCT``. When such an operand is
used in a join, ``extend``, ``aggr`` or ``dj.U(...).aggr``, the ``DISTINCT``
must stay with the operand, inside a subquery, so that each key is counted
once (#1660). ``dj.U(...) & expr`` and the outer union also deduplicate when
any table ``expr`` is built from has no primary key, whatever ``expr``'s own
primary key. The integration tests in ``tests/integration/test_distinct.py``
check the same cases against MySQL and PostgreSQL.
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
    lineage = {"parent_id": "s.parent.parent_id", "child_id": "s.child.child_id", "tag": "s.tag.tag"}
    parent = make_table(connection, "parent", ["parent_id"], ["label"], lineage)
    child = make_table(connection, "child", ["parent_id", "child_id"], ["kind"], lineage)
    tag = make_table(connection, "tag", ["tag"], [], lineage)
    return parent, child, tag


def distinct_subqueries(sql):
    return sql.count("(SELECT DISTINCT ")


def test_universal_restriction_distinct_only_without_the_key(tables):
    _, child, _ = tables
    assert (U("parent_id") & child).make_sql().startswith("SELECT DISTINCT ")
    assert "DISTINCT" not in (U("parent_id", "child_id") & child).make_sql()


def test_universal_restriction_of_keyless_table_is_distinct(tables):
    """A table without a primary key can hold repeated rows, whatever U's attributes."""
    parent, _, _ = tables
    message = make_table(parent.connection, "message", [], ["msg", "n"], {})
    assert (U("msg") & message).make_sql().startswith("SELECT DISTINCT ")
    assert (U("msg", "n") & message).make_sql().startswith("SELECT DISTINCT ")


@pytest.mark.parametrize("operand", ["right", "left"])
def test_join_wraps_distinct_operand(tables, operand):
    parent, child, _ = tables
    has_children = U("parent_id") & child
    query = parent * has_children if operand == "right" else has_children * parent
    sql = query.make_sql()
    assert distinct_subqueries(sql) == 1, sql
    assert not sql.startswith("SELECT DISTINCT"), sql


def test_restricted_distinct_operand_keeps_distinct_in_join(tables):
    parent, child, _ = tables
    query = (U("parent_id") & child & "parent_id < 3") * parent
    assert distinct_subqueries(query.make_sql()) == 1


def test_extend_wraps_distinct_operand(tables):
    parent, child, _ = tables
    has_children = U("parent_id") & child
    assert distinct_subqueries(parent.extend(has_children).make_sql()) == 1
    assert distinct_subqueries(has_children.extend(parent).make_sql()) == 1


def test_aggr_wraps_distinct_operand(tables):
    parent, child, _ = tables
    kinds = U("parent_id", "kind") & child
    has_children = U("parent_id") & child
    assert distinct_subqueries(parent.aggr(kinds, n="count(kind)").make_sql()) == 1
    assert distinct_subqueries(has_children.aggr(parent, n="count(label)").make_sql()) == 1


def test_universal_aggr_wraps_distinct_group(tables):
    _, child, _ = tables
    parents_per_kind = U("kind").aggr(U("parent_id", "kind") & child, n="count(*)")
    assert distinct_subqueries(parents_per_kind.make_sql()) == 1
    number_of_kinds = U().aggr(U("kind") & child, n="count(*)")
    assert distinct_subqueries(number_of_kinds.make_sql()) == 1


def test_aggregation_emits_no_distinct(tables):
    parent, child, _ = tables
    assert "DISTINCT" not in parent.aggr(child, n="count(*)").make_sql()


def test_join_with_union_on_the_right_without_common_attributes(tables):
    """A union operand on the right is a subquery even when the join has no common attributes."""
    parent, _, tag = tables
    tags = (tag & "tag < 3") + (tag & "tag > 5")
    sql = (parent * tags).make_sql()
    assert "UNION" in sql
    assert " CROSS JOIN (" in sql


def keyless_table(connection):
    """A stand-in for a table without a primary key: it can hold repeated rows."""
    return make_table(connection, "message", [], ["msg", "n"], {})


def make_note(connection):
    """A table that depends on parent, with a secondary attribute: a union operand."""
    return make_table(connection, "note", ["parent_id"], ["note"], {"parent_id": "s.parent.parent_id"})


def test_built_from_keyed_tables_walks_the_whole_expression(tables):
    """A table without a primary key anywhere in the expression is found, whatever its primary key."""
    parent, child, _ = tables
    message = keyless_table(parent.connection)
    note = make_note(parent.connection)
    top = importlib.import_module("datajoint").Top

    for query in (parent, child, parent * child, child.extend(parent), parent.aggr(child, n="count(*)"), parent + note):
        assert query._built_from_keyed_tables()

    doubled = message.proj(..., k="n * 2") & "k > 1"  # a subquery over the keyless table
    for query in (
        message,
        message & "n > 1",
        parent * message,
        message * parent,
        parent.extend(message),
        parent * doubled,
        parent * (message & top(2, order_by="n")),
        (parent * message) + note,
        U("msg").aggr(message, c="count(*)"),
        U("parent_id") & (parent * message),
    ):
        assert not query._built_from_keyed_tables()
    assert (parent * message).primary_key == ["parent_id"]


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda parent, message: parent * message, id="joined"),
        pytest.param(lambda parent, message: message * parent, id="joined-on-the-left"),
        pytest.param(lambda parent, message: parent.extend(message), id="extended"),
        pytest.param(lambda parent, message: parent * (message.proj(..., k="n * 2") & "k > 1"), id="in-subquery"),
    ],
)
def test_universal_restriction_over_keyless_table_is_distinct(tables, build):
    """``dj.U(...) & expr`` deduplicates when expr is built from a keyless table, even if U covers its key."""
    parent, _, _ = tables
    query = build(parent, keyless_table(parent.connection))
    assert query.primary_key == ["parent_id"]
    assert (U("parent_id") & query).make_sql().startswith("SELECT DISTINCT ")
    assert (U("parent_id", "msg", "n") & query).make_sql().startswith("SELECT DISTINCT ")


def test_union_with_keyless_operand_deduplicates(tables):
    """The outer union keeps UNION when an operand is built from a keyless table."""
    parent, _, _ = tables
    note = make_note(parent.connection)
    assert "UNION ALL" in (parent + note).make_sql()
    sql = ((parent * keyless_table(parent.connection)) + note).make_sql()
    assert "UNION" in sql and "UNION ALL" not in sql
