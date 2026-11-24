"""
Secondary indexes read back from the database, on MySQL and PostgreSQL.

``Heading.indexes`` feeds ``describe()``, and ``alter()`` compares the indexes
of the new definition with those of ``describe()``. On PostgreSQL the heading
used to find no indexes at all, so ``describe()`` dropped ``[unique]`` foreign
keys and secondary indexes, and ``alter()`` with an unchanged definition raised
``NotImplementedError`` on any table with a secondary index.
"""

import time

import pytest

import datajoint as dj
from datajoint.declare import declare

CHILD_DEFINITION = """
child_id : int32
---
-> [unique] Parent
-> Other
a : int32
b : varchar(32)
c : int32
index(a)
unique index(b, c)
"""

# What MySQL reports for Child: the foreign keys' indexes and the two declared ones.
EXPECTED_INDEXES = {
    ("a",): {"unique": False, "nullable": False},
    ("b", "c"): {"unique": True, "nullable": False},
    ("other_id",): {"unique": False, "nullable": False},
    ("parent_id",): {"unique": True, "nullable": False},
}


# What describe() returns for Child on MySQL, with runs of spaces collapsed.
EXPECTED_DESCRIBE = [
    "child_id : int32",
    "---",
    "-> [unique] Parent",
    "-> Other",
    "a : int32",
    "b : varchar(32)",
    "c : int32",
    "INDEX (a)",
    "UNIQUE INDEX (b, c)",
]


class Parent(dj.Manual):
    definition = """
    parent_id : int32
    """


class Other(dj.Manual):
    definition = """
    other_id : int32
    """


class Child(dj.Manual):
    definition = CHILD_DEFINITION


class ChildCopy(dj.Manual):
    definition = CHILD_DEFINITION  # replaced by Child().describe() in the test


@pytest.fixture
def schema_indexes(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_indexes_{backend}_{test_id}"
    adapter = connection_by_backend.adapter
    connection_by_backend.query(f"DROP DATABASE IF EXISTS {adapter.quote_identifier(schema_name)}")
    schema = dj.Schema(schema_name, context=dict(Parent=Parent, Other=Other), connection=connection_by_backend)
    schema(Parent)
    schema(Other)
    schema(Child)
    yield schema
    schema.drop(prompt=False)


def _context():
    return dict(Parent=Parent, Other=Other, Child=Child)


def _indexes(table):
    """The heading's indexes, after loading the heading (it loads on first use)."""
    heading = table.heading
    assert heading.table_status is not None
    return heading.indexes


def test_heading_indexes(schema_indexes):
    """Both backends report the same secondary indexes, without the primary key."""
    indexes = _indexes(Child())
    assert indexes == EXPECTED_INDEXES
    assert list(indexes) == list(EXPECTED_INDEXES)  # ordered by index name on both


def test_describe_shows_indexes(schema_indexes):
    """describe() keeps the [unique] foreign key and the secondary indexes."""
    definition = Child().describe(context=_context())
    assert [" ".join(line.split()) for line in definition.splitlines()] == EXPECTED_DESCRIBE


def test_describe_declares_same_table(schema_indexes):
    """The output of describe() declares the same table as the definition."""
    child = Child()
    adapter = child.connection.adapter
    context = _context()
    from_definition = declare(child.full_table_name, child.definition, context, adapter)
    from_describe = declare(child.full_table_name, child.describe(context=context), context, adapter)
    assert from_describe[0] == from_definition[0]  # CREATE TABLE
    assert from_describe[5] == from_definition[5]  # CREATE INDEX and comments (PostgreSQL)


def test_redeclare_from_describe(schema_indexes):
    """A table declared from describe() has the same indexes."""
    ChildCopy.definition = Child().describe(context=_context())
    try:
        schema_indexes(ChildCopy, context=_context())
        assert _indexes(ChildCopy()) == EXPECTED_INDEXES
        assert ChildCopy().describe(context=_context()) == Child().describe(context=_context())
    finally:
        ChildCopy.definition = CHILD_DEFINITION


def test_alter_unchanged_definition(schema_indexes, monkeypatch):
    """alter() with an unchanged definition runs no statement."""
    connection = Child.connection
    before = (Child().describe(context=_context()), _indexes(Child()))
    statements = []
    query = connection.query

    def recording_query(sql, *args, **kwargs):
        statements.append(sql)
        return query(sql, *args, **kwargs)

    monkeypatch.setattr(connection, "query", recording_query)
    Child.alter(prompt=False, context=_context())
    monkeypatch.undo()
    assert not [s for s in statements if s.lstrip().upper().startswith(("ALTER", "COMMENT", "CREATE", "DROP"))]
    assert (Child().describe(context=_context()), _indexes(Child())) == before


def test_expression_index_is_skipped(schema_indexes):
    """An index with an expression is left out of the heading.

    Neither backend names a column for an expression. Listing only the plain
    columns of such an index would describe a different index, so the heading
    leaves out any index that contains an expression, on both backends.
    """
    child = Child()
    connection = child.connection
    connection.query(f"CREATE INDEX child_expr ON {child.full_table_name} ((lower(b)))")
    connection.query(f"CREATE INDEX child_expr_first ON {child.full_table_name} ((c + 1), a)")
    # A unique index that mixes a column and an expression. Its name sorts after
    # the plain index on (a) on both backends, so it used to replace that index
    # in the heading as a unique index on (a) alone.
    connection.query(f"CREATE UNIQUE INDEX zz_mixed_unique ON {child.full_table_name} (a, (lower(b)))")
    indexes = _indexes(dj.FreeTable(connection, child.full_table_name))
    assert indexes == EXPECTED_INDEXES
    lines = [" ".join(line.split()) for line in child.describe(context=_context()).splitlines()]
    assert lines == EXPECTED_DESCRIBE


class Ordered(dj.Manual):
    # The order of index names differs between backends: MySQL lists INDEX (a, c)
    # first and PostgreSQL lists INDEX (a_b) first. On PostgreSQL, index-name
    # order also differs from declared order.
    definition = """
    ordered_id : int32
    ---
    a : int32
    a_b : int32
    c : int32
    index(a, c)
    index(a_b)
    """


class Unsorted(dj.Manual):
    # Declared index order differs from index-name order on both backends:
    # index(z) is declared first, and its name sorts after that of index(a).
    definition = """
    unsorted_id : int32
    ---
    a : int32
    z : int32
    index(z)
    index(a)
    """


@pytest.mark.parametrize(
    "table, expected",
    [
        (Ordered, ["INDEX (a, c)", "INDEX (a_b)"]),
        (Unsorted, ["INDEX (a)", "INDEX (z)"]),
    ],
    ids=["ordered", "unsorted"],
)
def test_alter_ignores_index_order(schema_indexes, monkeypatch, table, expected):
    """alter() with an unchanged definition does not depend on index order."""
    schema_indexes(table)
    connection = table.connection
    described = [line for line in table().describe().splitlines() if "INDEX" in line]
    assert sorted(" ".join(line.split()) for line in described) == expected
    statements = []
    query = connection.query

    def recording_query(sql, *args, **kwargs):
        statements.append(sql)
        return query(sql, *args, **kwargs)

    monkeypatch.setattr(connection, "query", recording_query)
    table.alter(prompt=False)
    monkeypatch.undo()
    assert not [s for s in statements if s.lstrip().upper().startswith(("ALTER", "COMMENT", "CREATE", "DROP"))]
