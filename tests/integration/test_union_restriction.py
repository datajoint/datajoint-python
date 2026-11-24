"""
Restriction by a union, and unions whose left operand is a union.

Restricting by a union raised ``TypeError: Union.make_sql() takes 1 positional argument
but 2 were given``. A union with secondary attributes restricts its right operand by its
left one, so a union whose left operand is a union failed the same way.
See https://github.com/datajoint/datajoint-python/issues/1739.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_union_restr_{backend}_{test_id}"[:64]

    def drop():
        if connection_by_backend.is_connected:
            try:
                connection_by_backend.query(
                    f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
                )
            except Exception:
                pass

    drop()
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    drop()


@pytest.fixture
def tables(schema_by_backend):
    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        ---
        label : varchar(16)
        """

    @schema_by_backend
    class Note(dj.Manual):
        definition = """
        -> Parent
        ---
        note : varchar(16)
        """

    Parent.insert([{"parent_id": i, "label": str(i)} for i in range(1, 6)])
    Note.insert([{"parent_id": 2, "note": "x"}, {"parent_id": 3, "note": "y"}])
    return Parent, Note


def keys(expr):
    return sorted(row["parent_id"] for row in expr.to_dicts())


def test_nested_union_with_secondary_attributes(tables):
    Parent, Note = tables
    inner = (Parent & "parent_id = 1") + Note
    u = inner + (Parent.proj(extra="parent_id*10") & "parent_id > 4")

    assert u.heading.names == ["parent_id", "label", "note", "extra"]
    rows = sorted(u.to_dicts(), key=lambda r: r["parent_id"])
    for row in rows:
        if row["extra"] is not None:
            row["extra"] = int(row["extra"])
    assert rows == [
        {"parent_id": 1, "label": "1", "note": None, "extra": None},
        {"parent_id": 2, "label": None, "note": "x", "extra": None},
        {"parent_id": 3, "label": None, "note": "y", "extra": None},
        {"parent_id": 5, "label": None, "note": None, "extra": 50},
    ]
    assert len(u) == 4


def test_restrict_by_union_with_secondary_attributes(tables):
    Parent, Note = tables
    inner = (Parent & "parent_id = 1") + Note

    assert keys(Parent.proj() & inner) == [1, 2, 3]
    assert keys(Parent.proj() - inner) == [4, 5]
    assert len(Parent.proj() & inner) == 3


def test_restrict_by_nested_union(tables):
    Parent, Note = tables
    inner = (Parent & "parent_id = 1") + Note
    u = inner + (Parent.proj(extra="parent_id*10") & "parent_id > 4")

    assert keys(Parent.proj() & u) == [1, 2, 3, 5]
    assert keys(Parent.proj() - u) == [4]


def test_restrict_by_union_without_secondary_attributes(tables):
    Parent, Note = tables
    keys_only = (Parent.proj() & "parent_id = 1") + Note.proj()

    assert keys(Parent & keys_only) == [1, 2, 3]
    assert keys(Parent - keys_only) == [4, 5]


def test_restrict_by_union_without_common_attributes(tables, schema_by_backend):
    """Without common attributes, a non-empty union keeps every row and an empty one none."""
    Parent, Note = tables

    @schema_by_backend
    class Other(dj.Manual):
        definition = """
        other_id : int32
        """

    Other.insert([{"other_id": 7}, {"other_id": 8}])
    nonempty = (Parent & "parent_id = 1") + Note
    empty = (Parent & "parent_id = 0") + (Note & "parent_id = 0")

    assert len(Other & nonempty) == 2
    assert len(Other - nonempty) == 0
    assert len(Other & empty) == 0
    assert len(Other - empty) == 2
