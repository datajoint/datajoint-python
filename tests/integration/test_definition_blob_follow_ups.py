"""
Definition and blob follow-ups (#1717), on both backends.

- ``blob.max_decompressed_size`` refuses a compressed blob on fetch whose recorded
  uncompressed size exceeds it. Unset, blobs are fetched as before.
- ``describe()`` refuses to write a projected foreign-key attribute name read from
  the database that ``alter()`` would not read back as itself. Names it reads back,
  Python keywords such as ``class`` included, are written as before.
- A column comment and a referenced column name go through ``describe()`` and
  ``alter()``. (Double quotes in comments on PostgreSQL are left to
  https://github.com/datajoint/datajoint-python/issues/1729.)
"""

import time

import numpy as np
import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture
def schema_fu(connection_by_backend, db_creds_by_backend):
    name = f"djtest_def_blob_{db_creds_by_backend['backend']}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


@pytest.fixture
def blob_table(schema_fu):
    @schema_fu
    class Signal(dj.Manual):
        definition = """
        signal_id : int32
        ---
        samples : <blob>
        """

    return Signal


def test_blob_size_limit_on_fetch(blob_table):
    value = np.zeros(5_000_000, dtype=np.uint8)  # about 5 kB compressed
    blob_table.insert1({"signal_id": 1, "samples": value})
    np.testing.assert_array_equal((blob_table & {"signal_id": 1}).fetch1("samples"), value)
    with dj.config.override(blob__max_decompressed_size=1_000_000):
        with pytest.raises(DataJointError, match="max_decompressed_size"):
            (blob_table & {"signal_id": 1}).fetch1("samples")
    with dj.config.override(blob__max_decompressed_size=10_000_000):
        np.testing.assert_array_equal((blob_table & {"signal_id": 1}).fetch1("samples"), value)


@pytest.fixture
def projected_tables(schema_fu):
    class Parent(dj.Manual):
        definition = """
        parent_id : int32  # the parent's id
        """

    class Child(dj.Manual):
        definition = """
        child_id : int32
        ---
        -> Parent.proj(parent_ref="parent_id")
        note : varchar(64)  # it's a note # with a hash
        """

    context = {"Parent": Parent, "Child": Child}
    schema_fu(Parent, context=context)
    schema_fu(Child, context=context)
    return Parent, Child, context


def test_comment_and_referenced_name_through_describe_and_alter(projected_tables):
    """alter() parses describe() of the current table, so both must survive the round trip."""
    _, Child, context = projected_tables
    described = Child().describe(context=context)
    assert '-> Parent.proj(parent_ref="parent_id")' in described
    assert "# it's a note # with a hash" in described
    Child.definition = Child.definition.rstrip() + "\n        extra = null : int32  # added 'later' # also a hash\n"
    Child().alter(prompt=False, context=context)
    described = Child().describe(context=context)
    assert '-> Parent.proj(parent_ref="parent_id")' in described
    assert "# it's a note # with a hash" in described
    assert "# added 'later' # also a hash" in described
    assert Child().heading.attributes["parent_ref"].comment == "the parent's id"
    Child().alter(prompt=False, context=context)  # nothing left to alter


@pytest.mark.parametrize("new_name", ["class", "from"])
def test_projected_name_that_is_a_python_keyword(schema_fu, new_name):
    """A Python keyword is a valid attribute name, and alter() reads it back from describe()."""

    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        """

    class Child(dj.Manual):
        definition = f"""
        child_id : int32
        ---
        -> Parent.proj({new_name}="parent_id")
        """

    context = {"Parent": Parent, "Child": Child}
    schema_fu(Parent, context=context)
    schema_fu(Child, context=context)
    Parent.insert1({"parent_id": 1})
    Child.insert1({"child_id": 1, new_name: 1})
    assert f'-> Parent.proj({new_name}="parent_id")' in Child().describe(context=context)
    Child().alter(prompt=False, context=context)  # describe() is parsed back; nothing to alter
    assert Child.fetch1(new_name) == 1


@pytest.mark.parametrize("new_name", ["x=1", 'a"b', "a'b", "a\\b"])
def test_describe_refuses_projected_name_not_read_back(projected_tables, connection_by_backend, new_name):
    _, Child, _ = projected_tables
    quote = connection_by_backend.adapter.quote_identifier
    q = quote("")[0]  # the backend's identifier quote, doubled inside a quoted name
    connection_by_backend.query(
        f"ALTER TABLE {Child.full_table_name} RENAME COLUMN {quote('parent_ref')} TO {quote(new_name.replace(q, q + q))}"
    )
    connection_by_backend.dependencies.load()
    table = dj.FreeTable(connection_by_backend, Child.full_table_name)
    with pytest.raises(DataJointError, match="not a valid attribute name") as error:
        table.describe()
    assert repr(new_name) in str(error.value)
