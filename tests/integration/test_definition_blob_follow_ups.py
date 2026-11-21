"""
Definition and blob follow-ups (#1717), on both backends.

- ``blob.max_decompressed_size`` refuses a compressed blob on fetch whose recorded
  uncompressed size exceeds it. Unset, blobs are fetched as before.
- A projected foreign-key attribute named with any Python identifier, a keyword
  such as ``class`` or a name with characters that ``\\w`` does not match, is
  declared, written by ``describe()`` and read back by ``alter()``.
- A projected foreign-key attribute named in the database with a name no definition
  can hold (``x=1``, ``a-b``) is written as is by ``describe()``, with a warning;
  ``alter()`` refuses the table, and ``dj.Diagram`` still renders the schema.
- ``dj.Diagram`` renders a table whose ``describe()`` raises, without its definition
  in the tooltip.
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


@pytest.mark.parametrize("new_name", ["class", "from", "किताब_id", "x\u00b7y"])
def test_projected_name_that_is_a_python_identifier(schema_fu, new_name):
    """
    Every Python identifier is a valid projected name: a keyword, or a name with a
    character that \\w does not match (a combining vowel sign, U+00B7). alter() reads
    it back from describe(), and the diagram shows the table.
    """

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
    assert "Child" in dj.Diagram(schema_fu, context=context).make_svg().data


# Column names that SQL accepts and that no definition can hold: the new name of a
# projected foreign key is written unquoted, as a keyword argument name, and the
# parser reads only a Python identifier there.
NAMES_NOT_READ_BACK = ["x=1", 'a"b', "a'b", "a\\b", "a-b", "a b", "1st_ref", "a$b"]


def _rename_foreign_key_column(connection, table, new_name):
    quote = connection.adapter.quote_identifier
    q = quote("")[0]  # the backend's identifier quote, doubled inside a quoted name
    connection.query(
        f"ALTER TABLE {table.full_table_name} RENAME COLUMN {quote('parent_ref')} TO {quote(new_name.replace(q, q + q))}"
    )
    connection.dependencies.load()


@pytest.mark.parametrize("new_name", NAMES_NOT_READ_BACK)
def test_describe_writes_projected_name_not_read_back(projected_tables, connection_by_backend, new_name):
    """describe() writes such a name as is and warns; alter() refuses the table instead of parsing that text."""
    _, Child, context = projected_tables
    _rename_foreign_key_column(connection_by_backend, Child, new_name)
    table = dj.FreeTable(connection_by_backend, Child.full_table_name)
    with pytest.warns(UserWarning, match="cannot be read back") as record:
        described = table.describe(context=context)
    assert repr(new_name) in str(record[0].message)
    assert f'.proj({new_name}="parent_id")' in described
    assert "note" in described  # the rest of the definition is written
    with pytest.raises(DataJointError, match="Cannot alter") as error:
        table.alter(prompt=False, context=context)
    assert repr(new_name) in str(error.value)


@pytest.mark.parametrize("new_name", NAMES_NOT_READ_BACK)
def test_diagram_of_schema_with_projected_name_not_read_back(projected_tables, connection_by_backend, new_name):
    """One table whose definition cannot be read back does not stop the diagram of its schema."""
    _, Child, _ = projected_tables
    _rename_foreign_key_column(connection_by_backend, Child, new_name)
    module = dj.VirtualModule("module", Child.database, connection=connection_by_backend)
    with pytest.warns(UserWarning, match="cannot be read back"):
        svg = dj.Diagram(module.schema, context=vars(module)).make_svg().data
    assert "Child" in svg and "Parent" in svg


def test_diagram_tooltip_without_definition(schema_fu, projected_tables, monkeypatch):
    """A table whose describe() raises gets a tooltip without its definition; the other tables keep theirs."""
    _, Child, context = projected_tables

    def describe(self, context=None, printout=False):
        raise DataJointError("cannot describe")

    monkeypatch.setattr(Child, "describe", describe)
    diagram = dj.Diagram(schema_fu, context=context)
    tooltips = {node.get_name().strip('"'): node.get("tooltip") for node in diagram.make_dot().get_nodes()}
    assert tooltips["Child"].strip('"') == "Child"
    assert "parent_id" in tooltips["Parent"]
    assert "Child" in diagram.make_svg().data
