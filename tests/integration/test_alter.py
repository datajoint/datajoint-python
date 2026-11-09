import importlib
import re
import sys
import time

import pytest

import datajoint as dj

from tests import schema as schema_any_module
from tests.schema_alter import LOCALS_ALTER, Experiment, Parent

COMBINED_CONTEXT = {
    **schema_any_module.LOCALS_ANY,
    **LOCALS_ALTER,
}


@pytest.fixture
def schema_alter(connection_test, schema_any_fresh):
    # Overwrite Experiment and Parent nodes using fresh schema
    schema_any_fresh(Experiment, context=LOCALS_ALTER)
    schema_any_fresh(Parent, context=LOCALS_ALTER)
    yield schema_any_fresh
    schema_any_fresh.drop()


class TestAlter:
    def verify_alter(self, schema_alter, table, attribute_sql):
        definition_original = schema_alter.connection.query(f"SHOW CREATE TABLE {table.full_table_name}").fetchone()[1]
        table.definition = table.definition_new
        table.alter(prompt=False)
        definition_new = schema_alter.connection.query(f"SHOW CREATE TABLE {table.full_table_name}").fetchone()[1]
        assert re.sub(f"{attribute_sql},\n  ", "", definition_new) == definition_original

    def test_alter(self, schema_alter):
        original = schema_alter.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
        Experiment.definition = Experiment.definition1
        Experiment.alter(prompt=False, context=COMBINED_CONTEXT)
        altered = schema_alter.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
        assert original != altered
        Experiment.definition = Experiment.original_definition
        Experiment().alter(prompt=False, context=COMBINED_CONTEXT)
        restored = schema_alter.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
        assert altered != restored
        assert original == restored

    def test_alter_part(self, schema_alter):
        """
        https://github.com/datajoint/datajoint-python/issues/936
        """
        # Regex includes optional COMMENT for type annotations
        self.verify_alter(schema_alter, table=Parent.Child, attribute_sql=r"`child_id` .* DEFAULT NULL[^,]*")
        self.verify_alter(
            schema_alter,
            table=Parent.Grandchild,
            attribute_sql=r"`grandchild_id` .* DEFAULT NULL[^,]*",
        )


# A pipeline module that declares its tables at import, like the one in #1602.
# Its schema is activated by the test, on the test's connection.
PIPELINE_MODULE = '''
import datajoint as dj

schema = dj.Schema()


@schema
class Item(dj.Manual):
    definition = """
    item : int32
    """


@schema
class Measurement(dj.Manual):
    definition = """
    -> Item
    ---
    value : float32
    """

    class Note(dj.Part):
        definition = """
        -> master
        ---
        note : varchar(64)
        """


@schema
class Link(dj.Manual):
    definition = """
    -> Item
    -> Item.proj(other_item='item')
    """
'''


@pytest.fixture
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema = dj.Schema(f"djtest_alter_{backend}_{test_id}"[:64], connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


@pytest.fixture
def pipeline(schema_by_backend, tmp_path, monkeypatch):
    """Import the pipeline module from tmp_path, its tables declared in the test schema."""
    (tmp_path / "alter_pipeline.py").write_text(PIPELINE_MODULE)
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        module = importlib.import_module("alter_pipeline")
        module.schema.activate(schema_by_backend.database, connection=schema_by_backend.connection)
        yield module
    finally:
        sys.modules.pop("alter_pipeline", None)


# Each alter adds an attribute: changing an attribute's type emits MODIFY, which
# PostgreSQL does not accept. Foreign keys resolve before any SQL is generated.


def test_alter_from_another_module(pipeline):
    """alter() resolves foreign keys in the declaration context, not in the caller's namespace (#1602)."""
    # This module has no `Item`, so resolving `-> Item` here would fail
    pipeline.Measurement.definition = """
    -> Item
    ---
    value : float32
    unit = "" : varchar(16)
    """
    pipeline.Measurement.alter(prompt=False)
    assert pipeline.Measurement.heading.secondary_attributes == ["value", "unit"]


def test_alter_part_from_another_module(pipeline):
    """A Part table still resolves `-> master` in its declaration context."""
    pipeline.Measurement.Note.definition = """
    -> master
    ---
    note : varchar(64)
    author = "" : varchar(32)
    """
    pipeline.Measurement.Note.alter(prompt=False)
    assert pipeline.Measurement.Note.heading.secondary_attributes == ["note", "author"]


def test_alter_explicit_context(pipeline):
    """An explicit context takes precedence over the declaration context."""
    # `Upstream` is defined nowhere else, so it resolves only in the explicit context
    pipeline.Measurement.definition = """
    -> Upstream
    ---
    value : float32
    unit = "" : varchar(16)
    """
    pipeline.Measurement.alter(prompt=False, context={"Upstream": pipeline.Item})
    assert pipeline.Measurement.heading.secondary_attributes == ["value", "unit"]


def test_alter_projected_foreign_key(pipeline):
    """A projected foreign key round-trips through describe() and alter()."""
    assert '-> Item.proj(other_item="item")' in pipeline.Link.describe(context=vars(pipeline))
    pipeline.Link.definition = """
    -> Item
    -> Item.proj(other_item='item')
    ---
    weight = 1 : float32
    """
    pipeline.Link.alter(prompt=False)
    assert pipeline.Link.heading.primary_key == ["item", "other_item"]
    assert pipeline.Link.heading.secondary_attributes == ["weight"]


def test_alter_reads_catalog_text_as_data(pipeline):
    """Text stored in the catalog is reproduced by describe() on one line and only read by alter()."""
    calls = []

    def record():
        calls.append(True)
        return pipeline.Item

    context = dict(vars(pipeline), record=record)
    table = pipeline.Item()
    comment = "note\n-> record()"
    if table.connection.adapter.backend == "postgresql":
        sql = f"COMMENT ON TABLE {table.full_table_name} IS %s"
    else:
        sql = f"ALTER TABLE {table.full_table_name} COMMENT = %s"
    table.connection.query(sql, args=(comment,))
    table.__class__._heading = dj.heading.Heading(table_info=table.heading.table_info)
    described = table.describe(context=context)
    assert described.splitlines()[0] == "# note -> record()"
    # keep the comment unchanged so that only the new attribute is altered
    table.definition = (
        described.splitlines()[0]
        + """
    item : int32
    ---
    label = "" : varchar(16)
    """
    )
    table.alter(prompt=False, context=context)
    assert calls == []
    assert table.heading.secondary_attributes == ["label"]


def test_alter_unresolved_parent(pipeline):
    """A parent that is not in the context is reported as unresolved."""
    pipeline.Measurement.definition = """
    -> Item
    ---
    value : float32
    unit = "" : varchar(16)
    """
    with pytest.raises(dj.DataJointError, match="could not be resolved"):
        pipeline.Measurement.alter(prompt=False, context={})
