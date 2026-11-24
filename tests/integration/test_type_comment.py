"""
Declared types read back from column comments, on both backends (#1711).

``declare`` records a core or codec type at the start of the column comment as
``:type:comment``. A comment whose marker is not a single-line core or codec type
is read back as a plain comment, so it cannot add lines to the jobs table definition.
"""

import time

import pytest

import datajoint as dj
from datajoint.declare import declare
from datajoint.jobs import Job


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """A fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema = dj.Schema(f"djtest_typecomment_{backend}_{test_id}"[:64], connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def _plant_column_comment(table, column, sql_type, comment):
    """Set a column comment through SQL and reload the table's heading."""
    conn = table.connection
    adapter = conn.adapter
    if adapter.backend == "postgresql":
        sql = f"COMMENT ON COLUMN {table.full_table_name}.{adapter.quote_identifier(column)} IS %s"
    else:
        sql = f"ALTER TABLE {table.full_table_name} MODIFY {adapter.quote_identifier(column)} {sql_type} NOT NULL COMMENT %s"
    conn.query(sql, args=(comment,))
    table.__class__._heading = dj.heading.Heading(table_info=table.heading.table_info)


def _all_tables(schema):
    """Every table in the schema, hidden ones included."""
    adapter = schema.connection.adapter
    return {row[0] for row in schema.connection.query(adapter.list_tables_sql(schema.database)).fetchall()}


def _attribute_lines(definition):
    """Lines of a jobs table definition above the primary-key divider."""
    lines = [line.strip() for line in definition.strip().splitlines()]
    return [line for line in lines[: lines.index("---")] if not line.startswith("#")]


@pytest.mark.parametrize("comment", [":int32\n-> Other:", ":<blob>\n-> Other:"])
def test_line_break_in_comment_type(schema_by_backend, comment):
    """A planted multi-line marker stays a plain comment and adds nothing to the jobs table."""
    schema = schema_by_backend

    @schema
    class Other(dj.Manual):
        definition = """
        other : int32
        """

    @schema
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    @schema
    class Result(dj.Computed):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            self.insert1(dict(key, value=key["item"]))

    _plant_column_comment(Result(), "item", "int", comment)
    attr = Result.heading["item"]
    assert attr.original_type is None
    assert attr.codec is None
    assert attr.comment == comment

    definition = Job(Result()).definition
    assert _attribute_lines(definition) == [f"item : {attr.type}"]
    assert "Other" not in definition

    tables = _all_tables(schema)
    Item.insert1({"item": 1})
    Result.populate(reserve_jobs=True)
    assert Result().to_dicts() == [{"item": 1, "value": 1}]
    jobs = Result.jobs
    assert _all_tables(schema) - tables == {jobs.table_name}
    assert jobs.heading.primary_key == ["item"]
    assert jobs.parents() == []


def test_declared_types_round_trip(schema_by_backend):
    """Core types, codec types and ordinary comments round-trip through describe() and alter()."""
    schema = schema_by_backend

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32   # subject number
        ---
        name : varchar(64)   # full name: given, family
        """

    @schema
    class Sample(dj.Manual):
        definition = """
        -> Subject
        sample_id : uuid
        ---
        weight : float64     # grams
        ratio : decimal(8, 3)   # ratio 1:2
        state : enum('fresh', 'frozen')
        taken : datetime(3)
        flagged : bool
        data = null : <blob>   # raw data
        note = null : varchar(255)
        """

    @schema
    class Analysis(dj.Computed):
        definition = """
        -> Sample
        ---
        score : float32
        """

        def make(self, key):
            self.insert1(dict(key, score=1.0))

    adapter = schema.connection.adapter
    context = dict(Subject=Subject, Sample=Sample)
    for table in (Subject(), Sample()):
        expected = declare(table.full_table_name, table.definition, context, adapter)[0]
        described = declare(table.full_table_name, table.describe(context=context), context, adapter)[0]
        assert described == expected

    heading = Sample.heading
    assert heading["subject_id"].original_type == "int32"
    assert heading["sample_id"].original_type == "uuid"
    assert heading["sample_id"].uuid
    assert heading["weight"].original_type == "float64"
    assert heading["weight"].comment == "grams"
    assert heading["ratio"].original_type == "decimal(8, 3)"
    assert heading["ratio"].comment == "ratio 1:2"
    assert heading["flagged"].original_type == "bool"
    assert heading["data"].codec is not None
    assert heading["data"].codec.name == "blob"
    assert heading["data"].comment == "raw data"
    assert Subject.heading["name"].comment == "full name: given, family"

    assert _attribute_lines(Job(Analysis()).definition) == ["subject_id : int32", "sample_id : uuid"]

    Sample.definition = Sample.definition.replace(
        "note = null : varchar(255)", "note = null : varchar(255)\n        size = 0 : int16"
    )
    Sample.alter(prompt=False, context=context)
    heading = Sample.heading
    assert heading["size"].original_type == "int16"
    assert heading["sample_id"].original_type == "uuid"
    assert heading["data"].codec.name == "blob"
    assert heading["ratio"].comment == "ratio 1:2"
