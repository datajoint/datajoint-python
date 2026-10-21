"""
Tests that dropping an auto-populated table drops its jobs table, and that
``jobs`` rejects a jobs table left by an earlier table of the same name (#1601).
"""

import logging
import re
import time

import pytest

import datajoint as dj
from datajoint.errors import DataJointError
from datajoint.lineage import get_table_lineages


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema = dj.Schema(f"djtest_dropjobs_{backend}_{test_id}"[:64], connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def _table_names(schema):
    """Every table in the schema, hidden ones included."""
    sql = schema.connection.adapter.list_tables_sql(schema.database)
    return {row[0] for row in schema.connection.query(sql).fetchall()}


def _enum_types(schema, table_name):
    """PostgreSQL enum types used by a table's columns (none on MySQL)."""
    adapter = schema.connection.adapter
    if not hasattr(adapter, "get_table_enum_types_sql"):
        return set()
    sql = adapter.get_table_enum_types_sql(schema.database, table_name)
    return {row[0] for row in schema.connection.query(sql).fetchall()}


def _existing_enum_types(schema):
    """Enum types defined in the schema's namespace (PostgreSQL only)."""
    sql = (
        "SELECT n.nspname || '.' || t.typname FROM pg_catalog.pg_type t "
        "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
        "WHERE t.typtype = 'e' AND n.nspname = %s"
    )
    return {row[0] for row in schema.connection.query(sql, args=(schema.database,)).fetchall()}


def test_drop_removes_jobs_table(schema_by_backend):
    """drop() removes the jobs table, so a table declared again under the same name populates in full."""
    schema = schema_by_backend

    @schema
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    @schema
    class Param(dj.Lookup):
        definition = """
        param : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Result(dj.Computed):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            raise RuntimeError("not ready")

    Item.insert({"item": i} for i in range(3))
    Result.populate(reserve_jobs=True, suppress_errors=True)
    assert len(Result.jobs.errors) == 3
    assert get_table_lineages(schema.connection, schema.database, "~~result")

    Result.drop(prompt=False)
    assert "~~result" not in _table_names(schema)
    assert not get_table_lineages(schema.connection, schema.database, "~~result")

    @schema
    class Result(dj.Computed):  # same name, primary key now includes Param
        definition = """
        -> Item
        -> Param
        ---
        value : int32
        """

        def make(self, key):
            self.insert1(dict(key, value=1))

    assert Result.jobs.primary_key == ["item", "param"]
    assert len(Result.jobs) == 0
    Result.populate(reserve_jobs=True)
    assert len(Result()) == len(Result.key_source) == 6


def test_drop_quick_removes_jobs_table(schema_by_backend):
    """drop_quick() removes the jobs table with its ~lineage rows and, on PostgreSQL, its enum type."""
    schema = schema_by_backend

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

    Item.insert({"item": i} for i in range(3))
    Result.jobs.refresh()
    assert "~~result" in _table_names(schema)
    enum_types = _enum_types(schema, "~~result")
    assert bool(enum_types) == (schema.connection.adapter.backend == "postgresql")

    Result.drop_quick()
    tables = _table_names(schema)
    assert "__result" not in tables
    assert "~~result" not in tables
    assert not get_table_lineages(schema.connection, schema.database, "~~result")
    if enum_types:
        assert not enum_types & _existing_enum_types(schema)


def test_drop_manual_table_keeps_jobs_table(schema_by_backend):
    """Dropping a Manual table leaves the jobs table its name maps to, which belongs to a Computed table."""
    schema = schema_by_backend

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

    computed = Result
    Item.insert({"item": i} for i in range(3))
    computed.jobs.refresh()
    assert len(computed.jobs) == 3

    @schema
    class Result(dj.Manual):  # table `result`; `~~result` belongs to `__result`
        definition = """
        result_id : int32
        """

    Result.drop(prompt=False)
    tables = _table_names(schema)
    assert "result" not in tables
    assert {"__result", "~~result"} <= tables
    assert len(computed.jobs) == 3


def test_jobs_rejects_stale_jobs_table(schema_by_backend):
    """jobs raises on a jobs table keyed differently from the table's, and its hint removes it."""
    schema = schema_by_backend

    @schema
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    @schema
    class Param(dj.Lookup):
        definition = """
        param : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Result(dj.Computed):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            raise RuntimeError("not ready")

    Item.insert({"item": i} for i in range(3))
    Result.populate(reserve_jobs=True, suppress_errors=True)
    jobs_table = Result.jobs.full_table_name
    # Earlier releases dropped the table and left its jobs table; drop it the same way
    schema.connection.query(f"DROP TABLE {Result.full_table_name}")

    @schema
    class Result(dj.Computed):  # same name, primary key now includes Param
        definition = """
        -> Item
        -> Param
        ---
        value : int32
        """

        def make(self, key):
            self.insert1(dict(key, value=1))

    result = Result()
    with pytest.raises(DataJointError, match="left by an earlier table of the same name") as excinfo:
        result.jobs
    message = str(excinfo.value)
    assert jobs_table in message
    assert "['item']" in message and "['item', 'param']" in message
    assert result._jobs is None  # the mismatched jobs table is not kept

    # The hint runs as written
    hint = re.search(r"dj\.FreeTable\(.*\)\.drop_quick\(\)", message).group(0)
    eval(hint, {"dj": dj, "Result": Result})
    assert result.jobs.primary_key == ["item", "param"]
    Result.populate(reserve_jobs=True)
    assert len(Result()) == len(Result.key_source) == 6


def test_jobs_accepts_table_with_native_key_attribute(schema_by_backend, caplog):
    """A jobs table keyed on the FK-derived part of the primary key passes the check, warning once."""
    schema = schema_by_backend

    @schema
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    with dj.config.override(jobs__allow_new_pk_fields_in_computed_tables=True):

        @schema
        class Result(dj.Computed):
            definition = """
            -> Item
            index : int32
            ---
            value : int32
            """

    Result.jobs.refresh()  # declares the jobs table, keyed on item alone
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="datajoint"):
        assert Result.jobs.primary_key == ["item"]
    warnings = [r for r in caplog.records if "Ignoring non-FK primary key attribute 'index'" in r.getMessage()]
    assert len(warnings) == 1
