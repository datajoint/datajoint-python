"""
Tests that dropping an auto-populated table drops its jobs table, that declaring
a table drops a jobs table left by an earlier table of the same name, and that
``jobs`` rejects a leftover jobs table keyed differently (#1601).
"""

import logging
import re
import time
from unittest import mock

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


def test_cascading_drop_removes_only_dropped_tables_jobs(schema_by_backend):
    """A cascading drop through a virtual module removes the jobs table of each dropped table, and no other."""
    schema = schema_by_backend

    @schema
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    @schema
    class Stage1(dj.Computed):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            self.insert1(dict(key, value=key["item"]))

    @schema
    class Stage2(dj.Computed):
        definition = """
        -> Stage1
        ---
        value : int32
        """

        def make(self, key):
            raise RuntimeError("not ready")

    @schema
    class Other(dj.Computed):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            raise RuntimeError("not ready")

    Item.insert({"item": i} for i in range(3))
    for table in (Stage1, Stage2, Other):
        table.populate(reserve_jobs=True, suppress_errors=True)
    assert len(Stage2.jobs.errors) == len(Other.jobs.errors) == 3
    assert {"~~stage1", "~~stage2", "~~other"} <= _table_names(schema)

    # A virtual module's classes have never accessed .jobs
    module = dj.VirtualModule("module", schema.database, connection=schema.connection)
    module.Stage1.drop(prompt=False)

    tables = _table_names(schema)
    assert not {"__stage1", "__stage2", "~~stage1", "~~stage2"} & tables
    assert {"__other", "~~other"} <= tables
    # The remaining jobs table keeps its records and, on PostgreSQL, its status column
    other_jobs = dj.FreeTable(schema.connection, Other.jobs.full_table_name)
    assert "status" in other_jobs.heading.names
    assert sorted(other_jobs.to_arrays("status")) == ["error"] * 3


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

    # ...and declared the new table without removing the leftover jobs table
    with mock.patch("datajoint.table._own_jobs_table", return_value=None):

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


@pytest.mark.parametrize("wider_key", [False, True])
def test_declare_drops_leftover_jobs_table(schema_by_backend, caplog, wider_key):
    """
    Declaring a table drops a jobs table left under its name by an earlier table,
    whether or not the primary keys match, so populate computes every key.
    """
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
        contents = [{"param": 1}]

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
    # Earlier releases dropped the table and left its jobs table; drop it the same way
    schema.connection.query(f"DROP TABLE {Result.full_table_name}")
    assert "~~result" in _table_names(schema)

    with caplog.at_level(logging.WARNING, logger="datajoint"):

        @schema
        class Result(dj.Computed):  # same name, same primary key or one that includes Param
            definition = f"""
            -> Item
            {"-> Param" if wider_key else ""}
            ---
            value : int32
            """

            def make(self, key):
                self.insert1(dict(key, value=1))

    assert "~~result" not in _table_names(schema)
    assert any("left by an earlier table" in r.getMessage() for r in caplog.records)
    # The three error records would have kept every key from being computed
    Result.populate(reserve_jobs=True)
    assert len(Result()) == 3


def _namesakes(schema, computed_wider_key=False):
    """An Imported `_result` and a Computed `__result`, which map to the same jobs table `~~result`."""

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
        contents = [{"param": 1}]

    @schema
    class Result(dj.Computed):
        definition = f"""
        -> Item
        {"-> Param" if computed_wider_key else ""}
        ---
        value : int32
        """

        def make(self, key):
            raise RuntimeError("not ready")

    computed = Result
    Item.insert({"item": i} for i in range(3))
    computed.populate(reserve_jobs=True, suppress_errors=True)
    assert len(computed.jobs.errors) == 3

    @schema
    class Result(dj.Imported):
        definition = """
        -> Item
        ---
        value : int32
        """

        def make(self, key):
            self.insert1(dict(key, value=1))

    return Item, computed, Result


@pytest.mark.parametrize("first", ["imported", "computed"])
def test_drop_keeps_jobs_table_shared_with_namesake(schema_by_backend, caplog, first):
    """
    Dropping either of an Imported `_result` and a Computed `__result` keeps their
    shared jobs table and its records; dropping the other then drops it.
    """
    schema = schema_by_backend
    _, computed, imported = _namesakes(schema)
    # Declaring the Imported table kept the jobs table of the Computed one
    assert {"_result", "__result", "~~result"} <= _table_names(schema)
    jobs = dj.FreeTable(schema.connection, computed.jobs.full_table_name)
    assert sorted(jobs.to_arrays("status")) == ["error"] * 3

    dropped_first, dropped_second = (imported, computed) if first == "imported" else (computed, imported)
    with caplog.at_level(logging.WARNING, logger="datajoint"):
        dropped_first.drop(prompt=False)
    assert dropped_first.table_name not in _table_names(schema)
    assert "~~result" in _table_names(schema)
    assert sorted(jobs.to_arrays("status")) == ["error"] * 3
    assert any("is shared by" in r.getMessage() for r in caplog.records)

    dropped_second.drop(prompt=False)
    assert not {"_result", "__result", "~~result"} & _table_names(schema)


def test_cascading_drop_of_both_namesakes_drops_shared_jobs_table(schema_by_backend):
    """A drop that removes both namesakes removes their shared jobs table."""
    schema = schema_by_backend
    item, _, _ = _namesakes(schema)
    item.drop(prompt=False)
    assert not {"_result", "__result", "~~result"} & _table_names(schema)


def test_jobs_rejects_jobs_table_shared_with_namesake_of_other_key(schema_by_backend):
    """jobs raises on a jobs table shared with a namesake keyed differently, and suggests no drop."""
    schema = schema_by_backend
    _, computed, imported = _namesakes(schema, computed_wider_key=True)
    assert computed.jobs.primary_key == ["item", "param"]
    with pytest.raises(DataJointError, match="share this jobs table") as excinfo:
        imported().jobs
    assert "drop_quick" not in str(excinfo.value)
    jobs = dj.FreeTable(schema.connection, computed.jobs.full_table_name)
    assert sorted(jobs.to_arrays("status")) == ["error"] * 3
