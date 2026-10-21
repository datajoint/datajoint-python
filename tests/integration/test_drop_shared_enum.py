"""
Dropping a table must not drop columns of other tables.

On PostgreSQL, an enum attribute is declared with an enum type named by its
values, so every column in the schema with the same values shares one type.
Dropping a table drops its enum types only when no other column uses them.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    schema_name = f"djtest_shared_enum_{backend}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    if connection_by_backend.is_connected:
        connection_by_backend.query(
            f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            if backend == "mysql"
            else f"DROP SCHEMA IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)} CASCADE"
        )


def _columns(schema, table_name):
    rows = schema.connection.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        args=(schema.database, table_name),
    ).fetchall()
    return [row[0] for row in rows]


def _enum_types(schema):
    if schema.connection.adapter.backend != "postgresql":
        return []
    rows = schema.connection.query(
        "SELECT t.typname FROM pg_catalog.pg_type t "
        "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
        "WHERE t.typtype = 'e' AND n.nspname = %s",
        args=(schema.database,),
    ).fetchall()
    return [row[0] for row in rows]


def test_drop_keeps_a_shared_enum_column(schema_by_backend):
    """Two tables declare the same enum. Dropping one keeps the other's column
    and its values; dropping the second removes the type."""

    @schema_by_backend
    class Animal(dj.Manual):
        definition = """
        animal_id : int32
        ---
        species : enum('mouse', 'rat')
        """

    @schema_by_backend
    class Cage(dj.Manual):
        definition = """
        cage_id : int32
        ---
        species : enum('mouse', 'rat')
        """

    Animal.insert([(1, "mouse"), (2, "rat")])
    Cage.insert([(1, "rat")])
    types_before = _enum_types(schema_by_backend)

    Cage().drop_quick()
    assert _columns(schema_by_backend, "animal") == ["animal_id", "species"]
    assert Animal.to_dicts(order_by="animal_id") == [
        {"animal_id": 1, "species": "mouse"},
        {"animal_id": 2, "species": "rat"},
    ]
    assert _enum_types(schema_by_backend) == types_before

    Animal().drop_quick()
    assert _enum_types(schema_by_backend) == []


def test_dropping_a_jobs_table_keeps_other_jobs_tables_whole(schema_by_backend):
    """Every jobs table's status column has the same enum. Dropping one jobs
    table leaves the others' status columns, and their jobs, in place."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item : int32
        """

    @schema_by_backend
    class First(dj.Computed):
        definition = """
        -> Item
        """

        def make(self, key):
            self.insert1(key)

    @schema_by_backend
    class Second(dj.Computed):
        definition = """
        -> Item
        """

        def make(self, key):
            raise RuntimeError("not ready")

    Item.insert([(1,), (2,)])
    First.populate(reserve_jobs=True)
    Second.populate(reserve_jobs=True, suppress_errors=True)

    First.jobs.drop_quick()
    assert "status" in _columns(schema_by_backend, "~~second")
    assert sorted(Second.jobs.to_arrays("status").tolist()) == ["error", "error"]
