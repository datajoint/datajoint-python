"""
Tests for foreign-key supporting indexes on the PostgreSQL backend (#1512).

MySQL/InnoDB auto-creates an index on every foreign key's referencing columns;
PostgreSQL does not. DataJoint therefore emits an explicit, coverage-aware index
on the Postgres path: one is created when the FK columns are NOT already a
left-prefix of the child's primary key (secondary FKs, and FKs in a non-leading
position of a composite PK), and skipped when they are (a leading primary FK,
already served by the PK index).

These tests are PostgreSQL-specific: on MySQL the equivalent index is created
implicitly by InnoDB, not by DataJoint's DDL, so there is nothing of ours to
assert.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_fkidx_{backend}_{test_id}"[:64]

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


def _fk_support_indexes(conn, schema_name, table_name):
    """Names of the DataJoint-created FK-support indexes (idx_*) on a table."""
    rows = conn.query(
        "SELECT indexname FROM pg_indexes "
        f"WHERE schemaname = '{schema_name}' AND tablename = '{table_name}' "
        "AND indexname LIKE 'idx%%'"
    ).fetchall()
    return {r[0] for r in rows}


def _indexes_covering(conn, schema_name, table_name, column):
    """idx_* indexes on a table whose definition references `column`."""
    rows = conn.query(
        "SELECT indexname, indexdef FROM pg_indexes "
        f"WHERE schemaname = '{schema_name}' AND tablename = '{table_name}' "
        "AND indexname LIKE 'idx%%'"
    ).fetchall()
    return {name for name, defn in rows if column in defn}


def test_secondary_fk_gets_index(schema_by_backend, db_creds_by_backend, connection_by_backend):
    """A secondary foreign key (below the ---) is not in the child PK, so its
    columns are unindexed by the PK; DataJoint must emit a supporting index."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific: MySQL/InnoDB indexes FK columns implicitly")

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        child_id : int32
        ---
        -> Master
        """

    covering = _indexes_covering(connection_by_backend, schema_by_backend.database, Child.table_name, "master_id")
    assert covering, "secondary FK column `master_id` must have a supporting index on PostgreSQL"


def test_nonleading_primary_fk_gets_index(schema_by_backend, db_creds_by_backend, connection_by_backend):
    """A primary FK in a non-leading position of a composite PK (PK = (b_id,
    a_id), a_id from the FK) is not a left-prefix of the PK index, so it needs
    its own index."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific")

    @schema_by_backend
    class A(dj.Manual):
        definition = """
        a_id : int32
        """

    @schema_by_backend
    class B(dj.Manual):
        definition = """
        b_id : int32
        -> A
        """

    covering = _indexes_covering(connection_by_backend, schema_by_backend.database, B.table_name, "a_id")
    assert covering, "non-leading primary FK column `a_id` must have a supporting index on PostgreSQL"


def test_leading_primary_fk_no_redundant_index(schema_by_backend, db_creds_by_backend, connection_by_backend):
    """A leading primary FK (PK = (p_id, q_id), p_id from the FK) is a left-prefix
    of the PK index, which already serves FK lookups; no extra index is created."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific")

    @schema_by_backend
    class P(dj.Manual):
        definition = """
        p_id : int32
        """

    @schema_by_backend
    class Q(dj.Manual):
        definition = """
        -> P
        q_id : int32
        """

    assert not _fk_support_indexes(
        connection_by_backend, schema_by_backend.database, Q.table_name
    ), "a leading primary FK is covered by the PK index; no redundant FK-support index should be created"


def test_secondary_fk_covered_by_declared_index(schema_by_backend, db_creds_by_backend, connection_by_backend):
    """A secondary FK whose columns are a left-prefix of a user-declared index
    needs no separate FK-support index (post-parse coverage vs declared indexes)."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific")

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        child_id : int32
        ---
        -> Master
        note : varchar(16)
        index (master_id, note)
        """

    # The declared index (master_id, note) already left-covers master_id, so no
    # standalone FK-support index on master_id should be created.
    names = _fk_support_indexes(connection_by_backend, schema_by_backend.database, Child.table_name)
    assert (
        f"idx_{Child.table_name}_master_id" not in names
    ), f"a secondary FK covered by a declared index must not get a redundant FK-support index; got {names}"
    assert f"idx_{Child.table_name}_master_id_note" in names, "the declared composite index should still exist"
