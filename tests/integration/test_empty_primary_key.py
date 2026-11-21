"""
Queries whose primary key is empty: ``dj.U() & table`` and ``dj.U().aggr(...)``.

An expression with an empty primary key and no attributes holds at most one
row. ``len()`` must count that row (#1581), and a restriction on a ``dj.U()``
aggregation must filter it (#1580). A table created outside DataJoint without a
primary key also has an empty primary key, but any number of rows, duplicates
included: ``len()`` must count them all. All tests run on MySQL and PostgreSQL.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_emptypk_{backend}_{test_id}"[:64]

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


@pytest.fixture
def File(schema_by_backend):
    @schema_by_backend
    class File(dj.Manual):
        definition = """
        file_id : int32
        ---
        kind : varchar(16)
        """

    return File


FILES = [dict(file_id=1, kind="data"), dict(file_id=2, kind="data"), dict(file_id=3, kind="meta")]


def test_len_of_universal_restriction(File):
    """``dj.U() & table`` has one row when the table is non-empty, none otherwise."""
    assert len(dj.U() & File) == 0
    File.insert(FILES)
    assert len(dj.U() & File) == 1
    assert len(dj.U() & (File & "kind = 'meta'")) == 1
    assert len(dj.U() & (File & "kind = 'none'")) == 0


def test_len_of_universal_aggregation_subquery(File):
    """``len()`` of a subquery over a ``dj.U()`` aggregation, restricted or not."""
    File.insert(FILES)
    total = dj.U().aggr(File, n="count(*)").make_subquery()
    assert len(total) == 1
    assert len(total & "n = 3") == 1
    assert len(total & "n = 4") == 0


def test_restrict_universal_aggregation(File):
    """A restriction on the aggregated value of a ``dj.U()`` aggregation filters its row."""
    File.insert(FILES)
    total = dj.U().aggr(File, n="count(*)")

    assert (total & "n = 3").to_dicts() == [{"n": 3}]
    assert (total & {"n": 3}).to_dicts() == [{"n": 3}]
    assert (total - "n = 4").to_dicts() == [{"n": 3}]
    assert (total & "n = 4").to_dicts() == []
    assert (total & {"n": 4}).to_dicts() == []
    assert (total - "n = 3").to_dicts() == []
    assert (total & False).to_dicts() == []

    assert len(total & "n = 3") == 1
    assert len(total & "n = 4") == 0
    assert total & "n = 3"
    assert not total & "n = 4"


def test_restrict_universal_aggregation_of_restricted_group(File):
    """Conditions on the group (WHERE) and on the aggregate (HAVING) combine."""
    File.insert(FILES)
    data_count = dj.U().aggr(File & "kind = 'data'", n="count(*)")
    assert (data_count & "n = 2").to_dicts() == [{"n": 2}]
    assert (data_count & "n = 3").to_dicts() == []


def test_restrict_by_restricted_universal_aggregation(File):
    """A restricted ``dj.U()`` aggregation shares no attributes with ``File``:
    it keeps every row when non-empty and none when empty."""
    File.insert(FILES)
    total = dj.U().aggr(File, n="count(*)")
    assert len(File & (total & "n = 3")) == 3
    assert len(File & (total & "n = 4")) == 0
    assert len(File - (total & "n = 4")) == 3


def test_restrict_grouped_aggregation(File):
    """Restricting an aggregation with grouping attributes still filters by group."""
    File.insert(FILES)
    per_kind = dj.U("kind").aggr(File, n="count(*)")
    assert (per_kind & "n > 1").to_dicts() == [{"kind": "data", "n": 2}]
    assert len(per_kind & "n > 1") == 1
    assert (per_kind & {"kind": "meta"}).to_dicts() == [{"kind": "meta", "n": 1}]
    assert (per_kind & False).to_dicts() == []


@pytest.fixture
def keyless(schema_by_backend):
    """A table created by plain SQL, without a primary key, holding a duplicate row."""
    connection = schema_by_backend.connection
    name = f"{connection.adapter.quote_identifier(schema_by_backend.database)}.{connection.adapter.quote_identifier('log')}"
    connection.query(f"CREATE TABLE {name} (msg varchar(16) NOT NULL)")
    connection.query(f"INSERT INTO {name} (msg) VALUES ('a'), ('a'), ('b')")
    return dj.FreeTable(connection, name)


def test_len_of_table_without_primary_key(keyless):
    """``len()`` counts every row of a table without a primary key, duplicates included."""
    assert keyless.primary_key == []
    assert len(keyless) == 3
    assert len(keyless) == len(keyless.to_dicts())
    assert len(keyless & "msg = 'a'") == 2
    assert len(keyless & {"msg": "b"}) == 1
    assert len(keyless - {"msg": "a"}) == 1
    assert len(keyless & "msg = 'c'") == 0
    assert len(keyless.proj(m="msg")) == 3


def test_len_of_universal_restriction_of_table_without_primary_key(keyless):
    """``dj.U(...) & table`` deduplicates the rows of a table without a primary key."""
    assert len(dj.U() & keyless) == 1
    assert len(dj.U() & (keyless & "msg = 'c'")) == 0
    assert len(dj.U("msg") & keyless) == 2
