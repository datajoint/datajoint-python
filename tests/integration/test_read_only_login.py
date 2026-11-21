"""
A login holding only SELECT on a schema must see the same table structure as
the schema's owner: primary keys, headings, and the dependency graph (#1554).

On PostgreSQL, information_schema hides constraints on tables where the current
user holds only SELECT, so key and foreign-key metadata is read from the system
catalogs instead. The same checks run on MySQL, where a SELECT-only login has
always seen this metadata.
"""

import pytest

import datajoint as dj

READER = "djreadonly"
TABLES = ("Rig", "Subject", "Session", "Pairing", "Analysis")


@pytest.fixture
def owner_schema(connection_by_backend, backend, prefix):
    """A small pipeline declared and populated by the owner."""
    schema = dj.Schema(f"{prefix}_read_only_{backend}", connection=connection_by_backend)

    @schema
    class Rig(dj.Lookup):
        definition = """
        rig : varchar(16)
        """
        contents = [("A",), ("B",)]

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        ---
        species : varchar(32)
        """

    @schema
    class Session(dj.Manual):
        # Primary key order is not alphabetical, so a reordered key would show.
        definition = """
        -> Subject
        session_idx : int16
        ---
        -> Rig
        note = '' : varchar(255)
        """

    @schema
    class Pairing(dj.Manual):
        definition = """
        -> Subject
        -> Subject.proj(partner_id='subject_id')
        """

    @schema
    class Analysis(dj.Computed):
        definition = """
        -> Session
        ---
        result : float64
        """

        def make(self, key):
            self.insert1(dict(key, result=1.0))

    Subject.insert([(1, "mouse"), (2, "rat")], skip_duplicates=True)
    Session.insert([(1, 1, "A", ""), (1, 2, "B", ""), (2, 1, "A", "")], skip_duplicates=True)
    Pairing.insert([(1, 2)], skip_duplicates=True)
    Analysis.populate(Session & "session_idx = 1")

    yield schema
    schema.drop()


def _drop_reader(conn, backend):
    if backend == "postgresql":
        if conn.query("SELECT 1 FROM pg_roles WHERE rolname = %s", args=(READER,)).fetchone():
            conn.query(f"DROP OWNED BY {READER}")
            conn.query(f"DROP ROLE {READER}")
    else:
        conn.query(f"DROP USER IF EXISTS '{READER}'@'%%'")


@pytest.fixture
def reader_connection(owner_schema, connection_by_backend, db_creds_by_backend, backend):
    """A connection whose login holds only SELECT on the owner's schema."""
    owner = connection_by_backend
    database = owner.adapter.quote_identifier(owner_schema.database)
    _drop_reader(owner, backend)
    if backend == "postgresql":
        owner.query(f"CREATE ROLE {READER} LOGIN PASSWORD '{READER}'")
        owner.query(f"GRANT USAGE ON SCHEMA {database} TO {READER}")
        owner.query(f"GRANT SELECT ON ALL TABLES IN SCHEMA {database} TO {READER}")
    else:
        owner.query(f"CREATE USER '{READER}'@'%%' IDENTIFIED BY '{READER}'")
        owner.query(f"GRANT SELECT ON {database}.* TO '{READER}'@'%%'")

    reader = dj.Connection(host=db_creds_by_backend["host"], user=READER, password=READER)
    yield reader
    reader.close()
    _drop_reader(owner, backend)


@pytest.fixture
def owner_and_reader(owner_schema, connection_by_backend, reader_connection):
    """The same schema loaded as virtual modules by the owner and by the reader."""
    owner = dj.VirtualModule("owner", owner_schema.database, connection=connection_by_backend)
    reader = dj.VirtualModule("reader", owner_schema.database, connection=reader_connection)
    return owner, reader


def test_primary_keys(owner_and_reader):
    owner, reader = owner_and_reader
    for name in TABLES:
        assert getattr(owner, name).primary_key, name
        assert getattr(reader, name).primary_key == getattr(owner, name).primary_key, name
    assert reader.Session.primary_key == ["subject_id", "session_idx"]


def test_headings(owner_and_reader):
    owner, reader = owner_and_reader
    for name in TABLES:
        assert getattr(reader, name).heading.attributes == getattr(owner, name).heading.attributes, name


def _graph(conn):
    conn.dependencies.load()
    nodes = dict(conn.dependencies.nodes(data=True))
    edges = {(u, v, k): d for u, v, k, d in conn.dependencies.edges(keys=True, data=True)}
    return nodes, edges


def test_dependencies(owner_and_reader, connection_by_backend, reader_connection):
    owner, reader = owner_and_reader
    for name in TABLES:
        owner_table, reader_table = getattr(owner, name), getattr(reader, name)
        for primary in (None, True, False):
            assert reader_table.parents(primary=primary) == owner_table.parents(primary=primary), (name, primary)
            assert reader_table.children(primary=primary) == owner_table.children(primary=primary), (name, primary)

    nodes, edges = _graph(connection_by_backend)
    assert _graph(reader_connection) == (nodes, edges)
    assert len(edges) == 5  # Subject->Session, Rig->Session, Subject->Pairing (x2), Session->Analysis


def test_queries_that_need_keys(owner_and_reader):
    owner, reader = owner_and_reader
    assert len(reader.Session()) == len(owner.Session()) == 3
    assert len(reader.Analysis.key_source) == 3
    assert len(reader.Analysis.key_source - reader.Analysis) == 1
    assert reader.Session.keys(order_by="KEY") == owner.Session.keys(order_by="KEY")
    assert reader.Session.describe() == owner.Session.describe()


def test_foreign_keys_sql(owner_and_reader, connection_by_backend, reader_connection):
    """``get_foreign_keys_sql`` returns one row per foreign-key column, for any login."""
    owner, _ = owner_and_reader
    adapter = connection_by_backend.adapter
    rows = {}
    for name in TABLES:
        table = getattr(owner, name)
        sql = adapter.get_foreign_keys_sql(table.database, table.table_name)
        rows[name] = connection_by_backend.query(sql).fetchall()
        assert reader_connection.query(sql).fetchall() == rows[name], name
    assert [len(rows[name]) for name in TABLES] == [0, 0, 2, 2, 2]
