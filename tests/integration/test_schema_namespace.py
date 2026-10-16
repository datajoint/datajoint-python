"""
``database.name`` is the namespace of a connection's schemas on both backends.

On PostgreSQL it selects the database. On MySQL, which has no level between the
server and its schemas, it becomes a prefix that DataJoint adds to every schema
name. Either way, a pipeline writes plain schema names, and two namespaces can
hold schemas with the same name.
"""

import warnings

import pytest

import datajoint as dj


@pytest.fixture
def namespaces(connection_by_backend, db_creds_by_backend, backend, prefix):
    """Two namespaces on the same server, and a connection to each."""
    names = [f"{prefix}_ns_a", f"{prefix}_ns_b"]
    root = connection_by_backend
    if backend == "postgresql":
        for name in names:
            root.query(f'DROP DATABASE IF EXISTS "{name}"')
            root.query(f'CREATE DATABASE "{name}"')
    connections = {
        name: dj.Connection(
            host=db_creds_by_backend["host"],
            user=db_creds_by_backend["user"],
            password=db_creds_by_backend["password"],
            database_name=name,
        )
        for name in names
    }
    yield connections
    for conn in connections.values():
        conn.close()
    if backend == "postgresql":
        for name in names:
            root.query(f'DROP DATABASE IF EXISTS "{name}"')
    else:
        for name in names:
            for schema_name in dj.list_schemas(connection=root):
                if schema_name.startswith(f"{name}_"):
                    root.query(f"DROP DATABASE `{schema_name}`")


def _declare_subject(conn, schema_name):
    schema = dj.Schema(schema_name, connection=conn)

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    return schema, Subject


@pytest.mark.backend_agnostic
def test_plain_schema_names_in_each_namespace(namespaces, connection_by_backend, backend):
    (name_a, conn_a), (name_b, conn_b) = namespaces.items()
    schema_a, subject_a = _declare_subject(conn_a, "subject")
    schema_b, subject_b = _declare_subject(conn_b, "subject")

    subject_a.insert1({"subject_id": 1})
    subject_b.insert([{"subject_id": 2}, {"subject_id": 3}])

    # the same schema name in two namespaces holds two separate tables
    assert len(subject_a()) == 1
    assert len(subject_b()) == 2

    # each namespace lists the name the pipeline wrote
    assert "subject" in dj.list_schemas(connection=conn_a)
    assert "subject" in dj.list_schemas(connection=conn_b)

    if backend == "mysql":
        assert schema_a.database == f"{name_a}_subject"
        assert schema_b.database == f"{name_b}_subject"
        # a connection without a namespace sees the prefixed names
        server_schemas = dj.list_schemas(connection=connection_by_backend)
        assert {f"{name_a}_subject", f"{name_b}_subject"} <= set(server_schemas)
    else:
        assert schema_a.database == schema_b.database == "subject"


@pytest.mark.backend_agnostic
def test_virtual_module_resolves_in_namespace(namespaces):
    (_, conn_a), _ = namespaces.items()
    _, subject = _declare_subject(conn_a, "subject")
    subject.insert1({"subject_id": 7})

    module = dj.VirtualModule("subject", "subject", connection=conn_a)
    assert module.Subject.to_arrays("subject_id").tolist() == [7]


def test_prefixed_name_is_used_as_is(namespaces, backend):
    if backend != "mysql":
        pytest.skip("only MySQL adds a prefix")
    (name_a, conn_a), _ = namespaces.items()
    with pytest.warns(UserWarning, match="already starts with the prefix"):
        schema = dj.Schema(f"{name_a}_subject", connection=conn_a)
    assert schema.database == f"{name_a}_subject"


def test_no_warning_for_database_name_on_mysql(db_creds_by_backend, connection_by_backend, backend, prefix):
    if backend != "mysql":
        pytest.skip("MySQL only")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        conn = dj.Connection(
            host=db_creds_by_backend["host"],
            user=db_creds_by_backend["user"],
            password=db_creds_by_backend["password"],
            database_name=f"{prefix}_ns_a",
        )
    conn.close()
    assert not [w for w in caught if "database.name" in str(w.message)]
