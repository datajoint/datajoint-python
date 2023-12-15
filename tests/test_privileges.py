import os
import pytest
import datajoint as dj
from . import schema, schema_privileges

namespace = locals()


@pytest.fixture
def schema_priv(connection_test):
    schema = dj.Schema(
        context=schema_privileges.LOCALS_PRIV,
        connection=connection_test,
    )
    schema(schema_privileges.Parent)
    schema(schema_privileges.Child)
    schema(schema_privileges.NoAccess)
    schema(schema_privileges.NoAccessAgain)
    yield schema
    if schema.is_activated():
        schema.drop()


@pytest.fixture
def connection_djsubset(connection_root, db_creds_root, schema_priv, prefix):
    user = "djsubset"
    conn = dj.conn(**db_creds_root, reset=True)
    schema_priv.activate(f"{prefix}_schema_privileges")
    conn.query(
        f"""
        CREATE USER IF NOT EXISTS '{user}'@'%%'
        IDENTIFIED BY '{user}'
        """
    )
    conn.query(
        f"""
        GRANT SELECT, INSERT, UPDATE, DELETE
        ON `{prefix}_schema_privileges`.`#parent`
        TO '{user}'@'%%'
        """
    )
    conn.query(
        f"""
        GRANT SELECT, INSERT, UPDATE, DELETE
        ON `{prefix}_schema_privileges`.`__child`
        TO '{user}'@'%%'
        """
    )
    conn_djsubset = dj.conn(
        host=db_creds_root["host"],
        user=user,
        password=user,
        reset=True,
    )
    yield conn_djsubset
    conn.query(f"DROP USER {user}")
    conn.query(f"DROP DATABASE {prefix}_schema_privileges")


@pytest.fixture
def connection_djview(connection_root, db_creds_root):
    """
    A connection with only SELECT privilege to djtest schemas.
    Requires connection_root fixture so that `djview` user exists.
    """
    connection = dj.conn(
        host=db_creds_root["host"],
        user="djview",
        password="djview",
        reset=True,
    )
    yield connection


class TestUnprivileged:
    def test_fail_create_schema(self, connection_djview):
        """creating a schema with no CREATE privilege"""
        with pytest.raises(dj.DataJointError):
            return dj.Schema(
                "forbidden_schema", namespace, connection=connection_djview
            )

    def test_insert_failure(self, connection_djview, schema_any):
        unprivileged = dj.Schema(
            schema_any.database, namespace, connection=connection_djview
        )
        unprivileged.spawn_missing_classes()
        assert issubclass(Language, dj.Lookup) and len(Language()) == len(
            schema.Language()
        ), "failed to spawn missing classes"
        with pytest.raises(dj.DataJointError):
            Language().insert1(("Socrates", "Greek"))

    def test_failure_to_create_table(self, connection_djview, schema_any):
        unprivileged = dj.Schema(
            schema_any.database, namespace, connection=connection_djview
        )

        @unprivileged
        class Try(dj.Manual):
            definition = """  # should not matter really
            id : int
            ---
            value : float
            """

        with pytest.raises(dj.DataJointError):
            Try().insert1((1, 1.5))


class TestSubset:
    def test_populate_activate(self, connection_djsubset, schema_priv, prefix):
        schema_priv.activate(
            f"{prefix}_schema_privileges", create_schema=True, create_tables=False
        )
        schema_privileges.Child.populate()
        assert schema_privileges.Child.progress(display=False)[0] == 0
