import os
import pytest
import importlib
import datajoint as dj
from . import schema, CONN_INFO_ROOT, PREFIX
from . import schema_privileges

namespace = locals()

@pytest.fixture
def connection_djview(connection_root):
    """
    A connection with only SELECT privilege to djtest schemas.
    Requires connection_root fixture so that `djview` user exists.
    """
    connection = dj.conn(
        host=os.getenv("DJ_HOST"),
        user="djview",
        password="djview",
        reset=True,
    )
    yield connection


class TestUnprivileged:
    def test_fail_create_schema(self, connection_djview):
        """creating a schema with no CREATE privilege"""
        with pytest.raises(dj.DataJointError):
            return dj.Schema("forbidden_schema", namespace, connection=connection_djview)

    def test_insert_failure(self, connection_djview, schema_any):
        unprivileged = dj.Schema(
            schema_any.database, namespace, connection=connection_djview
        )
        unprivileged.spawn_missing_classes()
        assert issubclass(Language, dj.Lookup) and len(Language()) == len(schema.Language()), "failed to spawn missing classes"
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
    USER = "djsubset"

    @classmethod
    def setup_class(cls):
        conn = dj.conn(
            host=CONN_INFO_ROOT["host"],
            user=CONN_INFO_ROOT["user"],
            password=CONN_INFO_ROOT["password"],
            reset=True,
        )
        schema_privileges.schema.activate(f"{PREFIX}_schema_privileges")
        conn.query(
            f"""
            CREATE USER IF NOT EXISTS '{cls.USER}'@'%%'
            IDENTIFIED BY '{cls.USER}'
            """
        )
        conn.query(
            f"""
            GRANT SELECT, INSERT, UPDATE, DELETE
            ON `{PREFIX}_schema_privileges`.`#parent`
            TO '{cls.USER}'@'%%'
            """
        )
        conn.query(
            f"""
            GRANT SELECT, INSERT, UPDATE, DELETE
            ON `{PREFIX}_schema_privileges`.`__child`
            TO '{cls.USER}'@'%%'
            """
        )
        cls.connection = dj.conn(
            host=CONN_INFO_ROOT["host"],
            user=cls.USER,
            password=cls.USER,
            reset=True,
        )

    @classmethod
    def teardown_class(cls):
        conn = dj.conn(
            host=CONN_INFO_ROOT["host"],
            user=CONN_INFO_ROOT["user"],
            password=CONN_INFO_ROOT["password"],
            reset=True,
        )
        conn.query(f"DROP USER {cls.USER}")
        conn.query(f"DROP DATABASE {PREFIX}_schema_privileges")

    def test_populate_activate(self):
        importlib.reload(schema_privileges)
        schema_privileges.schema.activate(
            f"{PREFIX}_schema_privileges", create_schema=True, create_tables=False
        )
        schema_privileges.Child.populate()
        assert schema_privileges.Child.progress(display=False)[0] == 0
