import importlib
import datajoint as dj
from . import schema, CONN_INFO_ROOT, PREFIX
from . import schema_privileges as pipeline
from nose.tools import assert_true, raises

namespace = locals()


class TestUnprivileged:
    @classmethod
    def setup_class(cls):
        """A connection with only SELECT privilege to djtest schemas"""
        cls.connection = dj.conn(
            host=CONN_INFO_ROOT["host"], user="djview", password="djview", reset=True
        )

    @raises(dj.DataJointError)
    def test_fail_create_schema(self):
        """creating a schema with no CREATE privilege"""
        return dj.Schema("forbidden_schema", namespace, connection=self.connection)

    @raises(dj.DataJointError)
    def test_insert_failure(self):
        unprivileged = dj.Schema(
            schema.schema.database, namespace, connection=self.connection
        )
        unprivileged.spawn_missing_classes()
        assert_true(
            issubclass(Language, dj.Lookup)
            and len(Language()) == len(schema.Language()),
            "failed to spawn missing classes",
        )
        Language().insert1(("Socrates", "Greek"))

    @raises(dj.DataJointError)
    def test_failure_to_create_table(self):
        unprivileged = dj.Schema(
            schema.schema.database, namespace, connection=self.connection
        )

        @unprivileged
        class Try(dj.Manual):
            definition = """  # should not matter really
            id : int
            ---
            value : float
            """

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
        pipeline.schema.activate(f"{PREFIX}_pipeline")
        conn.query(
            f"""
            CREATE USER IF NOT EXISTS '{cls.USER}'@'%%'
            IDENTIFIED BY '{cls.USER}'
            """
        )
        conn.query(
            f"""
            GRANT SELECT, INSERT, UPDATE, DELETE
            ON `{PREFIX}_pipeline`.`#parent`
            TO '{cls.USER}'@'%%'
            """
        )
        conn.query(
            f"""
            GRANT SELECT, INSERT, UPDATE, DELETE
            ON `{PREFIX}_pipeline`.`__child`
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
        conn.query(f"DROP DATABASE {PREFIX}_pipeline")

    def test_populate_activate(self):
        importlib.reload(pipeline)
        pipeline.schema.activate(
            f"{PREFIX}_pipeline", create_schema=True, create_tables=False
        )
        pipeline.Child.populate()
        assert pipeline.Child.progress(display=False)[0] == 0
