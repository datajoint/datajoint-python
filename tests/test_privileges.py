from nose.tools import assert_false, assert_true, raises
import datajoint as dj
from os import environ
from . import schema

namespace = locals()


class TestUnprivileged:

    def __init__(self):
        self.connection = None
        self.previous_connection = None

    def setUp(self):
        # a connection with only SELECT privileges.  This user must be defined through mysql with only SELECT
        # privileges to djtest schemas
        view_connection = dict(
            host=environ.get('DJ_TEST_HOST', 'localhost'),
            user='djview',
            password='djview')
        self.previous_connection = dj.conn.connection if hasattr(dj.conn, 'connection') else None
        self.connection = dj.conn(**view_connection, reset=True)

    def tearDown(self):
        # restore connection
        if self.previous_connection is None:
            del dj.conn.connection
        else:
            dj.conn.connection = self.previous_connection

    @raises(dj.DataJointError)
    def test_fail_create_schema(self):
        """creating a schema with no CREATE privilege"""
        return dj.schema('forbidden_schema', namespace, connection=self.connection)

    @raises(dj.DataJointError)
    def test_insert_failure(self):
        unprivileged = dj.schema(schema.schema.database, namespace, connection=self.connection)
        unprivileged.spawn_missing_classes()
        assert_true(issubclass(Language, dj.Lookup) and len(Language()) == len(schema.Language()),
                    'failed to spawn missing classes')
        Language().insert1(('Socrates', 'Greek'))

    @raises(dj.DataJointError)
    def test_failure_to_create_table(self):
        unprivileged = dj.schema(schema.schema.database, namespace, connection=self.connection)

        @unprivileged
        class Try(dj.Manual):
            definition = """  # should not matter really
            id : int
            ---
            value : float
            """

        Try().insert((1, 1.5))

