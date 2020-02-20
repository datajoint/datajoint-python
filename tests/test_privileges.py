from nose.tools import assert_true, raises
import datajoint as dj
from os import environ
from . import schema, CONN_INFO

namespace = locals()


class TestUnprivileged:

    @classmethod
    def setup_class(cls):
        """A connection with only SELECT privilege to djtest schemas"""
        cls.connection = dj.conn(host=CONN_INFO['host'], user='djview', password='djview',
                                    reset=True)

    @raises(dj.DataJointError)
    def test_fail_create_schema(self):
        """creating a schema with no CREATE privilege"""
        return dj.Schema('forbidden_schema', namespace, connection=self.connection)

    @raises(dj.DataJointError)
    def test_insert_failure(self):
        unprivileged = dj.Schema(schema.schema.database, namespace, connection=self.connection)
        unprivileged.spawn_missing_classes()
        assert_true(issubclass(Language, dj.Lookup) and len(Language()) == len(schema.Language()),
                    'failed to spawn missing classes')
        Language().insert1(('Socrates', 'Greek'))

    @raises(dj.DataJointError)
    def test_failure_to_create_table(self):
        unprivileged = dj.Schema(schema.schema.database, namespace, connection=self.connection)

        @unprivileged
        class Try(dj.Manual):
            definition = """  # should not matter really
            id : int
            ---
            value : float
            """

        Try().insert1((1, 1.5))
