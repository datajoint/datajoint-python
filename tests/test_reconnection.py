"""
Collection of test cases to test connection module.
"""

from nose.tools import assert_true, assert_false, assert_equal, raises
import datajoint as dj
from datajoint import DataJointError
from . import CONN_INFO


class TestReconnect:
    """
    test reconnection
    """

    def setup(self):
        self.conn = dj.conn(reset=True, **CONN_INFO)

    def test_close(self):
        assert_true(self.conn.is_connected, "Connection should be alive")
        self.conn.close()
        assert_false(self.conn.is_connected, "Connection should now be closed")

    def test_reconnect(self):
        assert_true(self.conn.is_connected, "Connection should be alive")
        self.conn.close()
        self.conn.query('SHOW DATABASES;', reconnect=True).fetchall()
        assert_true(self.conn.is_connected, "Connection should be alive")

    @raises(DataJointError)
    def test_reconnect_throws_error_in_transaction(self):
        assert_true(self.conn.is_connected, "Connection should be alive")
        with self.conn.transaction:
            self.conn.close()
            self.conn.query('SHOW DATABASES;', reconnect=True).fetchall()
