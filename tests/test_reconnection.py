"""
Collection of test cases to test connection module.
"""

import pytest
import datajoint as dj
from datajoint import DataJointError
from . import CONN_INFO


@pytest.fixture
def conn(connection_root):
    return dj.conn(reset=True, **CONN_INFO)


class TestReconnect:
    """
    Test reconnection
    """

    def test_close(self, conn):
        assert conn.is_connected, "Connection should be alive"
        conn.close()
        assert not conn.is_connected, "Connection should now be closed"

    def test_reconnect(self, conn):
        assert conn.is_connected, "Connection should be alive"
        conn.close()
        conn.query("SHOW DATABASES;", reconnect=True).fetchall()
        assert conn.is_connected, "Connection should be alive"

    def test_reconnect_throws_error_in_transaction(self, conn):
        assert conn.is_connected, "Connection should be alive"
        with conn.transaction, pytest.raises(DataJointError):
            conn.close()
            conn.query("SHOW DATABASES;", reconnect=True).fetchall()
