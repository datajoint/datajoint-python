"""
Collection of test cases to test connection module.
"""

import pytest

import datajoint as dj
from datajoint import DataJointError


@pytest.fixture
def conn(connection_root, db_creds_root):
    return dj.conn(reset=True, **db_creds_root)


def test_close(conn):
    assert conn.is_connected, "Connection should be alive"
    conn.close()
    assert not conn.is_connected, "Connection should now be closed"


def test_reconnect(conn):
    assert conn.is_connected, "Connection should be alive"
    conn.close()
    conn.query("SHOW DATABASES;", reconnect=True).fetchall()
    assert conn.is_connected, "Connection should be alive"


def test_reconnect_throws_error_in_transaction(conn):
    assert conn.is_connected, "Connection should be alive"
    with conn.transaction, pytest.raises(DataJointError):
        conn.close()
        conn.query("SHOW DATABASES;", reconnect=True).fetchall()


def test_reconnect_after_close_by_backend(connection_by_backend):
    """A closed connection reconnects on its next query, on PostgreSQL as on MySQL."""
    conn = connection_by_backend
    conn.close()
    assert conn.query("SELECT 1", reconnect=True).fetchone()[0] == 1
    assert conn.is_connected


def test_closed_connection_without_reconnect_by_backend(connection_by_backend):
    conn = connection_by_backend
    conn.close()
    with pytest.raises(dj.errors.LostConnectionError):
        conn.query("SELECT 1", reconnect=False)
    conn.connect()  # the fixture closes the connection at teardown


def test_reconnect_after_close_in_transaction_by_backend(connection_by_backend):
    """Reconnecting does not silently continue a transaction that the close ended."""
    conn = connection_by_backend
    with conn.transaction, pytest.raises(dj.errors.LostConnectionError, match="during a transaction"):
        conn.close()
        conn.query("SELECT 1", reconnect=True)
    assert not conn.in_transaction
    assert conn.query("SELECT 1").fetchone()[0] == 1
