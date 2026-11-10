"""TLS connection tests.

The official MySQL images generate a CA and a server certificate when the
server first starts, so every test server, from testcontainers or from
docker-compose, accepts TLS connections.
"""

import pytest
from pymysql.err import OperationalError

import datajoint as dj


def test_explicit_ssl_connection(db_creds_test, connection_test):
    """When use_tls=True is specified, SSL must be active."""
    result = dj.conn(use_tls=True, reset=True, **db_creds_test).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
    assert len(result) > 0, "SSL should be active when use_tls=True"


def test_ssl_auto_detect(db_creds_test, connection_test):
    """When use_tls is not specified, SSL is used if the server supports it."""
    conn = dj.conn(reset=True, **db_creds_test)
    result = conn.query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
    assert len(result) > 0, "SSL should be active when the server supports it"


def test_insecure_connection(db_creds_test, connection_test):
    """When use_tls=False, DataJoint must not configure client-side TLS.

    Note: this pins what DataJoint controls, not the negotiated cipher. A
    MySQL 8 server with TLS configured may still negotiate TLS during
    connection setup (e.g. to protect the auth handshake) even when the client
    requests no SSL, so ``Ssl_cipher`` is not guaranteed to be empty and must
    not be asserted on — doing so made this test depend on the resolved client
    stack rather than on DataJoint behavior.
    """
    conn = dj.conn(use_tls=False, reset=True, **db_creds_test)
    # DataJoint sent no client-side SSL configuration.
    assert conn.conn_info.get("ssl_input") is False
    assert "ssl" not in conn.conn_info
    # The connection is usable without client TLS.
    assert conn.query("SELECT 1").fetchone()[0] == 1


def test_reject_insecure(db_creds_test, connection_test):
    """Users with REQUIRE SSL cannot connect without SSL."""
    with pytest.raises(OperationalError):
        dj.conn(
            db_creds_test["host"],
            user="djssl",
            password="djssl",
            use_tls=False,
            reset=True,
        ).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
