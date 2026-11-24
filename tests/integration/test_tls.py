import os

import pytest
from pymysql.err import OperationalError

import datajoint as dj

# SSL tests require docker-compose with datajoint/mysql image (has SSL configured)
# Testcontainers with official mysql image doesn't have SSL certificates
requires_ssl = pytest.mark.skipif(
    os.environ.get("DJ_USE_EXTERNAL_CONTAINERS", "").lower() not in ("1", "true", "yes"),
    reason="SSL tests require external containers (docker-compose) with SSL configured",
)


@requires_ssl
def test_explicit_ssl_connection(db_creds_test, connection_test):
    """When use_tls=True is specified, SSL must be active."""
    result = dj.conn(use_tls=True, reset=True, **db_creds_test).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
    assert len(result) > 0, "SSL should be active when use_tls=True"


@requires_ssl
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


@requires_ssl
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


def test_insecure_connection_full_sha2_login(connection_root, db_creds_test):
    """use_tls=False logs in with caching_sha2_password when the server has no cached login.

    A new account has no cached login, so the server asks for the full
    exchange. Without TLS, PyMySQL encrypts the password with the server's RSA
    key, which needs the cryptography package and PyMySQL 1.2.4 or later
    (1.2.1 to 1.2.3 fail this login). See
    https://github.com/datajoint/datajoint-python/issues/1750.
    """
    user = "djsha2"
    connection_root.query(f"DROP USER IF EXISTS '{user}'@'%%'")
    connection_root.query(f"CREATE USER '{user}'@'%%' IDENTIFIED WITH caching_sha2_password BY '{user}'")
    try:
        conn = dj.Connection(db_creds_test["host"], user, user, use_tls=False, backend="mysql")
        try:
            # No TLS, so the login took the RSA path this test is about
            assert conn.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'").fetchone()[1] == ""
            assert conn.query("SELECT 1").fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        connection_root.query(f"DROP USER IF EXISTS '{user}'@'%%'")
