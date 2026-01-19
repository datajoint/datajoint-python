import logging

import pytest
from pymysql.err import OperationalError

import datajoint as dj


def test_explicit_ssl_connection(db_creds_test, connection_test):
    """When use_tls=True is specified, SSL must be active."""
    result = dj.conn(use_tls=True, reset=True, **db_creds_test).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
    assert len(result) > 0, "SSL should be active when use_tls=True"


def test_ssl_auto_detect(db_creds_test, connection_test, caplog):
    """When use_tls is not specified, SSL is preferred but fallback is allowed with warning."""
    with caplog.at_level(logging.WARNING):
        conn = dj.conn(reset=True, **db_creds_test)
        result = conn.query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]

    if len(result) > 0:
        # SSL connected successfully
        assert "SSL connection failed" not in caplog.text
    else:
        # SSL failed and fell back - warning should be logged
        assert "SSL connection failed" in caplog.text
        assert "Falling back to non-SSL" in caplog.text


def test_insecure_connection(db_creds_test, connection_test):
    """When use_tls=False, SSL should not be used."""
    result = dj.conn(use_tls=False, reset=True, **db_creds_test).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
    assert result == ""


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
