import pytest
import datajoint as dj
from pymysql.err import OperationalError


def test_secure_connection(db_creds_test, connection_test):
    result = (
        dj.conn(reset=True, **db_creds_test)
        .query("SHOW STATUS LIKE 'Ssl_cipher';")
        .fetchone()[1]
    )
    assert len(result) > 0


def test_insecure_connection(db_creds_test, connection_test):
    result = (
        dj.conn(use_tls=False, reset=True, **db_creds_test)
        .query("SHOW STATUS LIKE 'Ssl_cipher';")
        .fetchone()[1]
    )
    assert result == ""


def test_reject_insecure(db_creds_test, connection_test):
    with pytest.raises(OperationalError):
        dj.conn(
            db_creds_test["host"],
            user="djssl",
            password="djssl",
            use_tls=False,
            reset=True,
        ).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
