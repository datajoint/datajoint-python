"""
use_tls options against both backends (#1714).

- A ``use_tls`` dict with a key DataJoint does not know raises before connecting.
- With ``use_tls=None``, PostgreSQL falls back to a connection without TLS when the
  server does not support SSL, whatever the language of the error message.
"""

import pytest

import datajoint as dj
from datajoint.adapters import postgres
from datajoint.errors import DataJointError


def _connect(creds, use_tls):
    return dj.Connection(creds["host"], creds["user"], creds["password"], use_tls=use_tls, backend=creds["backend"])


def test_unknown_use_tls_key_raises(db_creds_by_backend):
    with pytest.raises(DataJointError, match="Unknown use_tls key.*'ssl_ca'.*Accepted keys"):
        _connect(db_creds_by_backend, {"ssl_ca": "/certs/ca.pem"})


def test_default_tls_connects_without_reading_the_message(db_creds_by_backend, monkeypatch):
    """With the English message not recognized, PostgreSQL asks the server whether it supports SSL."""
    monkeypatch.setattr(postgres, "_tls_rejected", lambda error: False)
    connection = _connect(db_creds_by_backend, None)
    try:
        assert connection.query("SELECT 1").fetchone()[0] == 1
    finally:
        connection.close()
