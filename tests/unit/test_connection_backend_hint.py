"""
Unit tests for the hint added when a connection with the default backend fails.

PostgreSQL is the default backend. A connection that fails while the backend
was left at that default says how to select MySQL; one whose backend was set,
by argument, config assignment, ``datajoint.json`` or ``DJ_BACKEND``, raises
the original error unchanged. No database server is needed.
"""

import json
import socket
from unittest.mock import patch

import pytest

from datajoint.connection import DEFAULT_BACKEND_HINT, Connection
from datajoint.errors import DataJointError
from datajoint.settings import Config


class _RefusingAdapter:
    """Adapter stub whose connect fails the way a refused connection does."""

    backend = "postgresql"

    def connect(self, **kwargs):
        raise OSError("connection refused")


@pytest.fixture
def fresh_config(monkeypatch):
    """A Config built without DJ_BACKEND or DJ_PORT from the developer's shell."""
    for var in ("DJ_BACKEND", "DJ_PORT"):
        monkeypatch.delenv(var, raising=False)
    return Config()


def _connect(config, **kwargs):
    with patch("datajoint.connection.get_adapter", return_value=_RefusingAdapter()):
        return Connection("localhost", "user", "password", config_override=config, **kwargs)


def test_default_backend_failure_adds_hint(fresh_config):
    """A failed connection with the default backend names the MySQL setting."""
    with pytest.raises(DataJointError) as info:
        _connect(fresh_config)
    message = str(info.value)
    assert "connection refused" in message
    assert DEFAULT_BACKEND_HINT in message
    assert "database.backend='mysql'" in message and "DJ_BACKEND=mysql" in message
    assert isinstance(info.value.__cause__, OSError)


def test_missing_driver_with_default_backend_adds_hint(fresh_config):
    """A missing PostgreSQL driver with the default backend also names the MySQL setting."""
    missing = ImportError("psycopg2 is required for PostgreSQL support.")
    with patch("datajoint.connection.get_adapter", side_effect=missing):
        with pytest.raises(DataJointError, match="psycopg2 is required") as info:
            Connection("localhost", "user", "password", config_override=fresh_config)
    assert DEFAULT_BACKEND_HINT in str(info.value)
    assert info.value.__cause__ is missing


def test_backend_argument_gets_no_hint(fresh_config):
    """backend= passed to Connection counts as set: the original error is raised."""
    with pytest.raises(OSError, match="connection refused"):
        _connect(fresh_config, backend="postgresql")


def test_backend_assigned_in_config_gets_no_hint(fresh_config):
    """A backend assigned in config, even to the default value, counts as set."""
    fresh_config["database.backend"] = "postgresql"
    with pytest.raises(OSError, match="connection refused"):
        _connect(fresh_config)


def test_backend_from_env_var_gets_no_hint(monkeypatch):
    """DJ_BACKEND counts as set."""
    monkeypatch.setenv("DJ_BACKEND", "postgresql")
    with pytest.raises(OSError, match="connection refused"):
        _connect(Config())


def test_backend_from_config_file_gets_no_hint(fresh_config, tmp_path):
    """database.backend in datajoint.json counts as set."""
    config_file = tmp_path / "datajoint.json"
    config_file.write_text(json.dumps({"database": {"backend": "postgresql"}}))
    fresh_config.load(config_file)
    with pytest.raises(OSError, match="connection refused"):
        _connect(fresh_config)


def test_refused_connection_with_default_backend(fresh_config):
    """A real refused connection through the PostgreSQL adapter carries the hint."""
    pytest.importorskip("psycopg2")
    # Find a local port with nothing listening on it.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with pytest.raises(DataJointError) as info:
        Connection("127.0.0.1", "user", "password", port, use_tls=False, config_override=fresh_config)
    assert DEFAULT_BACKEND_HINT in str(info.value)
