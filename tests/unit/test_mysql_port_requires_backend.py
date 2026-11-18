"""
Unit tests for refusing MySQL's default port when the backend is not set.

PostgreSQL is the default backend, and DataJoint never infers the backend from
the port. A connection on port 3306 whose backend was left at the default raises
a DataJointError before any network access, whichever way the port was given:
``DJ_PORT``, ``datajoint.json``, ``dj.config`` assignment, ``dj.Instance`` and
``dj.Connection`` arguments, or ``host:port``. A backend set explicitly, to
``"mysql"`` or to ``"postgresql"``, allows port 3306. No database server is needed.
"""

import importlib
import json
from unittest.mock import patch

import pytest


def _module(name):
    """
    The module as currently imported.

    Another unit test removes the datajoint modules from ``sys.modules`` and imports
    them again, so classes and patch targets are looked up at test time.
    """
    return importlib.import_module(name)


def Config():
    return _module("datajoint.settings").Config()


def Connection(*args, **kwargs):
    return _module("datajoint.connection").Connection(*args, **kwargs)


def Instance(**kwargs):
    return _module("datajoint.instance").Instance(**kwargs)


class _Reached(Exception):
    """Raised by the adapter stub: the connection got past the port check."""

    def __init__(self, backend, kwargs):
        super().__init__(f"reached {backend} adapter")
        self.backend = backend
        self.kwargs = kwargs


class _RecordingAdapter:
    """Adapter stub that records the connect arguments and stops there."""

    def __init__(self, backend):
        self.backend = backend

    def connect(self, **kwargs):
        raise _Reached(self.backend, kwargs)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """Keep the shell environment and any datajoint.json from setting a backend or port."""
    monkeypatch.setenv("DJ_THREAD_SAFE", "false")
    monkeypatch.setenv("DJ_IGNORE_CONFIG_FILE", "true")
    for var in ("DJ_BACKEND", "DJ_PORT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)
    with patch.object(_module("datajoint.connection"), "get_adapter", side_effect=_RecordingAdapter) as get_adapter:
        yield get_adapter


def _reached(connect):
    """Run ``connect``, which must get past the port check; return (backend, port) it used."""
    with pytest.raises(Exception) as info:
        connect()
    error = info.value
    if isinstance(error, _module("datajoint.errors").DataJointError):
        # The default backend adds a hint to a failed connection and chains the original error.
        assert _module("datajoint.connection").DEFAULT_BACKEND_HINT in str(error)
        error = error.__cause__
    assert isinstance(error, _Reached), f"expected the adapter to be reached, got {info.value!r}"
    return error.backend, error.kwargs["port"]


def _refused(connect, get_adapter):
    """Run ``connect``, which must stop at the port check; return the error message."""
    with pytest.raises(_module("datajoint.errors").DataJointError) as info:
        connect()
    assert not isinstance(info.value.__cause__, _Reached)
    get_adapter.assert_not_called()
    return str(info.value)


def _connection(config, *args, **kwargs):
    kwargs.setdefault("use_tls", False)
    return lambda: Connection("localhost", "user", "password", *args, config_override=config, **kwargs)


def _instance(**kwargs):
    return lambda: Instance(host="localhost", user="user", password="password", use_tls=False, **kwargs)


# --- The error message --------------------------------------------------------


def test_message_says_why_and_how_to_fix(_isolate):
    """The message names port 3306 as MySQL's, the PostgreSQL default, each fix, and port 5432."""
    message = _refused(_connection(Config(), 3306), _isolate)
    assert "localhost:3306" in message
    assert "MySQL's default port" in message
    assert "backend not set" in message
    assert "defaults to PostgreSQL" in message and "2.4" in message
    assert "DJ_BACKEND=mysql" in message
    assert '{"database": {"backend": "mysql"}}' in message
    assert "dj.config['database.backend'] = 'mysql'" in message
    assert "backend='mysql'" in message
    assert "5432" in message


# --- Each way of setting port 3306 without a backend raises ---------------------


def test_dj_port_env_raises(monkeypatch, _isolate):
    monkeypatch.setenv("DJ_PORT", "3306")
    config = Config()
    assert config.database.port == 3306
    _refused(_connection(config), _isolate)


def test_dj_port_env_raises_for_instance(monkeypatch, _isolate):
    monkeypatch.setenv("DJ_PORT", "3306")
    _refused(_instance(), _isolate)


def test_config_file_port_raises(tmp_path, _isolate):
    config_file = tmp_path / "datajoint.json"
    config_file.write_text(json.dumps({"database": {"port": 3306}}))
    config = Config()
    config.load(config_file)
    _refused(_connection(config), _isolate)


def test_config_assignment_raises(_isolate):
    config = Config()
    config["database.port"] = 3306
    _refused(_connection(config), _isolate)


def test_instance_port_argument_raises(_isolate):
    _refused(_instance(port=3306), _isolate)


def test_instance_port_kwarg_override_raises(_isolate):
    _refused(_instance(database__port=3306), _isolate)


def test_connection_port_argument_raises(_isolate):
    _refused(_connection(Config(), 3306), _isolate)


def test_port_in_host_raises(_isolate):
    config = Config()
    _refused(lambda: Connection("localhost:3306", "user", "password", use_tls=False, config_override=config), _isolate)


def test_dj_conn_raises(monkeypatch, _isolate):
    """dj.conn takes no port, but a port in the host or in config counts."""
    dj = _module("datajoint")
    monkeypatch.setattr(dj, "_global_config", Config())
    monkeypatch.setattr(_module("datajoint.instance"), "_singleton_connection", None)
    _refused(lambda: dj.conn("localhost:3306", "user", "password", use_tls=False), _isolate)
    dj._global_config["database.port"] = 3306
    _refused(lambda: dj.conn("localhost", "user", "password", reset=True, use_tls=False), _isolate)


def test_port_set_before_backend_is_not_refused_by_config(tmp_path):
    """The settings accept port 3306 first and the backend second: the check is on connecting."""
    config = Config()
    config["database.port"] = 3306
    config["database.backend"] = "mysql"
    assert (config.database.backend, config.database.port) == ("mysql", 3306)
    config_file = tmp_path / "datajoint.json"
    config_file.write_text(json.dumps({"database": {"port": 3306, "backend": "mysql"}}))
    loaded = Config()
    loaded.load(config_file)
    assert (loaded.database.backend, loaded.database.port) == ("mysql", 3306)


# --- An explicit mysql backend resolves and allows 3306 --------------------------


def test_mysql_env_resolves_3306(monkeypatch):
    monkeypatch.setenv("DJ_BACKEND", "mysql")
    assert _reached(_connection(Config())) == ("mysql", 3306)


def test_mysql_env_with_dj_port_3306(monkeypatch):
    monkeypatch.setenv("DJ_BACKEND", "mysql")
    monkeypatch.setenv("DJ_PORT", "3306")
    assert _reached(_connection(Config())) == ("mysql", 3306)


def test_mysql_config_file_resolves_3306(tmp_path):
    config_file = tmp_path / "datajoint.json"
    config_file.write_text(json.dumps({"database": {"backend": "mysql", "port": 3306}}))
    config = Config()
    config.load(config_file)
    assert _reached(_connection(config)) == ("mysql", 3306)


def test_mysql_config_assignment_resolves_3306():
    config = Config()
    config["database.backend"] = "mysql"
    assert _reached(_connection(config)) == ("mysql", 3306)


def test_mysql_instance_resolves_3306():
    assert _reached(_instance(backend="mysql")) == ("mysql", 3306)
    assert _reached(_instance(backend="mysql", port=3306)) == ("mysql", 3306)


def test_mysql_connection_argument_with_3306():
    assert _reached(_connection(Config(), 3306, backend="mysql")) == ("mysql", 3306)


# --- An explicit postgresql backend allows 3306 -------------------------------------


def test_postgresql_env_allows_3306(monkeypatch):
    monkeypatch.setenv("DJ_BACKEND", "postgresql")
    monkeypatch.setenv("DJ_PORT", "3306")
    assert _reached(_connection(Config())) == ("postgresql", 3306)


def test_postgresql_config_file_allows_3306(tmp_path):
    config_file = tmp_path / "datajoint.json"
    config_file.write_text(json.dumps({"database": {"backend": "postgresql", "port": 3306}}))
    config = Config()
    config.load(config_file)
    assert _reached(_connection(config)) == ("postgresql", 3306)


def test_postgresql_config_assignment_allows_3306():
    config = Config()
    config["database.backend"] = "postgresql"
    config["database.port"] = 3306
    assert _reached(_connection(config)) == ("postgresql", 3306)


def test_postgresql_instance_allows_3306():
    assert _reached(_instance(backend="postgresql", port=3306)) == ("postgresql", 3306)


def test_postgresql_connection_argument_allows_3306():
    assert _reached(_connection(Config(), 3306, backend="postgresql")) == ("postgresql", 3306)
    config = Config()
    connect = lambda: Connection("localhost:3306", "u", "p", use_tls=False, backend="postgresql", config_override=config)  # noqa: E731
    assert _reached(connect) == ("postgresql", 3306)


# --- The default backend without port 3306 is unaffected ----------------------------


def test_default_without_port_resolves_5432():
    assert _reached(_connection(Config())) == ("postgresql", 5432)
    assert _reached(_instance()) == ("postgresql", 5432)


def test_default_with_port_5432(monkeypatch):
    assert _reached(_connection(Config(), 5432)) == ("postgresql", 5432)
    assert _reached(_instance(port=5432)) == ("postgresql", 5432)
    monkeypatch.setenv("DJ_PORT", "5432")
    assert _reached(_connection(Config())) == ("postgresql", 5432)


def test_default_with_other_port():
    """Only MySQL's default port is refused; any other port with the default backend connects."""
    assert _reached(_connection(Config(), 3307)) == ("postgresql", 3307)
