"""Tests for thread-safe mode in connection management."""

import pytest

import datajoint as dj
from datajoint.connection import ConnectionConfig
from datajoint.errors import ThreadSafetyError


@pytest.fixture(autouse=True)
def reset_thread_safe_mode():
    """Reset thread_safe to False before and after each test."""
    # Use object.__setattr__ to bypass the one-way lock for test reset
    object.__setattr__(dj.config, "thread_safe", False)
    yield
    object.__setattr__(dj.config, "thread_safe", False)


class TestThreadSafeModeSetting:
    """Tests for thread_safe as a regular setting."""

    def test_thread_safe_default_false(self):
        """Thread-safe mode is disabled by default."""
        assert dj.config.thread_safe is False

    def test_thread_safe_can_be_enabled(self):
        """Thread-safe mode can be enabled."""
        dj.config.thread_safe = True
        assert dj.config.thread_safe is True

    def test_thread_safe_cannot_be_disabled(self):
        """Once enabled, thread-safe mode cannot be disabled."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Cannot disable"):
            dj.config.thread_safe = False

    def test_thread_safe_via_dict_access(self):
        """Thread-safe can be set via dict-style access."""
        dj.config["thread_safe"] = True
        assert dj.config.thread_safe is True

    def test_thread_safe_cannot_be_disabled_via_dict(self):
        """Cannot disable via dict access either."""
        dj.config["thread_safe"] = True
        with pytest.raises(ThreadSafetyError, match="Cannot disable"):
            dj.config["thread_safe"] = False

    def test_thread_safe_from_env_var(self, monkeypatch):
        """Thread-safe mode can be set via environment variable."""
        from datajoint.settings import Config

        monkeypatch.setenv("DJ_THREAD_SAFE", "true")
        cfg = Config()
        assert cfg.thread_safe is True


class TestConfigBlockedInThreadSafeMode:
    """Tests for config access being blocked in thread-safe mode."""

    def test_attribute_access_blocked(self):
        """Attribute access raises ThreadSafetyError in thread-safe mode."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            _ = dj.config.database

    def test_dict_access_blocked(self):
        """Dict-style access raises ThreadSafetyError in thread-safe mode."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            _ = dj.config["database.host"]

    def test_dict_set_blocked(self):
        """Dict-style setting raises ThreadSafetyError in thread-safe mode."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            dj.config["database.host"] = "newhost"

    def test_attribute_set_blocked(self):
        """Attribute setting raises ThreadSafetyError in thread-safe mode."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            dj.config.safemode = False

    def test_thread_safe_always_readable(self):
        """The thread_safe setting itself is always readable."""
        dj.config.thread_safe = True
        # Should not raise
        assert dj.config.thread_safe is True
        assert dj.config["thread_safe"] is True


class TestConnBlockedInThreadSafeMode:
    """Tests for dj.conn() being blocked in thread-safe mode."""

    def test_conn_blocked(self):
        """dj.conn() raises ThreadSafetyError in thread-safe mode."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="dj.conn\\(\\) is disabled"):
            dj.conn()


class TestConnectionFromConfig:
    """Tests for Connection.from_config() method."""

    def test_from_config_exists(self):
        """Connection.from_config class method exists."""
        assert hasattr(dj.Connection, "from_config")
        assert callable(dj.Connection.from_config)

    def test_from_config_requires_user(self):
        """from_config raises error if user not provided."""
        with pytest.raises(dj.DataJointError, match="user is required"):
            dj.Connection.from_config({"host": "localhost", "password": "test"})

    def test_from_config_requires_password(self):
        """from_config raises error if password not provided."""
        with pytest.raises(dj.DataJointError, match="password is required"):
            dj.Connection.from_config({"host": "localhost", "user": "test"})

    def test_from_config_with_explicit_params(self):
        """from_config accepts explicit keyword parameters."""
        from unittest.mock import patch

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["host"] = host
            captured_args["user"] = user
            captured_args["port"] = port

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(
                host="testhost",
                user="testuser",
                password="testpass",
                port=3307,
            )

        assert captured_args["host"] == "testhost"
        assert captured_args["user"] == "testuser"
        assert captured_args["port"] == 3307

    def test_from_config_with_dict(self):
        """from_config accepts configuration dict."""
        from unittest.mock import patch

        cfg = {
            "host": "dicthost",
            "user": "dictuser",
            "password": "dictpass",
            "port": 3308,
        }

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["host"] = host
            captured_args["port"] = port

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(cfg)

        assert captured_args["host"] == "dicthost"
        assert captured_args["port"] == 3308

    def test_from_config_kwargs_override_dict(self):
        """Keyword arguments override dict values."""
        from unittest.mock import patch

        cfg = {"host": "dicthost", "user": "dictuser", "password": "dictpass"}
        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["host"] = host
            captured_args["user"] = user

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(cfg, host="overridehost")

        assert captured_args["host"] == "overridehost"
        assert captured_args["user"] == "dictuser"

    def test_from_config_works_in_thread_safe_mode(self):
        """from_config works in thread-safe mode (no global config access)."""
        from unittest.mock import patch

        dj.config.thread_safe = True

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["host"] = host

        with patch.object(dj.Connection, "__init__", mock_init):
            # Should NOT raise ThreadSafetyError
            dj.Connection.from_config(
                host="testhost",
                user="testuser",
                password="testpass",
            )

        assert captured_args["host"] == "testhost"

    def test_from_config_default_port_mysql(self):
        """from_config uses default port 3306 for MySQL."""
        from unittest.mock import patch

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["port"] = port
            captured_args["backend"] = backend

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(host="h", user="u", password="p")

        assert captured_args["port"] == 3306
        assert captured_args["backend"] == "mysql"

    def test_from_config_default_port_postgresql(self):
        """from_config uses default port 5432 for PostgreSQL."""
        from unittest.mock import patch

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_args["port"] = port

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(host="h", user="u", password="p", backend="postgresql")

        assert captured_args["port"] == 5432


class TestThreadSafetyErrorExport:
    """Tests for ThreadSafetyError availability."""

    def test_error_exported(self):
        """ThreadSafetyError is exported from datajoint module."""
        assert hasattr(dj, "ThreadSafetyError")
        assert dj.ThreadSafetyError is ThreadSafetyError

    def test_error_is_subclass(self):
        """ThreadSafetyError is a subclass of DataJointError."""
        assert issubclass(ThreadSafetyError, dj.DataJointError)


class TestConnectionConfig:
    """Tests for ConnectionConfig class."""

    def test_defaults(self):
        """ConnectionConfig has correct defaults."""
        cfg = ConnectionConfig()
        assert cfg.safemode is True
        assert cfg.database_prefix == ""
        assert cfg.stores == {}
        assert cfg.cache is None
        assert cfg.reconnect is True
        assert cfg.display_limit == 12
        assert cfg.display_width == 14

    def test_explicit_values(self):
        """Explicit values override defaults."""
        cfg = ConnectionConfig(safemode=False, display_limit=25, stores={"raw": {}})
        assert cfg.safemode is False
        assert cfg.display_limit == 25
        assert cfg.stores == {"raw": {}}

    def test_read_write(self):
        """ConnectionConfig supports read/write access."""
        cfg = ConnectionConfig()
        cfg.safemode = False
        cfg.display_limit = 50
        assert cfg.safemode is False
        assert cfg.display_limit == 50

    def test_forwarding_to_global_when_not_thread_safe(self):
        """Unset values forward to global config when thread_safe=False."""
        # Set a value in global config
        original_safemode = dj.config.safemode
        dj.config.safemode = False

        try:
            cfg = ConnectionConfig(_thread_safe=False)
            # Should forward to global config
            assert cfg.safemode is False
        finally:
            dj.config.safemode = original_safemode

    def test_uses_defaults_when_thread_safe(self):
        """Unset values use defaults when thread_safe=True."""
        cfg = ConnectionConfig(_thread_safe=True)
        # Should use default, not global config
        assert cfg.safemode is True  # default
        assert cfg.display_limit == 12  # default

    def test_explicit_overrides_global(self):
        """Explicit values override global config even when not thread_safe."""
        original_safemode = dj.config.safemode
        dj.config.safemode = True

        try:
            cfg = ConnectionConfig(_thread_safe=False, safemode=False)
            assert cfg.safemode is False  # explicit value
        finally:
            dj.config.safemode = original_safemode

    def test_get_store_spec(self):
        """get_store_spec returns store configuration."""
        cfg = ConnectionConfig(stores={"raw": {"protocol": "file", "location": "/data"}})
        spec = cfg.get_store_spec("raw")
        assert spec["protocol"] == "file"
        assert spec["location"] == "/data"

    def test_get_store_spec_not_found(self):
        """get_store_spec raises error for unknown store."""
        cfg = ConnectionConfig(stores={})
        with pytest.raises(dj.DataJointError, match="not configured"):
            cfg.get_store_spec("unknown")

    def test_repr(self):
        """ConnectionConfig has informative repr."""
        cfg = ConnectionConfig(safemode=False)
        r = repr(cfg)
        assert "ConnectionConfig" in r
        assert "safemode=False" in r


class TestConnectionConfigAttribute:
    """Tests for Connection.config attribute."""

    def test_from_config_creates_connection_config(self):
        """from_config creates ConnectionConfig on connection."""
        from unittest.mock import patch

        captured_config = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_config["config"] = _config

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(host="h", user="u", password="p", safemode=False)

        assert captured_config["config"] is not None
        assert isinstance(captured_config["config"], ConnectionConfig)
        assert captured_config["config"].safemode is False

    def test_from_config_passes_all_settings(self):
        """from_config passes all connection-scoped settings."""
        from unittest.mock import patch

        captured_config = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_config["config"] = _config

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(
                host="h",
                user="u",
                password="p",
                safemode=False,
                database_prefix="test_",
                display_limit=100,
                stores={"main": {"protocol": "file"}},
            )

        cfg = captured_config["config"]
        assert cfg.safemode is False
        assert cfg.database_prefix == "test_"
        assert cfg.display_limit == 100
        assert cfg.stores == {"main": {"protocol": "file"}}

    def test_from_config_extracts_settings_from_dict(self):
        """from_config extracts connection-scoped settings from cfg dict."""
        from unittest.mock import patch

        captured_config = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_config["config"] = _config

        cfg_dict = {
            "host": "h",
            "user": "u",
            "password": "p",
            "safemode": False,
            "display_limit": 50,
        }

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(cfg_dict)

        cfg = captured_config["config"]
        assert cfg.safemode is False
        assert cfg.display_limit == 50


class TestSchemaThreadSafe:
    """Tests for Schema behavior in thread-safe mode."""

    def test_schema_without_connection_raises_in_thread_safe_mode(self):
        """Schema without explicit connection raises ThreadSafetyError."""
        dj.config.thread_safe = True
        with pytest.raises(ThreadSafetyError, match="Schema requires explicit connection"):
            dj.Schema("test_schema")

    def test_schema_with_connection_works_in_thread_safe_mode(self):
        """Schema with explicit connection works in thread-safe mode."""
        from unittest.mock import MagicMock, patch

        dj.config.thread_safe = True

        # Create a mock connection
        mock_conn = MagicMock(spec=dj.Connection)
        mock_conn.config = ConnectionConfig(_thread_safe=True)

        # Mock the schema activation to avoid database operations
        with patch.object(dj.Schema, "activate"):
            schema = dj.Schema("test_schema", connection=mock_conn)
            assert schema.connection is mock_conn
