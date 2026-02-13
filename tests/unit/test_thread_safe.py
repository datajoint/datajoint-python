"""Tests for thread-safe mode in connection management."""

import pytest

import datajoint as dj
from datajoint.connection import ConnectionConfig
from datajoint.errors import ThreadSafetyError


@pytest.fixture(autouse=True)
def reset_thread_safe_mode():
    """Reset thread_safe to False before and after each test."""
    # Use object.__setattr__ to bypass read-only restriction for test reset
    object.__setattr__(dj.config, "thread_safe", False)
    yield
    object.__setattr__(dj.config, "thread_safe", False)


def enable_thread_safe():
    """Helper to enable thread-safe mode in tests (bypasses read-only)."""
    object.__setattr__(dj.config, "thread_safe", True)


class TestThreadSafeModeSetting:
    """Tests for thread_safe as a read-only setting."""

    def test_thread_safe_default_false(self):
        """Thread-safe mode is disabled by default."""
        assert dj.config.thread_safe is False

    def test_thread_safe_cannot_be_set_programmatically(self):
        """Thread-safe mode cannot be set via attribute assignment."""
        with pytest.raises(ThreadSafetyError, match="cannot be set programmatically"):
            dj.config.thread_safe = True

    def test_thread_safe_cannot_be_set_via_dict_access(self):
        """Thread-safe mode cannot be set via dict-style access."""
        with pytest.raises(ThreadSafetyError, match="cannot be set programmatically"):
            dj.config["thread_safe"] = True

    def test_thread_safe_from_env_var(self, monkeypatch):
        """Thread-safe mode can be set via environment variable."""
        from datajoint.settings import Config

        monkeypatch.setenv("DJ_THREAD_SAFE", "true")
        cfg = Config()
        assert cfg.thread_safe is True

    def test_thread_safe_from_config_file(self, tmp_path):
        """Thread-safe mode can be set via config file."""
        import json

        from datajoint.settings import Config

        config_file = tmp_path / "datajoint.json"
        config_file.write_text(json.dumps({"thread_safe": True}))
        cfg = Config()
        cfg.load(config_file)
        assert cfg.thread_safe is True


class TestConfigBlockedInThreadSafeMode:
    """Tests for config access being blocked in thread-safe mode."""

    def test_attribute_access_blocked(self):
        """Attribute access raises ThreadSafetyError in thread-safe mode."""
        enable_thread_safe()
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            _ = dj.config.database

    def test_dict_access_blocked(self):
        """Dict-style access raises ThreadSafetyError in thread-safe mode."""
        enable_thread_safe()
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            _ = dj.config["database.host"]

    def test_dict_set_blocked(self):
        """Dict-style setting raises ThreadSafetyError in thread-safe mode."""
        enable_thread_safe()
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            dj.config["database.host"] = "newhost"

    def test_attribute_set_blocked(self):
        """Attribute setting raises ThreadSafetyError in thread-safe mode."""
        enable_thread_safe()
        with pytest.raises(ThreadSafetyError, match="Global config is inaccessible"):
            dj.config.safemode = False

    def test_thread_safe_always_readable(self):
        """The thread_safe setting itself is always readable."""
        enable_thread_safe()
        # Should not raise
        assert dj.config.thread_safe is True
        assert dj.config["thread_safe"] is True


class TestConnBlockedInThreadSafeMode:
    """Tests for dj.conn() being blocked in thread-safe mode."""

    def test_conn_blocked(self):
        """dj.conn() raises ThreadSafetyError in thread-safe mode."""
        enable_thread_safe()
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

        enable_thread_safe()

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

    def test_forwarding_to_global_with_legacy_api(self):
        """Unset values forward to global config with legacy API (dj.conn())."""
        # Set a value in global config
        original_safemode = dj.config.safemode
        object.__setattr__(dj.config, "safemode", False)

        try:
            # Legacy API uses _use_global_fallback=True
            cfg = ConnectionConfig(_use_global_fallback=True)
            # Should forward to global config
            assert cfg.safemode is False
        finally:
            object.__setattr__(dj.config, "safemode", original_safemode)

    def test_uses_defaults_with_new_api(self):
        """Unset values use defaults with new API (from_config())."""
        # New API uses _use_global_fallback=False
        cfg = ConnectionConfig(_use_global_fallback=False)
        # Should use default, not global config
        assert cfg.safemode is True  # default
        assert cfg.display_limit == 12  # default

    def test_new_api_works_identically_regardless_of_thread_safe(self):
        """New API (from_config) uses defaults, not global config, in both modes."""
        # Set different values in global config
        original_safemode = dj.config.safemode
        object.__setattr__(dj.config, "safemode", False)  # Different from default (True)

        try:
            # New API with thread_safe=False
            cfg1 = ConnectionConfig(_use_global_fallback=False)

            # Enable thread_safe mode
            enable_thread_safe()

            # New API with thread_safe=True
            cfg2 = ConnectionConfig(_use_global_fallback=False)

            # Both should use defaults, not global config
            assert cfg1.safemode is True  # default, not global (False)
            assert cfg2.safemode is True  # default, not global (False)
            assert cfg1.safemode == cfg2.safemode
        finally:
            object.__setattr__(dj.config, "safemode", original_safemode)

    def test_explicit_overrides_global_with_legacy_api(self):
        """Explicit values override global config even with legacy API."""
        original_safemode = dj.config.safemode
        object.__setattr__(dj.config, "safemode", True)

        try:
            cfg = ConnectionConfig(_use_global_fallback=True, safemode=False)
            assert cfg.safemode is False  # explicit value
        finally:
            object.__setattr__(dj.config, "safemode", original_safemode)

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

    def test_override_context_manager(self):
        """override temporarily changes values and restores them."""
        cfg = ConnectionConfig(safemode=True, display_limit=10)

        with cfg.override(safemode=False, display_limit=50):
            assert cfg.safemode is False
            assert cfg.display_limit == 50

        assert cfg.safemode is True
        assert cfg.display_limit == 10

    def test_override_restores_on_exception(self):
        """override restores values even when exception is raised."""
        cfg = ConnectionConfig(safemode=True)

        try:
            with cfg.override(safemode=False):
                assert cfg.safemode is False
                raise RuntimeError("test error")
        except RuntimeError:
            pass

        assert cfg.safemode is True

    def test_override_with_defaults(self):
        """override works when value was not explicitly set."""
        cfg = ConnectionConfig()  # Uses defaults
        assert cfg.safemode is True  # default

        with cfg.override(safemode=False):
            assert cfg.safemode is False

        # Should restore to default (not be in _values)
        assert cfg.safemode is True
        assert "safemode" not in cfg._values


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

    def test_from_config_does_not_use_global_fallback(self):
        """from_config creates config that doesn't fall back to global config."""
        from unittest.mock import patch

        # Set a non-default value in global config
        original_safemode = dj.config.safemode
        object.__setattr__(dj.config, "safemode", False)  # Different from default (True)

        captured_config = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None, *, _config=None):
            captured_config["config"] = _config

        try:
            with patch.object(dj.Connection, "__init__", mock_init):
                # Don't pass safemode - should use default, not global
                dj.Connection.from_config(host="h", user="u", password="p")

            cfg = captured_config["config"]
            # Should use default (True), not global (False)
            assert cfg.safemode is True
        finally:
            object.__setattr__(dj.config, "safemode", original_safemode)


class TestSchemaThreadSafe:
    """Tests for Schema behavior in thread-safe mode."""

    def test_schema_without_connection_raises_in_thread_safe_mode(self):
        """Schema without explicit connection raises ThreadSafetyError."""
        enable_thread_safe()
        with pytest.raises(ThreadSafetyError, match="Schema requires explicit connection"):
            dj.Schema("test_schema")

    def test_schema_with_connection_works_in_thread_safe_mode(self):
        """Schema with explicit connection works in thread-safe mode."""
        from unittest.mock import MagicMock, patch

        enable_thread_safe()

        # Create a mock connection with new API config (no global fallback)
        mock_conn = MagicMock(spec=dj.Connection)
        mock_conn.config = ConnectionConfig(_use_global_fallback=False)

        # Mock the schema activation to avoid database operations
        with patch.object(dj.Schema, "activate"):
            schema = dj.Schema("test_schema", connection=mock_conn)
            assert schema.connection is mock_conn
