"""Tests for thread-safe mode in connection management."""

import pytest

import datajoint as dj
from datajoint.errors import ThreadSafetyError


class TestConnThreadSafe:
    """Tests for dj.conn() in thread-safe mode."""

    def test_conn_blocked_in_thread_safe_mode(self, monkeypatch):
        """dj.conn() raises ThreadSafetyError when thread_safe is True."""
        # Enable thread-safe mode
        original = dj.config.thread_safe
        try:
            dj.config.thread_safe = True

            with pytest.raises(ThreadSafetyError, match="dj.conn\\(\\) is disabled"):
                dj.conn()
        finally:
            dj.config.thread_safe = original

    def test_conn_works_when_thread_safe_disabled(self, monkeypatch):
        """dj.conn() works normally when thread_safe is False."""
        # Ensure thread-safe mode is disabled
        original = dj.config.thread_safe
        try:
            dj.config.thread_safe = False

            # This will fail if no database is configured, but it shouldn't raise
            # ThreadSafetyError - that's what we're testing
            try:
                dj.conn(reset=True)
            except dj.DataJointError as e:
                # Expected if database credentials not configured
                assert "ThreadSafety" not in str(type(e))
        finally:
            dj.config.thread_safe = original


class TestConnectionFromConfig:
    """Tests for Connection.from_config() method."""

    def test_from_config_exists(self):
        """Connection.from_config class method exists."""
        assert hasattr(dj.Connection, "from_config")
        assert callable(dj.Connection.from_config)

    def test_from_config_requires_user(self):
        """from_config raises error if user not provided."""
        with pytest.raises(dj.DataJointError, match="user"):
            dj.Connection.from_config({"host": "localhost", "password": "test"})

    def test_from_config_requires_password(self):
        """from_config raises error if password not provided."""
        with pytest.raises(dj.DataJointError, match="password"):
            dj.Connection.from_config({"host": "localhost", "user": "test"})

    def test_from_config_with_explicit_params(self):
        """from_config accepts explicit keyword parameters."""
        # Mock the entire Connection class to avoid actual connection
        from unittest.mock import MagicMock, patch

        # Create a mock connection instance
        mock_conn_instance = MagicMock()
        mock_conn_instance.conn_info = {}

        # Capture the arguments passed to Connection.__init__
        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None):
            captured_args["host"] = host
            captured_args["user"] = user
            captured_args["password"] = password
            captured_args["port"] = port
            captured_args["backend"] = backend

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

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None):
            captured_args["host"] = host
            captured_args["user"] = user
            captured_args["port"] = port

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(cfg)

        assert captured_args["host"] == "dicthost"
        assert captured_args["user"] == "dictuser"
        assert captured_args["port"] == 3308

    def test_from_config_kwargs_override_dict(self):
        """Keyword arguments override dict values in from_config."""
        from unittest.mock import patch

        cfg = {
            "host": "dicthost",
            "user": "dictuser",
            "password": "dictpass",
        }

        captured_args = {}

        def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None):
            captured_args["host"] = host
            captured_args["user"] = user

        with patch.object(dj.Connection, "__init__", mock_init):
            dj.Connection.from_config(cfg, host="overridehost")

        # host should be overridden
        assert captured_args["host"] == "overridehost"
        # user should come from dict
        assert captured_args["user"] == "dictuser"

    def test_from_config_works_in_thread_safe_mode(self):
        """from_config works even when thread_safe is True."""
        from unittest.mock import patch

        original = dj.config.thread_safe
        try:
            dj.config.thread_safe = True

            captured_args = {}

            def mock_init(self, host, user, password, port=None, init_fun=None, use_tls=None, backend=None):
                captured_args["host"] = host

            with patch.object(dj.Connection, "__init__", mock_init):
                # This should NOT raise ThreadSafetyError
                dj.Connection.from_config(
                    host="testhost",
                    user="testuser",
                    password="testpass",
                )

            # Verify from_config was able to collect parameters in thread-safe mode
            assert captured_args["host"] == "testhost"
        finally:
            dj.config.thread_safe = original


class TestThreadSafetyErrorExport:
    """Tests for ThreadSafetyError availability."""

    def test_error_exported_from_main_module(self):
        """ThreadSafetyError is exported from datajoint module."""
        assert hasattr(dj, "ThreadSafetyError")
        assert dj.ThreadSafetyError is ThreadSafetyError

    def test_error_is_datajoint_error_subclass(self):
        """ThreadSafetyError is a subclass of DataJointError."""
        assert issubclass(ThreadSafetyError, dj.DataJointError)

    def test_error_has_descriptive_docstring(self):
        """ThreadSafetyError has a descriptive docstring."""
        assert ThreadSafetyError.__doc__ is not None
        assert "thread-safe" in ThreadSafetyError.__doc__.lower()
