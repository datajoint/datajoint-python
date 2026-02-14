"""Tests for thread-safe mode functionality."""

import os

import pytest


class TestThreadSafeMode:
    """Test thread-safe mode behavior."""

    def test_thread_safe_env_var_true(self, monkeypatch):
        """DJ_THREAD_SAFE=true enables thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        # Re-import to pick up the new env var
        from datajoint.instance import _load_thread_safe

        assert _load_thread_safe() is True

    def test_thread_safe_env_var_false(self, monkeypatch):
        """DJ_THREAD_SAFE=false disables thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")

        from datajoint.instance import _load_thread_safe

        assert _load_thread_safe() is False

    def test_thread_safe_env_var_1(self, monkeypatch):
        """DJ_THREAD_SAFE=1 enables thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "1")

        from datajoint.instance import _load_thread_safe

        assert _load_thread_safe() is True

    def test_thread_safe_env_var_yes(self, monkeypatch):
        """DJ_THREAD_SAFE=yes enables thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "yes")

        from datajoint.instance import _load_thread_safe

        assert _load_thread_safe() is True

    def test_thread_safe_default_false(self, monkeypatch):
        """Thread-safe mode defaults to False."""
        monkeypatch.delenv("DJ_THREAD_SAFE", raising=False)

        from datajoint.instance import _load_thread_safe

        assert _load_thread_safe() is False


class TestConfigProxyThreadSafe:
    """Test ConfigProxy behavior in thread-safe mode."""

    def test_config_access_raises_in_thread_safe_mode(self, monkeypatch):
        """Accessing config raises ThreadSafetyError in thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj
        from datajoint.errors import ThreadSafetyError

        with pytest.raises(ThreadSafetyError):
            _ = dj.config.database

    def test_config_access_works_in_normal_mode(self, monkeypatch):
        """Accessing config works in normal mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")

        import datajoint as dj

        # Should not raise
        host = dj.config.database.host
        assert isinstance(host, str)

    def test_config_set_raises_in_thread_safe_mode(self, monkeypatch):
        """Setting config raises ThreadSafetyError in thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj
        from datajoint.errors import ThreadSafetyError

        with pytest.raises(ThreadSafetyError):
            dj.config.safemode = False

    def test_save_template_works_in_thread_safe_mode(self, monkeypatch, tmp_path):
        """save_template is a static method and works in thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj

        # Should not raise - save_template is static
        config_file = tmp_path / "datajoint.json"
        dj.config.save_template(str(config_file), create_secrets_dir=False)
        assert config_file.exists()


class TestConnThreadSafe:
    """Test conn() behavior in thread-safe mode."""

    def test_conn_raises_in_thread_safe_mode(self, monkeypatch):
        """conn() raises ThreadSafetyError in thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj
        from datajoint.errors import ThreadSafetyError

        with pytest.raises(ThreadSafetyError):
            dj.conn()


class TestSchemaThreadSafe:
    """Test Schema behavior in thread-safe mode."""

    def test_schema_raises_in_thread_safe_mode(self, monkeypatch):
        """Schema() raises ThreadSafetyError in thread-safe mode without connection."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj
        from datajoint.errors import ThreadSafetyError

        with pytest.raises(ThreadSafetyError):
            dj.Schema("test_schema")


class TestFreeTableThreadSafe:
    """Test FreeTable behavior in thread-safe mode."""

    def test_freetable_raises_in_thread_safe_mode(self, monkeypatch):
        """FreeTable() raises ThreadSafetyError in thread-safe mode without connection."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        import datajoint as dj
        from datajoint.errors import ThreadSafetyError

        with pytest.raises(ThreadSafetyError):
            dj.FreeTable("test.table")


class TestInstance:
    """Test Instance class."""

    def test_instance_import(self):
        """Instance class is importable."""
        from datajoint import Instance

        assert Instance is not None

    def test_instance_always_allowed_in_thread_safe_mode(self, monkeypatch):
        """Instance() is allowed even in thread-safe mode."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "true")

        from datajoint import Instance

        # Instance class should be accessible
        # (actual creation requires valid credentials)
        assert callable(Instance)


class TestThreadSafetyError:
    """Test ThreadSafetyError exception."""

    def test_error_is_datajoint_error(self):
        """ThreadSafetyError is a subclass of DataJointError."""
        from datajoint.errors import DataJointError, ThreadSafetyError

        assert issubclass(ThreadSafetyError, DataJointError)

    def test_error_in_exports(self):
        """ThreadSafetyError is exported from datajoint."""
        import datajoint as dj

        assert hasattr(dj, "ThreadSafetyError")
