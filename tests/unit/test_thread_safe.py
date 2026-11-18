"""Tests for thread-safe mode functionality."""

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


class TestInstanceBackend:
    """Test Instance backend parameter."""

    def test_instance_backend_sets_config(self, monkeypatch):
        """Instance(backend=...) sets config.database.backend."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")
        from datajoint.instance import Instance
        from unittest.mock import patch

        with patch("datajoint.instance.Connection"):
            inst = Instance(
                host="localhost",
                user="root",
                password="secret",
                backend="postgresql",
            )
            assert inst.config.database.backend == "postgresql"

    def test_instance_backend_default_from_config(self, monkeypatch, tmp_path):
        """Instance without backend uses the config default, postgresql on port 5432."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")
        monkeypatch.delenv("DJ_BACKEND", raising=False)
        monkeypatch.delenv("DJ_PORT", raising=False)
        # Keep a datajoint.json in a parent directory from supplying the backend
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)
        from datajoint.instance import Instance
        from unittest.mock import patch

        with patch("datajoint.instance.Connection") as MockConn:
            inst = Instance(
                host="localhost",
                user="postgres",
                password="secret",
            )
            assert inst.config.database.backend == "postgresql"
            args, kwargs = MockConn.call_args
            assert args[3] == 5432  # port is the 4th positional arg

    def test_instance_backend_affects_port_default(self, monkeypatch):
        """Instance(backend='postgresql') uses port 5432 by default."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")
        from datajoint.instance import Instance
        from unittest.mock import patch

        with patch("datajoint.instance.Connection") as MockConn:
            Instance(
                host="localhost",
                user="root",
                password="secret",
                backend="postgresql",
            )
            # Connection should be called with port 5432 (PostgreSQL default)
            args, kwargs = MockConn.call_args
            assert args[3] == 5432  # port is the 4th positional arg

    def test_instance_mysql_backend_uses_port_3306(self, monkeypatch):
        """Instance(backend='mysql') uses port 3306 by default."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")
        monkeypatch.delenv("DJ_PORT", raising=False)
        from datajoint.instance import Instance
        from unittest.mock import patch

        with patch("datajoint.instance.Connection") as MockConn:
            inst = Instance(
                host="localhost",
                user="root",
                password="secret",
                backend="mysql",
            )
            assert inst.config.database.backend == "mysql"
            args, kwargs = MockConn.call_args
            assert args[3] == 3306


class TestInstancePortFromBackend:
    """An Instance with no port connects on the default port of the backend it ends up with."""

    @pytest.fixture(autouse=True)
    def _isolate_config(self, monkeypatch):
        """Keep the shell environment and any datajoint.json from supplying a backend or port."""
        monkeypatch.setenv("DJ_THREAD_SAFE", "false")
        monkeypatch.setenv("DJ_IGNORE_CONFIG_FILE", "true")
        for var in ("DJ_BACKEND", "DJ_PORT"):
            monkeypatch.delenv(var, raising=False)

    @staticmethod
    def _connect(**kwargs):
        """Create an Instance with the connection mocked; return it and the port it connects on."""
        from unittest.mock import patch

        from datajoint.instance import Instance

        with patch("datajoint.instance.Connection") as MockConn:
            inst = Instance(host="localhost", user="user", password="secret", **kwargs)
        args, _ = MockConn.call_args
        return inst, args[3]  # port is the 4th positional arg

    def test_default_backend_uses_port_5432(self):
        """With no backend set anywhere, the backend is postgresql on port 5432."""
        inst, port = self._connect()
        assert inst.config.database.backend == "postgresql"
        assert port == 5432

    @pytest.mark.parametrize(
        ("backend", "port"),
        [("mysql", 3306), ("postgresql", 5432)],
    )
    def test_database_backend_override_sets_port(self, monkeypatch, backend, port):
        """Instance(database__backend=...) derives the port, whatever DJ_BACKEND selected."""
        monkeypatch.setenv("DJ_BACKEND", "postgresql" if backend == "mysql" else "mysql")
        inst, connected_port = self._connect(database__backend=backend)
        assert inst.config.database.backend == backend
        assert inst.config.database.port == port
        assert connected_port == port

    def test_dj_backend_env_uses_port_3306(self, monkeypatch):
        """DJ_BACKEND=mysql with no DJ_PORT connects on port 3306."""
        monkeypatch.setenv("DJ_BACKEND", "mysql")
        inst, port = self._connect()
        assert inst.config.database.backend == "mysql"
        assert port == 3306

    @pytest.mark.parametrize("override", ["backend", "database__backend"])
    def test_backend_override_keeps_configured_port(self, monkeypatch, override):
        """A port set by DJ_PORT is kept when the Instance selects the backend."""
        monkeypatch.setenv("DJ_PORT", "3307")
        inst, port = self._connect(**{override: "mysql"})
        assert inst.config.database.backend == "mysql"
        assert port == 3307


class TestCrossConnectionValidation:
    """Test that cross-connection operations are rejected."""

    def test_join_different_connections_raises(self):
        """Join of expressions from different connections raises DataJointError."""
        from datajoint.expression import QueryExpression
        from datajoint.errors import DataJointError
        from unittest.mock import MagicMock

        expr1 = QueryExpression()
        expr1._connection = MagicMock()
        expr1._heading = MagicMock()
        expr1._heading.names = []

        expr2 = QueryExpression()
        expr2._connection = MagicMock()  # different connection object
        expr2._heading = MagicMock()
        expr2._heading.names = []

        with pytest.raises(DataJointError, match="different connections"):
            expr1 * expr2

    def test_join_same_connection_allowed(self):
        """Join of expressions from the same connection does not raise."""
        from datajoint.condition import assert_join_compatibility
        from datajoint.expression import QueryExpression
        from unittest.mock import MagicMock

        shared_conn = MagicMock()

        expr1 = QueryExpression()
        expr1._connection = shared_conn
        expr1._heading = MagicMock()
        expr1._heading.names = []
        expr1._heading.lineage_available = False

        expr2 = QueryExpression()
        expr2._connection = shared_conn
        expr2._heading = MagicMock()
        expr2._heading.names = []
        expr2._heading.lineage_available = False

        # Should not raise
        assert_join_compatibility(expr1, expr2)

    def test_restriction_different_connections_raises(self):
        """Restriction by expression from different connection raises DataJointError."""
        from datajoint.expression import QueryExpression
        from datajoint.errors import DataJointError
        from unittest.mock import MagicMock

        expr1 = QueryExpression()
        expr1._connection = MagicMock()
        expr1._heading = MagicMock()
        expr1._heading.names = ["a"]
        expr1._heading.__getitem__ = MagicMock()
        expr1._heading.new_attributes = set()
        expr1._support = ["`db`.`t1`"]
        expr1._restriction = []
        expr1._restriction_attributes = set()
        expr1._joins = []
        expr1._top = None
        expr1._original_heading = expr1._heading

        expr2 = QueryExpression()
        expr2._connection = MagicMock()  # different connection
        expr2._heading = MagicMock()
        expr2._heading.names = ["a"]

        with pytest.raises(DataJointError, match="different connections"):
            expr1 & expr2


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
