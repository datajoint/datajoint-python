"""
DataJoint Instance for thread-safe operation.

An Instance encapsulates a config and connection pair, providing isolated
database contexts for multi-tenant applications.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from .connection import Connection
from .errors import ThreadSafetyError
from .settings import Config, _create_config

if TYPE_CHECKING:
    from .schemas import Schema as SchemaClass
    from .table import FreeTable as FreeTableClass


def _load_thread_safe() -> bool:
    """
    Load thread_safe setting from environment or config file.

    Returns
    -------
    bool
        True if thread-safe mode is enabled.
    """
    # Check environment variable first
    env_val = os.environ.get("DJ_THREAD_SAFE", "").lower()
    if env_val in ("true", "1", "yes"):
        return True
    if env_val in ("false", "0", "no"):
        return False

    # Default: thread-safe mode is off
    return False


class Instance:
    """
    Encapsulates a DataJoint configuration and connection.

    Each Instance has its own Config and Connection, providing isolation
    for multi-tenant applications. Use ``dj.Instance()`` to create isolated
    instances, or access the singleton via ``dj.config``, ``dj.conn()``, etc.

    Parameters
    ----------
    host : str
        Database hostname.
    user : str
        Database username.
    password : str
        Database password.
    port : int, optional
        Database port. Default from config or 3306.
    use_tls : bool or dict, optional
        TLS configuration.
    **kwargs : Any
        Additional config overrides applied to this instance's config.

    Attributes
    ----------
    config : Config
        Configuration for this instance.
    connection : Connection
        Database connection for this instance.

    Examples
    --------
    >>> inst = dj.Instance(host="localhost", user="root", password="secret")
    >>> inst.config.safemode = False
    >>> schema = inst.Schema("my_schema")
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int | None = None,
        use_tls: bool | dict | None = None,
        **kwargs: Any,
    ) -> None:
        # Create fresh config with defaults loaded from env/file
        self.config = _create_config()

        # Apply any config overrides from kwargs
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
            elif "__" in key:
                # Handle nested keys like database__reconnect
                parts = key.split("__")
                obj = self.config
                for part in parts[:-1]:
                    obj = getattr(obj, part)
                setattr(obj, parts[-1], value)

        # Determine port
        if port is None:
            port = self.config.database.port

        # Create connection
        self.connection = Connection(host, user, password, port, use_tls)

        # Attach config to connection so tables can access it
        self.connection._config = self.config

    def Schema(
        self,
        schema_name: str,
        *,
        context: dict[str, Any] | None = None,
        create_schema: bool = True,
        create_tables: bool | None = None,
        add_objects: dict[str, Any] | None = None,
    ) -> "SchemaClass":
        """
        Create a Schema bound to this instance's connection.

        Parameters
        ----------
        schema_name : str
            Database schema name.
        context : dict, optional
            Namespace for foreign key lookup.
        create_schema : bool, optional
            If False, raise error if schema doesn't exist. Default True.
        create_tables : bool, optional
            If False, raise error when accessing missing tables.
        add_objects : dict, optional
            Additional objects for declaration context.

        Returns
        -------
        Schema
            A Schema using this instance's connection.
        """
        from .schemas import Schema

        return Schema(
            schema_name,
            context=context,
            connection=self.connection,
            create_schema=create_schema,
            create_tables=create_tables,
            add_objects=add_objects,
        )

    def FreeTable(self, full_table_name: str) -> "FreeTableClass":
        """
        Create a FreeTable bound to this instance's connection.

        Parameters
        ----------
        full_table_name : str
            Full table name as ``'schema.table'`` or ```schema`.`table```.

        Returns
        -------
        FreeTable
            A FreeTable using this instance's connection.
        """
        from .table import FreeTable

        return FreeTable(self.connection, full_table_name)

    def __repr__(self) -> str:
        return f"Instance({self.connection!r})"


# =============================================================================
# Singleton management
# =============================================================================
# The global config is created at module load time and can be modified
# The singleton connection is created lazily when conn() or Schema() is called

_global_config: Config = _create_config()
_singleton_connection: Connection | None = None


def _check_thread_safe() -> None:
    """
    Check if thread-safe mode is enabled and raise if so.

    Raises
    ------
    ThreadSafetyError
        If thread_safe mode is enabled.
    """
    if _load_thread_safe():
        raise ThreadSafetyError(
            "Global DataJoint state is disabled in thread-safe mode. "
            "Use dj.Instance() to create an isolated instance."
        )


def _get_singleton_connection() -> Connection:
    """
    Get or create the singleton Connection.

    Uses credentials from the global config.

    Raises
    ------
    ThreadSafetyError
        If thread_safe mode is enabled.
    DataJointError
        If credentials are not configured.
    """
    global _singleton_connection

    _check_thread_safe()

    if _singleton_connection is None:
        from .errors import DataJointError

        host = _global_config.database.host
        user = _global_config.database.user
        password = _global_config.database.password
        if password is not None:
            password = password.get_secret_value()
        port = _global_config.database.port
        use_tls = _global_config.database.use_tls

        if user is None:
            raise DataJointError(
                "Database user not configured. Set dj.config['database.user'] or DJ_USER environment variable."
            )
        if password is None:
            raise DataJointError(
                "Database password not configured. Set dj.config['database.password'] or DJ_PASS environment variable."
            )

        _singleton_connection = Connection(host, user, password, port, use_tls)
        # Attach global config to connection
        _singleton_connection._config = _global_config

    return _singleton_connection


class _ConfigProxy:
    """
    Proxy that delegates to the global config, with thread-safety checks.

    In thread-safe mode, all access raises ThreadSafetyError.
    """

    def __getattr__(self, name: str) -> Any:
        _check_thread_safe()
        return getattr(_global_config, name)

    def __setattr__(self, name: str, value: Any) -> None:
        _check_thread_safe()
        setattr(_global_config, name, value)

    def __getitem__(self, key: str) -> Any:
        _check_thread_safe()
        return _global_config[key]

    def __setitem__(self, key: str, value: Any) -> None:
        _check_thread_safe()
        _global_config[key] = value

    def __delitem__(self, key: str) -> None:
        _check_thread_safe()
        del _global_config[key]

    def get(self, key: str, default: Any = None) -> Any:
        _check_thread_safe()
        return _global_config.get(key, default)

    def override(self, **kwargs: Any):
        _check_thread_safe()
        return _global_config.override(**kwargs)

    def load(self, filename: str) -> None:
        _check_thread_safe()
        return _global_config.load(filename)

    def get_store_spec(self, store: str | None = None, *, use_filepath_default: bool = False) -> dict[str, Any]:
        _check_thread_safe()
        return _global_config.get_store_spec(store, use_filepath_default=use_filepath_default)

    @staticmethod
    def save_template(
        path: str = "datajoint.json",
        minimal: bool = True,
        create_secrets_dir: bool = True,
    ):
        # save_template is a static method, no thread-safety check needed
        return Config.save_template(path, minimal, create_secrets_dir)

    def __repr__(self) -> str:
        if _load_thread_safe():
            return "ConfigProxy (thread-safe mode - use dj.Instance())"
        return repr(_global_config)
