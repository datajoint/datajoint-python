"""
DataJoint for Python — a framework for scientific data pipelines.

DataJoint introduces the Relational Workflow Model, where your database schema
is an executable specification of your workflow. Tables represent workflow steps,
foreign keys encode dependencies, and computations are declarative.

Documentation: https://docs.datajoint.com
Source: https://github.com/datajoint/datajoint-python

Copyright 2014-2026 DataJoint Inc. and contributors.
Licensed under the Apache License, Version 2.0.

If DataJoint contributes to a publication, please cite:
https://doi.org/10.1101/031658
"""

__author__ = "DataJoint Contributors"
__date__ = "November 7, 2020"
__all__ = [
    "__author__",
    "__version__",
    "config",
    "conn",
    "Connection",
    "Instance",
    "Schema",
    "VirtualModule",
    "virtual_schema",
    "list_schemas",
    "Table",
    "FreeTable",
    "Manual",
    "Lookup",
    "Imported",
    "Computed",
    "Part",
    "Not",
    "AndList",
    "Top",
    "U",
    "Diagram",
    "MatCell",
    "MatStruct",
    # Codec API
    "Codec",
    "SchemaCodec",
    "list_codecs",
    "get_codec",
    "ObjectRef",
    "NpyRef",
    # Other
    "errors",
    "migrate",
    "DataJointError",
    "ThreadSafetyError",
    "logger",
    "cli",
    "ValidationResult",
]

# =============================================================================
# Eager imports — core functionality needed immediately
# =============================================================================
from . import errors
from . import migrate
from .codecs import (
    Codec,
    get_codec,
    list_codecs,
)
from .builtin_codecs import (
    SchemaCodec,
    NpyRef,
)
from .blob import MatCell, MatStruct
from .connection import Connection
from .errors import DataJointError, ThreadSafetyError
from .expression import AndList, Not, Top, U
from .instance import Instance, _ConfigProxy, _get_singleton_connection, _global_config, _check_thread_safe
from .logging import logger
from .objectref import ObjectRef
from .schemas import _Schema, VirtualModule, list_schemas, virtual_schema
from .table import FreeTable as _FreeTable, Table, ValidationResult
from .user_tables import Computed, Imported, Lookup, Manual, Part
from .version import __version__

# =============================================================================
# Singleton-aware API
# =============================================================================
# config is a proxy that delegates to the singleton instance's config
config = _ConfigProxy()


def conn(
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    *,
    reset: bool = False,
    use_tls: bool | dict | None = None,
) -> Connection:
    """
    Return a persistent connection object.

    When called without arguments, returns the singleton connection using
    credentials from dj.config. When connection parameters are provided,
    updates the singleton connection with the new credentials.

    Parameters
    ----------
    host : str, optional
        Database hostname. If provided, updates singleton.
    user : str, optional
        Database username. If provided, updates singleton.
    password : str, optional
        Database password. If provided, updates singleton.
    reset : bool, optional
        If True, reset existing connection. Default False.
    use_tls : bool or dict, optional
        TLS encryption option.

    Returns
    -------
    Connection
        Database connection.

    Raises
    ------
    ThreadSafetyError
        If thread_safe mode is enabled.
    """
    import datajoint.instance as instance_module
    from pydantic import SecretStr

    _check_thread_safe()

    # If reset requested, always recreate
    # If credentials provided and no singleton exists, create one
    # If credentials provided and singleton exists, return existing singleton
    if reset or (
        instance_module._singleton_connection is None and (host is not None or user is not None or password is not None)
    ):
        # Use provided values or fall back to config
        host = host if host is not None else _global_config.database.host
        user = user if user is not None else _global_config.database.user
        raw_password = password if password is not None else _global_config.database.password
        password = raw_password.get_secret_value() if isinstance(raw_password, SecretStr) else raw_password
        port = _global_config.database.port
        use_tls = use_tls if use_tls is not None else _global_config.database.use_tls

        if user is None:
            from .errors import DataJointError

            raise DataJointError("Database user not configured. Set dj.config['database.user'] or pass user= argument.")
        if password is None:
            from .errors import DataJointError

            raise DataJointError(
                "Database password not configured. Set dj.config['database.password'] or pass password= argument."
            )

        instance_module._singleton_connection = Connection(host, user, password, port, use_tls)
        instance_module._singleton_connection._config = _global_config

    return _get_singleton_connection()


class Schema(_Schema):
    """
    Decorator that binds table classes to a database schema.

    When connection is not provided, uses the singleton connection.
    In thread-safe mode (``DJ_THREAD_SAFE=true``), a connection must be
    provided explicitly or use ``dj.Instance().Schema()`` instead.

    Parameters
    ----------
    schema_name : str, optional
        Database schema name. If omitted, call ``activate()`` later.
    context : dict, optional
        Namespace for foreign key lookup. None uses caller's context.
    connection : Connection, optional
        Database connection. Defaults to singleton connection.
    create_schema : bool, optional
        If False, raise error if schema doesn't exist. Default True.
    create_tables : bool, optional
        If False, raise error when accessing missing tables.
    add_objects : dict, optional
        Additional objects for declaration context.

    Raises
    ------
    ThreadSafetyError
        If thread_safe mode is enabled and no connection is provided.

    Examples
    --------
    >>> schema = dj.Schema('my_schema')
    >>> @schema
    ... class Session(dj.Manual):
    ...     definition = '''
    ...     session_id : int
    ...     '''
    """

    def __init__(
        self,
        schema_name: str | None = None,
        context: dict | None = None,
        *,
        connection: Connection | None = None,
        create_schema: bool = True,
        create_tables: bool | None = None,
        add_objects: dict | None = None,
    ) -> None:
        if connection is None:
            _check_thread_safe()
        super().__init__(
            schema_name,
            context=context,
            connection=connection,
            create_schema=create_schema,
            create_tables=create_tables,
            add_objects=add_objects,
        )


def FreeTable(conn_or_name, full_table_name: str | None = None) -> _FreeTable:
    """
    Create a FreeTable for accessing a table without a dedicated class.

    Can be called in two ways:
    - ``FreeTable("schema.table")`` - uses singleton connection
    - ``FreeTable(connection, "schema.table")`` - uses provided connection

    Parameters
    ----------
    conn_or_name : Connection or str
        Either a Connection object, or the full table name if using singleton.
    full_table_name : str, optional
        Full table name when first argument is a connection.

    Returns
    -------
    FreeTable
        A FreeTable instance for the specified table.

    Raises
    ------
    ThreadSafetyError
        If thread_safe mode is enabled and using singleton.
    """
    if full_table_name is None:
        # Called as FreeTable("db.table") - use singleton connection
        _check_thread_safe()
        return _FreeTable(_get_singleton_connection(), conn_or_name)
    else:
        # Called as FreeTable(conn, "db.table") - use provided connection
        return _FreeTable(conn_or_name, full_table_name)


# =============================================================================
# Lazy imports — heavy dependencies loaded on first access
# =============================================================================
# These modules import heavy dependencies (networkx, matplotlib, click, pymysql)
# that slow down `import datajoint`. They are loaded on demand.

_lazy_modules = {
    # Diagram imports networkx and matplotlib
    "Diagram": (".diagram", "Diagram"),
    "diagram": (".diagram", None),  # Return the module itself
    # cli imports click
    "cli": (".cli", "cli"),
}


def __getattr__(name: str):
    """Lazy import for heavy dependencies."""
    if name in _lazy_modules:
        module_path, attr_name = _lazy_modules[name]
        import importlib

        module = importlib.import_module(module_path, __package__)
        # If attr_name is None, return the module itself
        attr = module if attr_name is None else getattr(module, attr_name)
        # Cache in module __dict__ to avoid repeated __getattr__ calls
        # and to override the submodule that importlib adds automatically
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
