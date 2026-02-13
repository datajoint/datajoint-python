"""
This module contains the Connection class that manages the connection to the database, and
the ``conn`` function that provides access to a persistent connection in datajoint.
"""

from __future__ import annotations

import hashlib
import logging
import pathlib
import re
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from . import errors
from .adapters import get_adapter
from .blob import pack, unpack
from .dependencies import Dependencies
from .settings import config
from .version import __version__

logger = logging.getLogger(__name__.split(".")[0])
query_log_max_length = 300


cache_key = "query_cache"  # the key to lookup the query_cache folder in dj.config


class ConnectionConfig:
    """
    Connection-scoped configuration (read/write).

    Provides access to settings that can vary per connection. Behavior depends on
    how the connection was created:

    - **New API** (``Connection.from_config()``): Uses explicit values or defaults.
      Never accesses global config. Works identically with ``thread_safe`` on or off.
    - **Legacy API** (``dj.conn()``): Forwards unset values to global ``dj.config``
      for backward compatibility.

    Parameters
    ----------
    **explicit_values : Any
        Explicitly provided configuration values. These take precedence over
        global config and defaults.

    Attributes
    ----------
    safemode : bool
        Require confirmation for destructive operations.
    database_prefix : str
        Prefix for schema names.
    stores : dict
        Blob storage configuration.
    cache : Path or None
        Local cache directory.
    query_cache : Path or None
        Query cache directory.
    reconnect : bool
        Auto-reconnect on lost connection.
    display_limit : int
        Max rows to display.
    display_width : int
        Column width for display.
    show_tuple_count : bool
        Show tuple count in repr.
    loglevel : str
        Logging level.
    filepath_checksum_size_limit : int or None
        Max file size for checksum.

    Examples
    --------
    Access settings through a connection:

    >>> conn = dj.Connection.from_config(host="localhost", user="root", password="pw")
    >>> conn.config.safemode
    True
    >>> conn.config.safemode = False  # Disable for this connection only
    >>> conn.config.display_limit = 25
    """

    _DEFAULTS: dict[str, Any] = {
        "safemode": True,
        "database_prefix": "",
        "stores": {},
        "cache": None,
        "query_cache": None,
        "reconnect": True,
        "create_tables": True,
        "display_limit": 12,
        "display_width": 14,
        "show_tuple_count": True,
        "loglevel": "INFO",
        "filepath_checksum_size_limit": None,
    }

    # Mapping from ConnectionConfig names to global config paths
    _GLOBAL_CONFIG_MAP: dict[str, str] = {
        "safemode": "safemode",
        "database_prefix": "database.database_prefix",
        "stores": "stores",
        "cache": "cache",
        "query_cache": "query_cache",
        "reconnect": "database.reconnect",
        "create_tables": "database.create_tables",
        "display_limit": "display.limit",
        "display_width": "display.width",
        "show_tuple_count": "display.show_tuple_count",
        "loglevel": "loglevel",
        "filepath_checksum_size_limit": "filepath_checksum_size_limit",
    }

    def __init__(self, **explicit_values: Any) -> None:
        object.__setattr__(self, "_values", {})  # Mutable storage for this connection
        # If True, forward unset values to global config (legacy API behavior)
        # If False, use defaults only (new API behavior)
        object.__setattr__(self, "_use_global_fallback", explicit_values.pop("_use_global_fallback", False))
        self._values.update(explicit_values)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        # If set on this connection, return that value
        if name in self._values:
            return self._values[name]

        # Legacy API: forward to global config for backward compatibility
        if self._use_global_fallback:
            global_path = self._GLOBAL_CONFIG_MAP.get(name)
            if global_path:
                return config[global_path]

        # New API: use defaults only (no global config access)
        return self._DEFAULTS.get(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            return object.__setattr__(self, name, value)

        # Store in connection-local values
        self._values[name] = value

    def __repr__(self) -> str:
        items = []
        for name in self._DEFAULTS:
            value = getattr(self, name)
            items.append(f"{name}={value!r}")
        return f"ConnectionConfig({', '.join(items)})"

    def get_store_spec(self, store_name: str) -> dict:
        """
        Get store specification by name.

        Parameters
        ----------
        store_name : str
            Name of the store to retrieve.

        Returns
        -------
        dict
            Store specification dictionary.

        Raises
        ------
        DataJointError
            If the store is not configured.
        """
        stores = self.stores
        if store_name not in stores:
            raise errors.DataJointError(f"Store '{store_name}' is not configured.")
        return stores[store_name]


def translate_query_error(client_error: Exception, query: str, adapter) -> Exception:
    """
    Translate client error to the corresponding DataJoint exception.

    Parameters
    ----------
    client_error : Exception
        The exception raised by the client interface.
    query : str
        SQL query with placeholders.
    adapter : DatabaseAdapter
        The database adapter instance.

    Returns
    -------
    Exception
        An instance of the corresponding DataJoint error subclass,
        or the original error if no mapping exists.
    """
    logger.debug("type: {}, args: {}".format(type(client_error), client_error.args))
    return adapter.translate_error(client_error, query)


def conn(
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    *,
    init_fun: Callable | None = None,
    reset: bool = False,
    use_tls: bool | dict | None = None,
) -> "Connection":
    """
    Return a persistent connection object shared by multiple modules.

    If the connection is not yet established or reset=True, a new connection is set up.
    If connection information is not provided, it is taken from config.

    .. warning::

        This function uses global state and is not suitable for multi-tenant
        applications. When ``config.thread_safe`` is True, this function raises
        :exc:`~datajoint.errors.ThreadSafetyError`. Use
        :meth:`Connection.from_config` instead for thread-safe connection management.

    Parameters
    ----------
    host : str, optional
        Database hostname.
    user : str, optional
        Database username. Required if not set in config.
    password : str, optional
        Database password. Required if not set in config.
    init_fun : callable, optional
        Initialization function called after connection.
    reset : bool, optional
        If True, reset existing connection. Default False.
    use_tls : bool or dict, optional
        TLS encryption option: True (required), False (no TLS),
        None (preferred, default), or dict for manual configuration.

    Returns
    -------
    Connection
        Persistent database connection.

    Raises
    ------
    DataJointError
        If user or password is not provided and not set in config.
    ThreadSafetyError
        If ``config.thread_safe`` is True.

    See Also
    --------
    Connection.from_config : Thread-safe connection creation.
    """
    # Check thread-safe mode
    if config.thread_safe:
        raise errors.ThreadSafetyError(
            "dj.conn() is disabled in thread-safe mode. " "Use Connection.from_config() with explicit configuration."
        )

    if not hasattr(conn, "connection") or reset:
        host = host if host is not None else config["database.host"]
        user = user if user is not None else config["database.user"]
        password = password if password is not None else config["database.password"]
        if user is None:
            raise errors.DataJointError(
                "Database user not configured. Set datajoint.config['database.user'] or pass user= argument."
            )
        if password is None:
            raise errors.DataJointError(
                "Database password not configured. Set datajoint.config['database.password'] or pass password= argument."
            )
        init_fun = init_fun if init_fun is not None else config["connection.init_function"]
        use_tls = use_tls if use_tls is not None else config["database.use_tls"]
        conn.connection = Connection(host, user, password, None, init_fun, use_tls)
    return conn.connection


class EmulatedCursor:
    """acts like a cursor"""

    def __init__(self, data):
        self._data = data
        self._iter = iter(self._data)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)

    def fetchall(self):
        return self._data

    def fetchone(self):
        return next(self._iter)

    @property
    def rowcount(self):
        return len(self._data)


class Connection:
    """
    Manages a connection to a database server.

    Catalogues schemas, tables, and their dependencies (foreign keys).
    Most parameters should be set in the configuration file.

    Parameters
    ----------
    host : str
        Hostname, may include port as ``hostname:port``.
    user : str
        Database username.
    password : str
        Database password.
    port : int, optional
        Port number. Overridden if specified in host.
    init_fun : str, optional
        SQL initialization command.
    use_tls : bool or dict, optional
        TLS encryption option.
    backend : str, optional
        Database backend ('mysql' or 'postgresql'). If not provided,
        uses the value from global config.

    Attributes
    ----------
    config : ConnectionConfig
        Connection-scoped configuration settings.
    schemas : dict
        Registered schema objects.
    dependencies : Dependencies
        Foreign key dependency graph.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int | None = None,
        init_fun: str | None = None,
        use_tls: bool | dict | None = None,
        backend: str | None = None,
        *,
        _config: ConnectionConfig | None = None,
    ) -> None:
        if ":" in host:
            # the port in the hostname overrides the port argument
            host, port = host.split(":")
            port = int(port)
        elif port is None:
            # In thread-safe mode, config is inaccessible, so we must use defaults
            if config.thread_safe:
                # Default based on backend
                port = 5432 if backend == "postgresql" else 3306
            else:
                port = config.database.port
        self.conn_info = dict(host=host, port=port, user=user, passwd=password)
        if use_tls is not False:
            # use_tls can be: None (auto-detect), True (enable), False (disable), or dict (custom config)
            if isinstance(use_tls, dict):
                self.conn_info["ssl"] = use_tls
            elif use_tls is None:
                # Auto-detect: try SSL, fallback to non-SSL if server doesn't support it
                self.conn_info["ssl"] = True
            else:
                # use_tls=True: enable SSL with default settings
                self.conn_info["ssl"] = True
        self.conn_info["ssl_input"] = use_tls
        self.init_fun = init_fun
        self._conn = None
        self._query_cache = None
        self._is_closed = True  # Mark as closed until connect() succeeds

        # Select adapter based on backend
        if backend is None:
            if config.thread_safe:
                backend = "mysql"  # Default in thread-safe mode
            else:
                backend = config.database.backend
        self.adapter = get_adapter(backend)

        self.connect()
        if self.is_connected:
            logger.info("DataJoint {version} connected to {user}@{host}:{port}".format(version=__version__, **self.conn_info))
            self.connection_id = self.adapter.get_connection_id(self._conn)
        else:
            raise errors.LostConnectionError("Connection failed {user}@{host}:{port}".format(**self.conn_info))
        self._in_transaction = False
        self.schemas = dict()
        self.dependencies = Dependencies(self)

        # Connection-scoped configuration
        # Legacy API (dj.conn()) uses global fallback for backward compatibility
        self.config = _config if _config is not None else ConnectionConfig(_use_global_fallback=True)

    @classmethod
    def from_config(
        cls,
        cfg: dict | None = None,
        *,
        host: str | None = None,
        user: str | None = None,
        password: str | None = None,
        port: int | None = None,
        backend: str | None = None,
        init_fun: str | None = None,
        use_tls: bool | dict | None = None,
        # Connection-scoped settings
        safemode: bool | None = None,
        database_prefix: str | None = None,
        stores: dict | None = None,
        cache: Path | str | None = None,
        query_cache: Path | str | None = None,
        reconnect: bool | None = None,
        display_limit: int | None = None,
        display_width: int | None = None,
        show_tuple_count: bool | None = None,
        loglevel: str | None = None,
        filepath_checksum_size_limit: int | None = None,
    ) -> "Connection":
        """
        Create a connection from explicit configuration.

        This is the recommended method for creating connections. It works in both
        ``thread_safe=False`` and ``thread_safe=True`` modes. Unlike :func:`conn`,
        this method does not require global state.

        Configuration can be provided via a dict or keyword arguments.
        Keyword arguments take precedence over dict values.

        Parameters
        ----------
        cfg : dict, optional
            Configuration dict with connection and settings keys.
        host : str, optional
            Database hostname. Overrides cfg['host']. Default: 'localhost'.
        user : str, optional
            Database username. Overrides cfg['user']. Required.
        password : str, optional
            Database password. Overrides cfg['password']. Required.
        port : int, optional
            Database port. Overrides cfg['port']. Default: 3306 (MySQL) or 5432 (PostgreSQL).
        backend : str, optional
            Database backend ('mysql' or 'postgresql'). Overrides cfg['backend'].
            Default: 'mysql'.
        init_fun : str, optional
            SQL initialization command. Overrides cfg['init_function'].
        use_tls : bool or dict, optional
            TLS encryption option. Overrides cfg['use_tls'].
        safemode : bool, optional
            Require confirmation for destructive operations. Default: True.
        database_prefix : str, optional
            Prefix for schema names. Default: ''.
        stores : dict, optional
            Blob storage configuration. Default: {}.
        cache : Path or str, optional
            Local cache directory. Default: None.
        query_cache : Path or str, optional
            Query cache directory. Default: None.
        reconnect : bool, optional
            Auto-reconnect on lost connection. Default: True.
        display_limit : int, optional
            Max rows to display. Default: 12.
        display_width : int, optional
            Column width for display. Default: 14.
        show_tuple_count : bool, optional
            Show tuple count in repr. Default: True.
        loglevel : str, optional
            Logging level. Default: 'INFO'.
        filepath_checksum_size_limit : int, optional
            Max file size for checksum. Default: None.

        Returns
        -------
        Connection
            A new database connection.

        Raises
        ------
        DataJointError
            If required parameters (user, password) are not provided.

        Examples
        --------
        Create connection with explicit parameters:

        >>> conn = Connection.from_config(
        ...     host='localhost',
        ...     user='myuser',
        ...     password='mypassword'
        ... )

        Create connection from a config dict (e.g., from request context):

        >>> tenant_config = {
        ...     'host': 'db.example.com',
        ...     'user': request.user.db_user,
        ...     'password': request.user.db_password,
        ... }
        >>> conn = Connection.from_config(tenant_config)

        Use with Schema for thread-safe pipeline access:

        >>> conn = Connection.from_config(tenant_config)
        >>> schema = dj.Schema('my_pipeline', connection=conn)

        See Also
        --------
        conn : Singleton connection (not available in thread-safe mode).
        """
        # Start with defaults (no global config access)
        effective_host = "localhost"
        effective_user = None
        effective_password = None
        effective_port = None  # Will be set based on backend
        effective_backend = "mysql"
        effective_init_fun = None
        effective_use_tls = None

        # Connection-scoped settings (will be passed to ConnectionConfig)
        config_kwargs: dict[str, Any] = {}

        # Override with cfg dict if provided
        if cfg is not None:
            # Connection parameters
            if "host" in cfg:
                effective_host = cfg["host"]
            if "user" in cfg:
                effective_user = cfg["user"]
            if "password" in cfg:
                effective_password = cfg["password"]
            if "port" in cfg:
                effective_port = cfg["port"]
            if "backend" in cfg:
                effective_backend = cfg["backend"]
            if "init_function" in cfg:
                effective_init_fun = cfg["init_function"]
            if "use_tls" in cfg:
                effective_use_tls = cfg["use_tls"]

            # Connection-scoped settings from cfg dict
            if "safemode" in cfg:
                config_kwargs["safemode"] = cfg["safemode"]
            if "database_prefix" in cfg:
                config_kwargs["database_prefix"] = cfg["database_prefix"]
            if "stores" in cfg:
                config_kwargs["stores"] = cfg["stores"]
            if "cache" in cfg:
                config_kwargs["cache"] = cfg["cache"]
            if "query_cache" in cfg:
                config_kwargs["query_cache"] = cfg["query_cache"]
            if "reconnect" in cfg:
                config_kwargs["reconnect"] = cfg["reconnect"]
            if "display_limit" in cfg:
                config_kwargs["display_limit"] = cfg["display_limit"]
            if "display_width" in cfg:
                config_kwargs["display_width"] = cfg["display_width"]
            if "show_tuple_count" in cfg:
                config_kwargs["show_tuple_count"] = cfg["show_tuple_count"]
            if "loglevel" in cfg:
                config_kwargs["loglevel"] = cfg["loglevel"]
            if "filepath_checksum_size_limit" in cfg:
                config_kwargs["filepath_checksum_size_limit"] = cfg["filepath_checksum_size_limit"]

        # Override with explicit keyword arguments (connection params)
        if host is not None:
            effective_host = host
        if user is not None:
            effective_user = user
        if password is not None:
            effective_password = password
        if port is not None:
            effective_port = port
        if backend is not None:
            effective_backend = backend
        if init_fun is not None:
            effective_init_fun = init_fun
        if use_tls is not None:
            effective_use_tls = use_tls

        # Override with explicit keyword arguments (connection-scoped settings)
        if safemode is not None:
            config_kwargs["safemode"] = safemode
        if database_prefix is not None:
            config_kwargs["database_prefix"] = database_prefix
        if stores is not None:
            config_kwargs["stores"] = stores
        if cache is not None:
            config_kwargs["cache"] = cache
        if query_cache is not None:
            config_kwargs["query_cache"] = query_cache
        if reconnect is not None:
            config_kwargs["reconnect"] = reconnect
        if display_limit is not None:
            config_kwargs["display_limit"] = display_limit
        if display_width is not None:
            config_kwargs["display_width"] = display_width
        if show_tuple_count is not None:
            config_kwargs["show_tuple_count"] = show_tuple_count
        if loglevel is not None:
            config_kwargs["loglevel"] = loglevel
        if filepath_checksum_size_limit is not None:
            config_kwargs["filepath_checksum_size_limit"] = filepath_checksum_size_limit

        # Set default port based on backend if not specified
        if effective_port is None:
            effective_port = 5432 if effective_backend == "postgresql" else 3306

        # Validate required fields
        if effective_user is None:
            raise errors.DataJointError(
                "Database user is required. " "Provide user= argument or include 'user' in config dict."
            )
        if effective_password is None:
            raise errors.DataJointError(
                "Database password is required. " "Provide password= argument or include 'password' in config dict."
            )

        # Create ConnectionConfig - new API never falls back to global config
        conn_config = ConnectionConfig(
            _use_global_fallback=False,
            **config_kwargs,
        )

        # Create connection with explicit backend parameter and config
        connection = cls(
            host=effective_host,
            user=effective_user,
            password=effective_password,
            port=effective_port,
            init_fun=effective_init_fun,
            use_tls=effective_use_tls,
            backend=effective_backend,
            _config=conn_config,
        )

        return connection

    def __eq__(self, other):
        return self.conn_info == other.conn_info

    def __repr__(self):
        connected = "connected" if self.is_connected else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(connected=connected, **self.conn_info)

    def connect(self) -> None:
        """Establish or re-establish connection to the database server."""
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", ".*deprecated.*")
            try:
                # Use adapter to create connection
                self._conn = self.adapter.connect(
                    host=self.conn_info["host"],
                    port=self.conn_info["port"],
                    user=self.conn_info["user"],
                    password=self.conn_info["passwd"],
                    init_command=self.init_fun,
                    use_tls=self.conn_info.get("ssl"),
                )
            except Exception as ssl_error:
                # If SSL fails, retry without SSL (if it was auto-detected)
                if self.conn_info.get("ssl_input") is None:
                    logger.warning(
                        "SSL connection failed (%s). Falling back to non-SSL connection. "
                        "To require SSL, set use_tls=True explicitly.",
                        ssl_error,
                    )
                    self._conn = self.adapter.connect(
                        host=self.conn_info["host"],
                        port=self.conn_info["port"],
                        user=self.conn_info["user"],
                        password=self.conn_info["passwd"],
                        init_command=self.init_fun,
                        use_tls=False,  # Explicitly disable SSL for fallback
                    )
                else:
                    raise
        self._is_closed = False  # Mark as connected after successful connection

    def set_query_cache(self, query_cache: str | None = None) -> None:
        """
        Enable query caching mode.

        When enabled:
        1. Only SELECT queries are allowed
        2. Results are cached under ``dj.config['query_cache']``
        3. Cache key differentiates cache states

        Parameters
        ----------
        query_cache : str, optional
            String to initialize the hash for query results.
            None disables caching.
        """
        self._query_cache = query_cache

    def purge_query_cache(self) -> None:
        """Delete all cached query results."""
        if config.thread_safe:
            # Query caching requires global config; not supported in thread-safe mode
            return
        if isinstance(config.get(cache_key), str) and pathlib.Path(config[cache_key]).is_dir():
            for path in pathlib.Path(config[cache_key]).iterdir():
                if not path.is_dir():
                    path.unlink()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
        self._is_closed = True

    def __enter__(self) -> "Connection":
        """
        Enter context manager.

        Returns
        -------
        Connection
            This connection object.

        Examples
        --------
        >>> with dj.Connection(host, user, password) as conn:
        ...     schema = dj.Schema('my_schema', connection=conn)
        ...     # perform operations
        ... # connection automatically closed
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Exit context manager and close connection.

        Parameters
        ----------
        exc_type : type or None
            Exception type if an exception was raised.
        exc_val : Exception or None
            Exception instance if an exception was raised.
        exc_tb : traceback or None
            Traceback if an exception was raised.

        Returns
        -------
        bool
            False to propagate exceptions.
        """
        self.close()
        return False

    def register(self, schema) -> None:
        """
        Register a schema with this connection.

        Parameters
        ----------
        schema : Schema
            Schema object to register.
        """
        self.schemas[schema.database] = schema
        self.dependencies.clear()

    def ping(self) -> None:
        """
        Ping the server to verify connection is alive.

        Raises
        ------
        Exception
            If the connection is closed.
        """
        self.adapter.ping(self._conn)

    @property
    def is_connected(self) -> bool:
        """
        Check if connected to the database server.

        Returns
        -------
        bool
            True if connected.
        """
        if self._is_closed:
            return False
        try:
            self.ping()
        except:
            self._is_closed = True
            return False
        return True

    def _execute_query(self, cursor, query, args, suppress_warnings):
        try:
            with warnings.catch_warnings():
                if suppress_warnings:
                    # suppress all warnings arising from underlying SQL library
                    warnings.simplefilter("ignore")
                cursor.execute(query, args)
        except Exception as err:
            raise translate_query_error(err, query, self.adapter)

    def query(
        self,
        query: str,
        args: tuple = (),
        *,
        as_dict: bool = False,
        suppress_warnings: bool = True,
        reconnect: bool | None = None,
    ):
        """
        Execute a SQL query and return the cursor.

        Parameters
        ----------
        query : str
            SQL query to execute.
        args : tuple, optional
            Query parameters for prepared statement.
        as_dict : bool, optional
            If True, return rows as dictionaries. Default False.
        suppress_warnings : bool, optional
            If True, suppress SQL library warnings. Default True.
        reconnect : bool, optional
            If True, reconnect if disconnected. None uses config setting.

        Returns
        -------
        cursor
            Database cursor with query results.

        Raises
        ------
        DataJointError
            If non-SELECT query during query caching mode.
        """
        # check cache first:
        use_query_cache = bool(self._query_cache)
        if use_query_cache and not re.match(r"\s*(SELECT|SHOW)", query):
            raise errors.DataJointError("Only SELECT queries are allowed when query caching is on.")
        if use_query_cache:
            if not config[cache_key]:
                raise errors.DataJointError(f"Provide filepath dj.config['{cache_key}'] when using query caching.")
            # Cache key is backend-specific (no identifier normalization needed)
            hash_ = hashlib.md5((str(self._query_cache)).encode() + pack(args) + query.encode()).hexdigest()
            cache_path = pathlib.Path(config[cache_key]) / str(hash_)
            try:
                buffer = cache_path.read_bytes()
            except FileNotFoundError:
                pass  # proceed to query the database
            else:
                return EmulatedCursor(unpack(buffer))

        if reconnect is None:
            reconnect = self.config.reconnect
        logger.debug("Executing SQL:" + query[:query_log_max_length])
        cursor = self.adapter.get_cursor(self._conn, as_dict=as_dict)
        try:
            self._execute_query(cursor, query, args, suppress_warnings)
        except errors.LostConnectionError:
            if not reconnect:
                raise
            logger.warning("Reconnecting to database server.")
            self.connect()
            if self._in_transaction:
                self.cancel_transaction()
                raise errors.LostConnectionError("Connection was lost during a transaction.")
            logger.debug("Re-executing")
            cursor = self.adapter.get_cursor(self._conn, as_dict=as_dict)
            self._execute_query(cursor, query, args, suppress_warnings)

        if use_query_cache:
            data = cursor.fetchall()
            cache_path.write_bytes(pack(data))
            return EmulatedCursor(data)

        return cursor

    def get_user(self) -> str:
        """
        Get the current user and host.

        Returns
        -------
        str
            User name and host as ``'user@host'``.
        """
        return self.query(f"SELECT {self.adapter.current_user_expr()}").fetchone()[0]

    # ---------- transaction processing
    @property
    def in_transaction(self) -> bool:
        """
        Check if a transaction is open.

        Returns
        -------
        bool
            True if a transaction is in progress.
        """
        self._in_transaction = self._in_transaction and self.is_connected
        return self._in_transaction

    def start_transaction(self) -> None:
        """
        Start a new transaction.

        Raises
        ------
        DataJointError
            If a transaction is already in progress.
        """
        if self.in_transaction:
            raise errors.DataJointError("Nested connections are not supported.")
        self.query(self.adapter.start_transaction_sql())
        self._in_transaction = True
        logger.debug("Transaction started")

    def cancel_transaction(self) -> None:
        """Cancel the current transaction and roll back all changes."""
        self.query(self.adapter.rollback_sql())
        self._in_transaction = False
        logger.debug("Transaction cancelled. Rolling back ...")

    def commit_transaction(self) -> None:
        """Commit all changes and close the transaction."""
        self.query(self.adapter.commit_sql())
        self._in_transaction = False
        logger.debug("Transaction committed and closed.")

    # -------- context manager for transactions
    @property
    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.

        Opens a transaction and automatically commits on success or rolls back
        on exception.

        Yields
        ------
        Connection
            This connection object.

        Examples
        --------
        >>> with dj.conn().transaction:
        ...     # All operations here are in one transaction
        ...     table.insert(data)
        """
        try:
            self.start_transaction()
            yield self
        except:
            self.cancel_transaction()
            raise
        else:
            self.commit_transaction()
