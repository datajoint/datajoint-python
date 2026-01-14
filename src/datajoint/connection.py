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
from getpass import getpass
from typing import Callable

import pymysql as client

from . import errors
from .blob import pack, unpack
from .dependencies import Dependencies
from .settings import config
from .version import __version__

logger = logging.getLogger(__name__.split(".")[0])
query_log_max_length = 300


cache_key = "query_cache"  # the key to lookup the query_cache folder in dj.config


def translate_query_error(client_error: Exception, query: str) -> Exception:
    """
    Translate client error to the corresponding DataJoint exception.

    Parameters
    ----------
    client_error : Exception
        The exception raised by the client interface.
    query : str
        SQL query with placeholders.

    Returns
    -------
    Exception
        An instance of the corresponding DataJoint error subclass,
        or the original error if no mapping exists.
    """
    logger.debug("type: {}, args: {}".format(type(client_error), client_error.args))

    err, *args = client_error.args

    match err:
        # Loss of connection errors
        case 0 | "(0, '')":
            return errors.LostConnectionError("Server connection lost due to an interface error.", *args)
        case 2006:
            return errors.LostConnectionError("Connection timed out", *args)
        case 2013:
            return errors.LostConnectionError("Server connection lost", *args)

        # Access errors
        case 1044 | 1142:
            return errors.AccessError("Insufficient privileges.", args[0], query)

        # Integrity errors
        case 1062:
            return errors.DuplicateError(*args)
        case 1217 | 1451 | 1452 | 3730:
            # 1217: Cannot delete parent row (FK constraint)
            # 1451: Cannot delete/update parent row (FK constraint)
            # 1452: Cannot add/update child row (FK constraint)
            # 3730: Cannot drop table referenced by FK constraint
            return errors.IntegrityError(*args)

        # Syntax errors
        case 1064:
            return errors.QuerySyntaxError(args[0], query)

        # Existence errors
        case 1146:
            return errors.MissingTableError(args[0], query)
        case 1364:
            return errors.MissingAttributeError(*args)
        case 1054:
            return errors.UnknownAttributeError(*args)

        # All other errors pass through unchanged
        case _:
            return client_error


def conn(
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    *,
    init_fun: Callable | None = None,
    reset: bool = False,
    use_tls: bool | dict | None = None,
) -> Connection:
    """
    Return a persistent connection object shared by multiple modules.

    If the connection is not yet established or reset=True, a new connection is set up.
    If connection information is not provided, it is taken from config.

    Parameters
    ----------
    host : str, optional
        Database hostname.
    user : str, optional
        MySQL username.
    password : str, optional
        MySQL password. Prompts if not provided.
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
    """
    if not hasattr(conn, "connection") or reset:
        host = host if host is not None else config["database.host"]
        user = user if user is not None else config["database.user"]
        password = password if password is not None else config["database.password"]
        if user is None:
            user = input("Please enter DataJoint username: ")
        if password is None:
            password = getpass(prompt="Please enter DataJoint password: ")
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

    Attributes
    ----------
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
    ) -> None:
        if ":" in host:
            # the port in the hostname overrides the port argument
            host, port = host.split(":")
            port = int(port)
        elif port is None:
            port = config["database.port"]
        self.conn_info = dict(host=host, port=port, user=user, passwd=password)
        if use_tls is not False:
            self.conn_info["ssl"] = use_tls if isinstance(use_tls, dict) else {"ssl": {}}
        self.conn_info["ssl_input"] = use_tls
        self.init_fun = init_fun
        self._conn = None
        self._query_cache = None
        self.connect()
        if self.is_connected:
            logger.info("DataJoint {version} connected to {user}@{host}:{port}".format(version=__version__, **self.conn_info))
            self.connection_id = self.query("SELECT connection_id()").fetchone()[0]
        else:
            raise errors.LostConnectionError("Connection failed {user}@{host}:{port}".format(**self.conn_info))
        self._in_transaction = False
        self.schemas = dict()
        self.dependencies = Dependencies(self)

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
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                    "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
                    charset=config["connection.charset"],
                    **{k: v for k, v in self.conn_info.items() if k not in ["ssl_input"]},
                )
            except client.err.InternalError:
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                    "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
                    charset=config["connection.charset"],
                    **{
                        k: v
                        for k, v in self.conn_info.items()
                        if not (k == "ssl_input" or k == "ssl" and self.conn_info["ssl_input"] is None)
                    },
                )
        self._conn.autocommit(True)

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
        if isinstance(config.get(cache_key), str) and pathlib.Path(config[cache_key]).is_dir():
            for path in pathlib.Path(config[cache_key]).iterdir():
                if not path.is_dir():
                    path.unlink()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

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
        self._conn.ping(reconnect=False)

    @property
    def is_connected(self) -> bool:
        """
        Check if connected to the database server.

        Returns
        -------
        bool
            True if connected.
        """
        try:
            self.ping()
        except:
            return False
        return True

    @staticmethod
    def _execute_query(cursor, query, args, suppress_warnings):
        try:
            with warnings.catch_warnings():
                if suppress_warnings:
                    # suppress all warnings arising from underlying SQL library
                    warnings.simplefilter("ignore")
                cursor.execute(query, args)
        except client.err.Error as err:
            raise translate_query_error(err, query)

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
            hash_ = hashlib.md5((str(self._query_cache) + re.sub(r"`\$\w+`", "", query)).encode() + pack(args)).hexdigest()
            cache_path = pathlib.Path(config[cache_key]) / str(hash_)
            try:
                buffer = cache_path.read_bytes()
            except FileNotFoundError:
                pass  # proceed to query the database
            else:
                return EmulatedCursor(unpack(buffer))

        if reconnect is None:
            reconnect = config["database.reconnect"]
        logger.debug("Executing SQL:" + query[:query_log_max_length])
        cursor_class = client.cursors.DictCursor if as_dict else client.cursors.Cursor
        cursor = self._conn.cursor(cursor=cursor_class)
        try:
            self._execute_query(cursor, query, args, suppress_warnings)
        except errors.LostConnectionError:
            if not reconnect:
                raise
            logger.warning("Reconnecting to MySQL server.")
            self.connect()
            if self._in_transaction:
                self.cancel_transaction()
                raise errors.LostConnectionError("Connection was lost during a transaction.")
            logger.debug("Re-executing")
            cursor = self._conn.cursor(cursor=cursor_class)
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
        return self.query("SELECT user()").fetchone()[0]

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
        self.query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        self._in_transaction = True
        logger.debug("Transaction started")

    def cancel_transaction(self) -> None:
        """Cancel the current transaction and roll back all changes."""
        self.query("ROLLBACK")
        self._in_transaction = False
        logger.debug("Transaction cancelled. Rolling back ...")

    def commit_transaction(self) -> None:
        """Commit all changes and close the transaction."""
        self.query("COMMIT")
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
