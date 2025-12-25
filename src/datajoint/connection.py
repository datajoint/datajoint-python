"""
Database connection management for DataJoint.

This module contains the Connection class that manages the connection to the database,
and the ``conn`` function that provides access to a persistent connection in datajoint.
"""

from __future__ import annotations

import logging
import pathlib
import re
import warnings
from contextlib import contextmanager
from getpass import getpass
from typing import TYPE_CHECKING, Any

import pymysql as client

from . import errors
from .blob import pack, unpack
from .dependencies import Dependencies
from .hash import uuid_from_buffer
from .settings import config
from .version import __version__

if TYPE_CHECKING:
    from collections.abc import Generator


logger = logging.getLogger(__name__.split(".")[0])
query_log_max_length = 300


cache_key = "query_cache"  # the key to lookup the query_cache folder in dj.config


def translate_query_error(client_error: Exception, query: str) -> Exception:
    """
    Translate a database client error into the corresponding DataJoint exception.

    Args:
        client_error: The exception raised by the pymysql client interface.
        query: The SQL query that caused the error (with placeholders).

    Returns:
        An instance of the appropriate DataJointError subclass, or the original
        error if no specific translation is available.
    """
    logger.debug("type: {}, args: {}".format(type(client_error), client_error.args))

    err, *args = client_error.args

    # Loss of connection errors
    if err in (0, "(0, '')"):
        return errors.LostConnectionError("Server connection lost due to an interface error.", *args)
    if err == 2006:
        return errors.LostConnectionError("Connection timed out", *args)
    if err == 2013:
        return errors.LostConnectionError("Server connection lost", *args)
    # Access errors
    if err in (1044, 1142):
        return errors.AccessError("Insufficient privileges.", args[0], query)
    # Integrity errors
    if err == 1062:
        return errors.DuplicateError(*args)
    if err == 1217:  # MySQL 8 error code
        return errors.IntegrityError(*args)
    if err == 1451:
        return errors.IntegrityError(*args)
    if err == 1452:
        return errors.IntegrityError(*args)
    # Syntax errors
    if err == 1064:
        return errors.QuerySyntaxError(args[0], query)
    # Existence errors
    if err == 1146:
        return errors.MissingTableError(args[0], query)
    if err == 1364:
        return errors.MissingAttributeError(*args)
    if err == 1054:
        return errors.UnknownAttributeError(*args)
    # all the other errors are re-raised in original form
    return client_error


def conn(
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    *,
    init_fun: str | None = None,
    reset: bool = False,
    use_tls: bool | dict[str, Any] | None = None,
) -> Connection:
    """
    Return a persistent connection object to be shared by multiple modules.

    If the connection is not yet established or reset=True, a new connection is set up.
    If connection information is not provided, it is taken from config which takes the
    information from dj_local_conf.json. If the password is not specified in that file,
    datajoint prompts for the password.

    Args:
        host: Database hostname, optionally with port (host:port).
        user: MySQL username.
        password: MySQL password.
        init_fun: SQL initialization statement to execute on connection.
        reset: If True, close existing connection and create a new one.
        use_tls: TLS encryption option:
            - True: Require TLS
            - False: Require no TLS
            - None: TLS preferred (default)
            - dict: Manual SSL configuration options

    Returns:
        A shared Connection object.
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
    """
    A cursor-like object that wraps pre-fetched query results.

    Used when query caching is enabled to provide a cursor interface
    over cached data.
    """

    def __init__(self, data: list[tuple | dict]) -> None:
        self._data = data
        self._iter = iter(self._data)

    def __iter__(self) -> EmulatedCursor:
        return self

    def __next__(self) -> tuple | dict:
        return next(self._iter)

    def fetchall(self) -> list[tuple | dict]:
        """Return all remaining rows."""
        return self._data

    def fetchone(self) -> tuple | dict:
        """Return the next row."""
        return next(self._iter)

    @property
    def rowcount(self) -> int:
        """Return the total number of rows."""
        return len(self._data)


class Connection:
    """
    Manage a connection to a DataJoint database server.

    This class handles database connectivity, query execution, transaction management,
    and maintains references to schemas and their dependencies (foreign keys).

    Most parameters should be configured in the local configuration file rather than
    passed directly.

    Args:
        host: Hostname, may include port as hostname:port.
        user: Database username.
        password: Database password.
        port: Port number (overridden if included in host).
        init_fun: SQL initialization statement to execute on connection.
        use_tls: TLS encryption option (True/False/None/dict).

    Attributes:
        conn_info: Dictionary of connection parameters.
        schemas: Dictionary mapping database names to Schema objects.
        dependencies: Dependencies graph for foreign key relationships.
        connection_id: MySQL connection ID for this session.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int | None = None,
        init_fun: str | None = None,
        use_tls: bool | dict[str, Any] | None = None,
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Connection):
            return NotImplemented
        return self.conn_info == other.conn_info

    def __repr__(self) -> str:
        connected = "connected" if self.is_connected else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(connected=connected, **self.conn_info)

    def connect(self) -> None:
        """Establish connection to the database server."""
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
        Enable or disable query caching mode.

        When query_cache is not None, the connection switches into caching mode:
        1. Only SELECT queries are allowed
        2. Results are cached under dj.config['query_cache']
        3. The query_cache string differentiates cache states

        Args:
            query_cache: String to initialize the hash for query results,
                or None to disable caching.
        """
        self._query_cache = query_cache

    def purge_query_cache(self) -> None:
        """Remove all cached query results from the cache directory."""
        if isinstance(config.get(cache_key), str) and pathlib.Path(config[cache_key]).is_dir():
            for path in pathlib.Path(config[cache_key]).iterdir():
                if not path.is_dir():
                    path.unlink()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def register(self, schema: Any) -> None:
        """Register a schema with this connection."""
        self.schemas[schema.database] = schema
        self.dependencies.clear()

    def ping(self) -> None:
        """Ping the connection; raises an exception if disconnected."""
        self._conn.ping(reconnect=False)

    @property
    def is_connected(self) -> bool:
        """Return True if connected to the database server."""
        try:
            self.ping()
        except:
            return False
        return True

    @staticmethod
    def _execute_query(cursor: Any, query: str, args: tuple, suppress_warnings: bool) -> None:
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
    ) -> Any:
        """
        Execute an SQL query and return a cursor with the results.

        Args:
            query: The SQL query string.
            args: Parameters to substitute into the query.
            as_dict: If True, return results as dictionaries instead of tuples.
            suppress_warnings: If True, suppress warnings from the database driver.
            reconnect: If True, reconnect on connection loss. If None, use config setting.

        Returns:
            A cursor object (or EmulatedCursor when caching is enabled).

        Raises:
            DataJointError: If caching is enabled and query is not SELECT/SHOW.
            LostConnectionError: If connection is lost and reconnect fails.
        """
        # check cache first:
        use_query_cache = bool(self._query_cache)
        if use_query_cache and not re.match(r"\s*(SELECT|SHOW)", query):
            raise errors.DataJointError("Only SELECT queries are allowed when query caching is on.")
        if use_query_cache:
            if not config[cache_key]:
                raise errors.DataJointError(f"Provide filepath dj.config['{cache_key}'] when using query caching.")
            hash_ = uuid_from_buffer((str(self._query_cache) + re.sub(r"`\$\w+`", "", query)).encode() + pack(args))
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
        Return the current database user.

        Returns:
            The username and host in 'user@host' format.
        """
        return self.query("SELECT user()").fetchone()[0]

    # ---------- transaction processing
    @property
    def in_transaction(self) -> bool:
        """Return True if there is an open transaction."""
        self._in_transaction = self._in_transaction and self.is_connected
        return self._in_transaction

    def start_transaction(self) -> None:
        """
        Start a new database transaction.

        Raises:
            DataJointError: If already in a transaction (nesting not supported).
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
        """Commit all changes made during the transaction and close it."""
        self.query("COMMIT")
        self._in_transaction = False
        logger.debug("Transaction committed and closed.")

    # -------- context manager for transactions
    @property
    @contextmanager
    def transaction(self) -> Generator[Connection, None, None]:
        """
        Context manager for database transactions.

        Opens a transaction and commits it after the with block completes successfully.
        If an exception is raised, the transaction is rolled back automatically.

        Yields:
            This Connection object.

        Example:
            >>> import datajoint as dj
            >>> with dj.conn().transaction as conn:
            ...     # transaction is open here
            ...     table.insert(data)
            # transaction is committed on successful exit
        """
        try:
            self.start_transaction()
            yield self
        except:
            self.cancel_transaction()
            raise
        else:
            self.commit_transaction()
