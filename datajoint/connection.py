"""
This module contains the Connection class that manages the connection to the database,
 and the `conn` function that provides access to a persistent connection in datajoint.
"""
import warnings
from contextlib import contextmanager
import pymysql as client
import logging
from getpass import getpass
import re
import pathlib

from .settings import config
from . import errors
from .dependencies import Dependencies
from .blob import pack, unpack
from .hash import uuid_from_buffer

logger = logging.getLogger(__name__)
query_log_max_length = 300


def translate_query_error(client_error, query):
    """
    Take client error and original query and return the corresponding DataJoint exception.
    :param client_error: the exception raised by the client interface
    :param query: sql query with placeholders
    :return: an instance of the corresponding subclass of datajoint.errors.DataJointError
    """
    logger.debug('type: {}, args: {}'.format(type(client_error), client_error.args))

    err, *args = client_error.args

    # Loss of connection errors
    if err in (0, "(0, '')"):
        return errors.LostConnectionError('Server connection lost due to an interface error.', *args)
    if err == 2006:
        return errors.LostConnectionError("Connection timed out", *args)
    if err == 2013:
        return errors.LostConnectionError("Server connection lost", *args)
    # Access errors
    if err in (1044, 1142):
        return errors.AccessError('Insufficient privileges.', args[0],  query)
    # Integrity errors
    if err == 1062:
        return errors.DuplicateError(*args)
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


def conn(host=None, user=None, password=None, *, init_fun=None, reset=False, use_tls=None):
    """
    Returns a persistent connection object to be shared by multiple modules.
    If the connection is not yet established or reset=True, a new connection is set up.
    If connection information is not provided, it is taken from config which takes the
    information from dj_local_conf.json. If the password is not specified in that file
    datajoint prompts for the password.

    :param host: hostname
    :param user: mysql user
    :param password: mysql password
    :param init_fun: initialization function
    :param reset: whether the connection should be reset or not
    :param use_tls: TLS encryption option. Valid options are: True (required),
                    False (required no TLS), None (TLS prefered, default),
                    dict (Manually specify values per
                    https://dev.mysql.com/doc/refman/5.7/en/connection-options.html
                        #encrypted-connection-options).
    """
    if not hasattr(conn, 'connection') or reset:
        host = host if host is not None else config['database.host']
        user = user if user is not None else config['database.user']
        password = password if password is not None else config['database.password']
        if user is None:  # pragma: no cover
            user = input("Please enter DataJoint username: ")
        if password is None:  # pragma: no cover
            password = getpass(prompt="Please enter DataJoint password: ")
        init_fun = init_fun if init_fun is not None else config['connection.init_function']
        use_tls = use_tls if use_tls is not None else config['database.use_tls']
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


class Connection:
    """
    A dj.Connection object manages a connection to a database server.
    It also catalogues modules, schemas, tables, and their dependencies (foreign keys).

    Most of the parameters below should be set in the local configuration file.

    :param host: host name, may include port number as hostname:port, in which case it overrides the value in port
    :param user: user name
    :param password: password
    :param port: port number
    :param init_fun: connection initialization function (SQL)
    :param use_tls: TLS encryption option
    """
    def __init__(self, host, user, password, port=None, init_fun=None, use_tls=None):
        if ':' in host:
            # the port in the hostname overrides the port argument
            host, port = host.split(':')
            port = int(port)
        elif port is None:
            port = config['database.port']
        self.conn_info = dict(host=host, port=port, user=user, passwd=password)
        if use_tls is not False:
            self.conn_info['ssl'] = use_tls if isinstance(use_tls, dict) else {'ssl': {}}
        self.conn_info['ssl_input'] = use_tls
        self.init_fun = init_fun
        print("Connecting {user}@{host}:{port}".format(**self.conn_info))
        self._conn = None
        self._query_cache = None
        self.connect()
        if self.is_connected:
            logger.info("Connected {user}@{host}:{port}".format(**self.conn_info))
            self.connection_id = self.query('SELECT connection_id()').fetchone()[0]
        else:
            raise errors.LostConnectionError('Connection failed.')
        self._in_transaction = False
        self.schemas = dict()
        self.dependencies = Dependencies(self)

    def __eq__(self, other):
        return self.conn_info == other.conn_info

    def __repr__(self):
        connected = "connected" if self.is_connected else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(
            connected=connected, **self.conn_info)

    def connect(self):
        """ Connect to the database server."""
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', '.*deprecated.*')
            try:
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                             "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION",
                    charset=config['connection.charset'],
                    **{k: v for k, v in self.conn_info.items()
                       if k != 'ssl_input'})
            except client.err.InternalError:
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                             "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION",
                    charset=config['connection.charset'],
                    **{k: v for k, v in self.conn_info.items()
                       if not(k == 'ssl_input' or
                              k == 'ssl' and self.conn_info['ssl_input'] is None)})
        self._conn.autocommit(True)

    def set_query_cache(self, query_cache):
        """
        When query_cache is not None, the connection switches into the query caching mode, which entails:
        1. Only SELECT queries are allowed.
        2. The results of queries are cached under the path indicated by dj.config['query_cache']
        3. query_cache is a string that differentiates different cache states.
        :param query_cache: a string to initialize the hash for query results
        """
        self._query_cache = query_cache

    def close(self):
        self._conn.close()

    def register(self, schema):
        self.schemas[schema.database] = schema
        self.dependencies.clear()

    def ping(self):
        """ Ping the connection or raises an exception if the connection is closed. """
        self._conn.ping(reconnect=False)

    @property
    def is_connected(self):
        """ Return true if the object is connected to the database server. """
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
            raise translate_query_error(err, query) from None

    def query(self, query, args=(), *, as_dict=False, suppress_warnings=True, reconnect=None):
        """
        Execute the specified query and return the tuple generator (cursor).
        :param query: SQL query
        :param args: additional arguments for the client.cursor
        :param as_dict: If as_dict is set to True, the returned cursor objects returns
                        query results as dictionary.
        :param suppress_warnings: If True, suppress all warnings arising from underlying query library
        :param reconnect: when None, get from config, when True, attempt to reconnect if disconnected
        """
        # check cache first:
        use_query_cache = bool(self._query_cache)
        if use_query_cache and not re.match(r"\s*(SELECT|SHOW)", query):
            raise errors.DataJointError("Only SELECT query are allowed when query caching is on.")
        if use_query_cache:
            if not config['query_cache']:
                raise errors.DataJointError("Provide filepath dj.config['query_cache'] when using query caching.")
            hash_ = uuid_from_buffer((str(self._query_cache) + re.sub(r'`\$\w+`', '', query)).encode() + pack(args))
            cache_path = pathlib.Path(config['query_cache']) / str(hash_)
            try:
                buffer = cache_path.read_bytes()
            except FileNotFoundError:
                pass   # proceed to the normal query
            else:
                return EmulatedCursor(unpack(buffer))

        if reconnect is None:
            reconnect = config['database.reconnect']
        logger.debug("Executing SQL:" + query[:query_log_max_length])
        cursor_class = client.cursors.DictCursor if as_dict else client.cursors.Cursor
        cursor = self._conn.cursor(cursor=cursor_class)
        try:
            self._execute_query(cursor, query, args, suppress_warnings)
        except errors.LostConnectionError:
            if not reconnect:
                raise
            warnings.warn("MySQL server has gone away. Reconnecting to the server.")
            self.connect()
            if self._in_transaction:
                self.cancel_transaction()
                raise errors.LostConnectionError("Connection was lost during a transaction.") from None
            logger.debug("Re-executing")
            cursor = self._conn.cursor(cursor=cursor_class)
            self._execute_query(cursor, query, args, suppress_warnings)

        if use_query_cache:
            data = cursor.fetchall()
            cache_path.write_bytes(pack(data))
            return EmulatedCursor(data)

        return cursor

    def get_user(self):
        """
        :return: the user name and host name provided by the client to the server.
        """
        return self.query('SELECT user()').fetchone()[0]

    # ---------- transaction processing
    @property
    def in_transaction(self):
        """
        :return: True if there is an open transaction.
        """
        self._in_transaction = self._in_transaction and self.is_connected
        return self._in_transaction

    def start_transaction(self):
        """
        Starts a transaction error.
        """
        if self.in_transaction:
            raise errors.DataJointError("Nested connections are not supported.")
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')
        self._in_transaction = True
        logger.info("Transaction started")

    def cancel_transaction(self):
        """
        Cancels the current transaction and rolls back all changes made during the transaction.
        """
        self.query('ROLLBACK')
        self._in_transaction = False
        logger.info("Transaction cancelled. Rolling back ...")

    def commit_transaction(self):
        """
        Commit all changes made during the transaction and close it.

        """
        self.query('COMMIT')
        self._in_transaction = False
        logger.info("Transaction committed and closed.")

    # -------- context manager for transactions
    @property
    @contextmanager
    def transaction(self):
        """
        Context manager for transactions. Opens an transaction and closes it after the with statement.
        If an error is caught during the transaction, the commits are automatically rolled back.
        All errors are raised again.

        Example:
        >>> import datajoint as dj
        >>> with dj.conn().transaction as conn:
        >>>     # transaction is open here
        """
        try:
            self.start_transaction()
            yield self
        except:
            self.cancel_transaction()
            raise
        else:
            self.commit_transaction()
