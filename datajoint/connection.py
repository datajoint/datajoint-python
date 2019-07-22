"""
This module contains the Connection class that manages the connection to the database,
 and the `conn` function that provides access to a persistent connection in datajoint.
"""
import warnings
from contextlib import contextmanager
import pymysql as client
import logging
from getpass import getpass
from pymysql import err

from .settings import config
from .errors import DataJointError, server_error_codes, is_connection_error
from .dependencies import Dependencies


logger = logging.getLogger(__name__)


def conn(host=None, user=None, password=None, init_fun=None, reset=False, use_tls=None):
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
    :param use_tls: TLS encryption option
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
        self.connect()
        if self.is_connected:
            logger.info("Connected {user}@{host}:{port}".format(**self.conn_info))
            self.connection_id = self.query('SELECT connection_id()').fetchone()[0]
        else:
            raise DataJointError('Connection failed.')
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
        """
        Connects to the database server.
        """
        ssl_input = self.conn_info.pop('ssl_input')
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', '.*deprecated.*')
            try:
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                            "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION",
                    charset=config['connection.charset'],
                    **self.conn_info)
            except err.InternalError:
                if ssl_input is None:
                    self.conn_info.pop('ssl')
                self._conn = client.connect(
                    init_command=self.init_fun,
                    sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
                            "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION",
                    charset=config['connection.charset'],
                    **self.conn_info)
        self.conn_info['ssl_input'] = ssl_input
        self._conn.autocommit(True)

    def close(self):
        self._conn.close()

    def register(self, schema):
        self.schemas[schema.database] = schema

    def ping(self):
        """
        Pings the connection. Raises an exception if the connection is closed.
        """
        self._conn.ping(reconnect=False)

    @property
    def is_connected(self):
        """
        Returns true if the object is connected to the database server.
        """
        try:
            self.ping()
        except:
            return False
        return True

    def query(self, query, args=(), as_dict=False, suppress_warnings=True, reconnect=None):
        """
        Execute the specified query and return the tuple generator (cursor).

        :param query: mysql query
        :param args: additional arguments for the client.cursor
        :param as_dict: If as_dict is set to True, the returned cursor objects returns
                        query results as dictionary.
        :param suppress_warnings: If True, suppress all warnings arising from underlying query library
        """
        if reconnect is None:
            reconnect = config['database.reconnect']

        cursor = client.cursors.DictCursor if as_dict else client.cursors.Cursor
        cur = self._conn.cursor(cursor=cursor)

        logger.debug("Executing SQL:" + query[0:300])
        try:
            with warnings.catch_warnings():
                if suppress_warnings:
                    # suppress all warnings arising from underlying SQL library
                    warnings.simplefilter("ignore")
                cur.execute(query, args)
        except (err.InterfaceError, err.OperationalError) as e:
            if is_connection_error(e) and reconnect:
                warnings.warn("Mysql server has gone away. Reconnecting to the server.")
                self.connect()
                if self._in_transaction:
                    self.cancel_transaction()
                    raise DataJointError("Connection was lost during a transaction.") from None
                else:
                    logger.debug("Re-executing SQL")
                    cur = self.query(query, args=args, as_dict=as_dict, suppress_warnings=suppress_warnings, reconnect=False)
            else:
                logger.debug("Caught InterfaceError/OperationalError.")
                raise
        except err.ProgrammingError as e:
            if e.args[0] == server_error_codes['parse error']:
                raise DataJointError("\n".join((
                    "Error in query:", query,
                    "Please check spelling, syntax, and existence of tables and attributes.",
                    "When restricting a relation by a condition in a string, enclose attributes in backquotes."
                ))) from None
        return cur

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

        :raise DataJointError: if there is an ongoing transaction.
        """
        if self.in_transaction:
            raise DataJointError("Nested connections are not supported.")
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
