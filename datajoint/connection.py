"""
This module hosts the Connection class that manages the connection to the mysql database,
 and the `conn` function that provides access to a persistent connection in datajoint.
"""
import warnings
from contextlib import contextmanager
import pymysql as client
import logging
from getpass import getpass

from . import config
from . import DataJointError
from .dependencies import Dependencies
from .jobs import JobManager
from pymysql import err

logger = logging.getLogger(__name__)


def conn(host=None, user=None, password=None, init_fun=None, reset=False):
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
        conn.connection = Connection(host, user, password, init_fun)
    return conn.connection


class Connection:
    """
    A dj.Connection object manages a connection to a database server.
    It also catalogues modules, schemas, tables, and their dependencies (foreign keys).

    Most of the parameters below should be set in the local configuration file.

    :param host: host name
    :param user: user name
    :param password: password
    :param init_fun: connection initialization function (SQL)
    """

    def __init__(self, host, user, password, init_fun=None):
        if ':' in host:
            host, port = host.split(':')
            port = int(port)
        else:
            port = config['database.port']
        self.conn_info = dict(host=host, port=port, user=user, passwd=password)
        self.init_fun = init_fun
        print("Connecting {user}@{host}:{port}".format(**self.conn_info))
        self._conn = None
        self.connect()
        if self.is_connected:
            logger.info("Connected {user}@{host}:{port}".format(**self.conn_info))
            self.connection_id = self.query('SELECT connection_id()').fetchone()[0]
        else:
            raise DataJointError('Connection failed.')
        self._conn.autocommit(True)
        self._in_transaction = False
        self.jobs = JobManager(self)
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
        self._conn = client.connect(init_command=self.init_fun, **self.conn_info)

    def register(self, schema):
        self.schemas[schema.database] = schema

    @property
    def is_connected(self):
        """
        Returns true if the object is connected to the database server.
        """
        return self._conn.ping()

    def query(self, query, args=(), as_dict=False):
        """
        Execute the specified query and return the tuple generator (cursor).

        :param query: mysql query
        :param args: additional arguments for the client.cursor
        :param as_dict: If as_dict is set to True, the returned cursor objects returns
                        query results as dictionary.
        """

        cursor = client.cursors.DictCursor if as_dict else client.cursors.Cursor
        cur = self._conn.cursor(cursor=cursor)

        try:
            # Log the query
            logger.debug("Executing SQL:" + query[0:300])
            # suppress all warnings arising from underlying SQL library
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cur.execute(query, args)
        except err.OperationalError as e:
            if 'MySQL server has gone away' in str(e) and config['database.reconnect']:
                warnings.warn('''Mysql server has gone away.
                    Reconnected to the server. Data from transactions might be lost and referential constraints may
                    be violated. You can switch off this behavior by setting the 'database.reconnect' to False.
                    ''')
                self.connect()
                logger.debug("Re-executing SQL: " + query[0:300])
                cur.execute(query, args)
            else:
                raise
        except err.ProgrammingError as e:
            print('Error in query:')
            print(query)
            raise
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
