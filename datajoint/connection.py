from contextlib import contextmanager
import pymysql
from . import DataJointError
import logging
from . import config
from .erd import ERD

logger = logging.getLogger(__name__)


def conn_container():
    """
    creates a persistent connections for everyone to use
    """
    _connection = None  # persistent connection object used by dj.conn()

    def conn_function(host=None, user=None, passwd=None, init_fun=None, reset=False):
        """
        Manage a persistent connection object.
        This is one of several ways to configure and access a datajoint connection.
        Users may customize their own connection manager.

        Set reset=True to reset the persistent connection object with new connection parameters
        """
        nonlocal _connection
        if not _connection or reset:
            host = host if host is not None else config['database.host']
            user = user if user is not None else config['database.user']
            passwd = passwd if passwd is not None else config['database.password']

            if passwd is None: passwd = input("Please enter database password: ")

            init_fun = init_fun if init_fun is not None else config['connection.init_function']
            _connection = Connection(host, user, passwd, init_fun)
        return _connection

    return conn_function

# The function conn is used by others to obtain a connection object
conn = conn_container()


class Connection:
    """
    A dj.Connection object manages a connection to a database server.
    It also catalogues modules, schemas, tables, and their dependencies (foreign keys).

    Most of the parameters below should be set in the local configuration file.

    :param host: host name
    :param user: user name
    :param passwd: password
    :param init_fun: initialization function

    """

    def __init__(self, host, user, passwd, init_fun=None):
        self.erd = ERD()
        if ':' in host:
            host, port = host.split(':')
            port = int(port)
        else:
            port = config['database.port']
        self.conn_info = dict(host=host, port=port, user=user, passwd=passwd)
        self._conn = pymysql.connect(init_command=init_fun, **self.conn_info)
        if self.is_connected:
            logger.info("Connected " + user + '@' + host + ':' + str(port))
        else:
            raise DataJointError('Connection failed.')
        self._conn.autocommit(True)
        self._in_transaction = False

    def __del__(self):
        logger.info('Disconnecting {user}@{host}:{port}'.format(**self.conn_info))
        self._conn.close()

    def __eq__(self, other):
        return self.conn_info == other.conn_info

    @property
    def is_connected(self):
        """
        Returns true if the object is connected to the database server.
        """
        return self._conn.ping()

    def __repr__(self):
        connected = "connected" if self.is_connected else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(
            connected=connected, **self.conn_info)

    def __del__(self):
        logger.info('Disconnecting {user}@{host}:{port}'.format(**self.conn_info))
        self._conn.close()

    def query(self, query, args=(), as_dict=False):
        """
        Execute the specified query and return the tuple generator.

        If as_dict is set to True, the returned cursor objects returns
        query results as dictionary.
        """
        cursor = pymysql.cursors.DictCursor if as_dict else pymysql.cursors.Cursor
        cur = self._conn.cursor(cursor=cursor)

        # Log the query
        logger.debug("Executing SQL:" + query)
        cur.execute(query, args)
        return cur

    # ---------- transaction processing
    @property
    def in_transaction(self):
        self._in_transaction = self._in_transaction and self.is_connected
        return self._in_transaction

    def start_transaction(self):
        if self.in_transaction:
            raise DataJointError("Nested connections are not supported.")
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')
        self._in_transaction = True
        logger.info("Transaction started")

    def cancel_transaction(self):
        self.query('ROLLBACK')
        self._in_transaction = False
        logger.info("Transaction cancelled. Rolling back ...")

    def commit_transaction(self):
        self.query('COMMIT')
        self._in_transaction = False
        logger.info("Transaction committed and closed.")

    # -------- context manager for transactions
    @contextmanager
    def transaction(self):
        try:
            self.start_transaction()
            yield self
        except:
            self.cancel_transaction()
            raise
        else:
            self.commit_transaction()

