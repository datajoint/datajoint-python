import pymysql
import re
from .utils import to_camel_case
from . import DataJointError
from .heading import Heading
from .settings import prefix_to_role, DEFAULT_PORT
import logging
from .erd import DBConnGraph
from . import config

logger = logging.getLogger(__name__)

# The following two regular expression are equivalent but one works in python
# and the other works in MySQL
table_name_regexp_sql = re.compile('^(#|_|__|~)?[a-z][a-z0-9_]*$')
table_name_regexp = re.compile('^(|#|_|__|~)[a-z][a-z0-9_]*$')  # MySQL does not accept this but MariaDB does


def conn_container():
    """
    creates a persistent connections for everyone to use
    """
    _connObj = None  # persistent connection object used by dj.conn()

    def conn_function(host=None, user=None, passwd=None, init_fun=None, reset=False):
        """
        Manage a persistent connection object.
        This is one of several ways to configure and access a datajoint connection.
        Users may customize their own connection manager.

        Set rest=True to reset the persistent connection object
        """
        nonlocal _connObj
        if not _connObj or reset:
            host = host if host is not None else config['database.host']
            user = user if user is not None else config['database.user']
            passwd = passwd if passwd is not None else config['database.password']

            if passwd is None: passwd = input("Please enter database password: ")

            init_fun = init_fun if init_fun is not None else config['connection.init_function']
            _connObj = Connection(host, user, passwd, init_fun)
        return _connObj
    return conn_function

# The function conn is used by others to obtain the package wide persistent connection object
conn = conn_container()


class Transaction(object):
    """
    Class that defines a transaction. Mainly for use in a with statement.

    :param conn: connection object that opens the transaction.
    """

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        assert self.conn.is_connected, "Connection is not connected"
        if self.conn.in_transaction:
            raise DataJointError("Connection object already has an open transaction")

        self.conn._in_transaction = True
        self.conn._start_transaction()
        return self

    @property
    def is_active(self):
        """
        :return: True if the transaction is active, i.e. the connection object is connected and
                the transaction flag in it is True
        """
        return self.conn.is_connected and self.conn.in_transaction


    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            self.conn._commit_transaction()
            self.conn._in_transaction = False
        else:
            self.conn._cancel_transaction()
            self.conn._in_transaction = False
            logger.debug("Transaction cancled because of an error.", exc_info=(exc_type, exc_val, exc_tb))



class Connection(object):
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
        if ':' in host:
            host, port = host.split(':')
            port = int(port)
        else:
            port = config['database.port']
        self.conn_info = dict(host=host, port=port, user=user, passwd=passwd)
        self._conn = pymysql.connect(init_command=init_fun, **self.conn_info)
        # TODO Do something if connection cannot be established
        if self.is_connected:
            print("Connected", user + '@' + host + ':' + str(port))
        self._conn.autocommit(True)

        self.db_to_mod = {}  # modules indexed by dbnames
        self.mod_to_db = {}  # database names indexed by modules
        self.table_names = {}  # tables names indexed by [dbname][class_name]
        self.headings = {}  # contains headings indexed by [dbname][table_name]
        self.tableInfo = {}  # table information indexed by [dbname][table_name]

        # dependencies from foreign keys
        self.parents = {}  # maps table names to their parent table names (primary foreign key)
        self.referenced = {}  # maps table names to table names they reference (non-primary foreign key
        self._graph = DBConnGraph(self)  # initialize an empty connection graph
        self._in_transaction = False

    def __eq__(self, other):
        return self.conn_info == other.conn_info

    @property
    def is_connected(self):
        """
        Returns true if the object is connected to the database server.
        """
        return self._conn.ping()

    def get_full_module_name(self, module):
        """
        Returns full module name of the module.

        :param module: module for which the name is requested.
        :return: full module name
        """
        return '.'.join(self.root_package, module)

    def bind(self, module, dbname):
        """
        Binds the `module` name to the database named `dbname`.
        Throws an error if `dbname` is already bound to another module.

        If the database `dbname` does not exist in the server, attempts
        to create the database and then bind the module.


        :param module: module name.
        :param dbname: database name. It should be a valid database identifier and not a match pattern.
        """

        if dbname in self.db_to_mod:
            raise DataJointError('Database `%s` is already bound to module `%s`'
                                 % (dbname, self.db_to_mod[dbname]))

        cur = self.query("SHOW DATABASES LIKE '{dbname}'".format(dbname=dbname))
        count = cur.rowcount

        if count == 1:
            # Database exists
            self.db_to_mod[dbname] = module
            self.mod_to_db[module] = dbname
        elif count == 0:
            # Database doesn't exist, attempt to create
            logger.info("Database `{dbname}` could not be found. "
                        "Attempting to create the database.".format(dbname=dbname))
            try:
                self.query("CREATE DATABASE `{dbname}`".format(dbname=dbname))
                logger.info('Created database `{dbname}`.'.format(dbname=dbname))
                self.db_to_mod[dbname] = module
                self.mod_to_db[module] = dbname
            except pymysql.OperationalError:
                raise DataJointError("Database named `{dbname}` was not defined, and"
                                     " an attempt to create has failed. Check"
                                     " permissions.".format(dbname=dbname))
        else:
            raise DataJointError("Database name {dbname} matched more than one "
                                 "existing databases. Database name should not be "
                                 "a pattern.".format(dbname=dbname))

    def load_headings(self, dbname=None, force=False):
        """
        Load table information including roles and list of attributes for all
        tables within dbname by examining respective table status.

        If dbname is not specified or None, will load headings for all
        databases that are bound to a module.

        By default, the heading is not loaded again if it already exists.
        Setting force=True will result in reloading of the heading even if one
        already exists.

        :param dbname=None: database name
        :param force=False: force reloading the heading
        """
        if dbname:
            self._load_headings(dbname, force)
            return

        for dbname in self.db_to_mod:
            self._load_headings(dbname, force)

    def _load_headings(self, dbname, force=False):
        """
        Load table information including roles and list of attributes for all
        tables within dbname by examining respective table status.

        By default, the heading is not loaded again if it already exists.
        Setting force=True will result in reloading of the heading even if one
        already exists.

        :param dbname: database name
        :param force: force reloading the heading
        """
        if dbname not in self.headings or force:
            logger.info('Loading table definitions from `{dbname}`...'.format(dbname=dbname))
            self.table_names[dbname] = {}
            self.headings[dbname] = {}
            self.tableInfo[dbname] = {}

            cur = self.query('SHOW TABLE STATUS FROM `{dbname}` WHERE name REGEXP "{sqlPtrn}"'.format(
                dbname=dbname, sqlPtrn=table_name_regexp_sql.pattern), as_dict=True)

            for info in cur:
                info = {k.lower(): v for k, v in info.items()}  # lowercase it
                table_name = info.pop('name')
                # look up role by table name prefix
                role = prefix_to_role[table_name_regexp.match(table_name).group(1)]
                class_name = to_camel_case(table_name)
                self.table_names[dbname][class_name] = table_name
                self.tableInfo[dbname][table_name] = dict(info, role=role)
                self.headings[dbname][table_name] = Heading.init_from_database(self, dbname, table_name)
            self.load_dependencies(dbname)

    def load_dependencies(self, dbname):  # TODO: Perhaps consider making this "private" by preceding with underscore?
        """
        Load dependencies (foreign keys) between tables by examining their
        respective CREATE TABLE statements.

        :param dbname: database name
        """

        foreign_key_regexp = re.compile(r"""
        FOREIGN KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[^\s]+)\s+             # table referenced
        \((?P<attr2>[`\w ,]+)\)                     # list of keys in the referenced table
        """, re.X)

        logger.info('Loading dependencies for `{dbname}`'.format(dbname=dbname))

        for tabName in self.tableInfo[dbname]:
            cur = self.query('SHOW CREATE TABLE `{dbname}`.`{tabName}`'.format(dbname=dbname, tabName=tabName),
                             as_dict=True)
            table_def = cur.fetchone()
            full_table_name = '`%s`.`%s`' % (dbname, tabName)
            self.parents[full_table_name] = []
            self.referenced[full_table_name] = []

            for m in foreign_key_regexp.finditer(table_def["Create Table"]):  # iterate through foreign key statements
                assert m.group('attr1') == m.group('attr2'), \
                    'Foreign keys must link identically named attributes'
                attrs = m.group('attr1')
                attrs = re.split(r',\s+', re.sub(r'`(.*?)`', r'\1', attrs))  # remove ` around attrs and split into list
                pk = self.headings[dbname][tabName].primary_key
                is_primary = all([k in pk for k in attrs])
                ref = m.group('ref')  # referenced table

                if not re.search(r'`\.`', ref):  # if referencing other table in same schema
                    ref = '`%s`.%s' % (dbname, ref)  # convert to full-table name

                (self.parents if is_primary else self.referenced)[full_table_name].append(ref)
                self.parents.setdefault(ref, [])
                self.referenced.setdefault(ref, [])

    def clear_dependencies(self, dbname=None):
        """
        Clears dependency mapping originating from `dbname`. If `dbname` is not
        specified, dependencies for all databases will be cleared.


        :param dbname: database name
        """
        if dbname is None:  # clear out all dependencies
            self.parents.clear()
            self.referenced.clear()
        else:
            table_keys = ('`%s`.`%s`' % (dbname, tblName) for tblName in self.tableInfo[dbname])
            for key in table_keys:
                if key in self.parents:
                    self.parents.pop(key)
                if key in self.referenced:
                    self.referenced.pop(key)

    def parents_of(self, child_table): #TODO: this function is not clear to me after reading the docu
        """
        Returns a list of tables that are parents for the childTable based on
        primary foreign keys.

        :param child_table: the child table
        """
        return self.parents.get(child_table, []).copy()

    def children_of(self, parent_table):#TODO: this function is not clear to me after reading the docu
        """
        Returns a list of tables for which parent_table is a parent (primary foreign key)

        :param parent_table: parent table
        """
        return [child_table for child_table, parents in self.parents.items() if parent_table in parents]

    def referenced_by(self, referencing_table):
        """
        Returns a list of tables that are referenced by non-primary foreign key
        by the referencing_table.

        :param referencing_table: referencing table
        """
        return self.referenced.get(referencing_table, []).copy()

    def referencing(self, referenced_table):
        """
        Returns a list of tables that references referencedTable as non-primary foreign key

        :param referenced_table: referenced table
        """
        return [referencing for referencing, referenced in self.referenced.items()
                if referenced_table in referenced]

    # TODO: Reimplement __str__
    def __str__(self):
        return self.__repr__()  # placeholder until more suitable __str__ is implemented

    def __repr__(self):
        connected = "connected" if self.is_connected else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(
            connected=connected, **self.conn_info)

    def __del__(self):
        logger.info('Disconnecting {user}@{host}:{port}'.format(**self.conn_info))
        self._conn.close()

    def erd(self, databases=None, tables=None, fill=True, reload=True):
        """
        Creates Entity Relation Diagram for the database or specified subset of
        tables.

        Set `fill` to False to only display specified tables. (By default
        connection tables are automatically included)
        """
        if reload:
            self.load_headings()  # load all tables and relations for bound databases

        self._graph.update_graph()  # update the graph

        graph = self._graph.copy_graph()
        if databases:
            graph = graph.restrict_by_modules(databases, fill)

        if tables:
            graph = graph.restrict_by_tables(tables, fill)

        graph.plot()

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

    def transaction(self):
        """
        Context manager to be used with python's with statement.

        :return: a :class:`Transaction` object

        :Example:

        >>> conn = dj.conn()
        >>> with conn.transaction() as tr:
                ... # do magic
        """
        return Transaction(self)

    @property
    def in_transaction(self):
        return self._in_transaction

    def _start_transaction(self):
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')
        logger.log(logging.INFO, "Transaction started")

    def _cancel_transaction(self):
        self.query('ROLLBACK')
        logger.log(logging.INFO, "Transaction cancelled. Rolling back ...")

    def _commit_transaction(self):
        self.query('COMMIT')
        logger.log(logging.INFO, "Transaction commited and closed.")
