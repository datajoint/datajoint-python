import pymysql
import re
from .core import DataJointError, to_camel_case
import os
from .heading import Heading
from .base import prefix_to_role
import logging
import networkx as nx
from networkx import pygraphviz_layout
import matplotlib.pyplot as plt
from .erd import DBConnGraph


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

    def conn(host=None, user=None, passwd=None, initFun=None, reset=False):
        """
        Manage a persistent connection object.
        This is one of several ways to configure and access a datajoint connection.
        Users may customize their own connection manager.

        Set rest=True to reset the persistent connection object
        """
        nonlocal _connObj
        if not _connObj or reset:
            host = host or os.getenv('DJ_HOST') #or input('Enter datajoint server address >> ')
            user = user or os.getenv('DJ_USER') #or input('Enter datajoint user name >> ')
            # had trouble with getpass
            print('passwd',passwd)
            print('DJ_PASS', os.getenv('DJ_PASS'))
            passwd = ''
            #passwd = passwd or os.getenv('DJ_PASS') #or input('Enter datajoint password >> ')
            initFun = initFun or os.getenv('DJ_INIT')
            _connObj = Connection(host, user, passwd, initFun)
        return _connObj
    return conn


# The function conn is used by others to obtain the package wide persistent connection object
conn = conn_container()

default_port = 3306


class Connection:
    """
    A dj.Connection object manages a connection to a database server.
    It also catalogues modules, schemas, tables, and their dependencies (foreign keys)
    """

    def __init__(self, host, user, passwd, initFun=None):
        try:
            host, port = host.split(':')
            port = int(port)
        except ValueError:
            port = default_port
        self.conn_info = dict(host=host, port=port, user=user, passwd=passwd)
        self._conn = pymysql.connect(init_command=initFun, **self.conn_info)
        # TODO Do something if connection cannot be established
        if self.is_connected:
            print("Connected", user + '@' + host + ':' + str(port))
        self._conn.autocommit(True)

        self.mod_to_db2 = {}  # database indexed by module names
        self.db_to_mod = {}  # modules indexed by dbnames
        self.mod_to_db = {}  # database names indexed by modules
        self.table_names = {}  # tables names indexed by [dbname][class_name]
        self.headings = {}  # contains headings indexed by [dbname][table_name]
        self.tableInfo = {}  # table information indexed by [dbname][table_name]

        # dependencies from foreign keys
        self.parents = {}  # maps table names to their parent table names (primary foreign key)
        self.referenced = {}  # maps table names to table names they reference (non-primary foreign key
        self._graph = DBConnGraph(self)  # initialize an empty connection graph

    def __eq__(self, other):
        """
        true if the connection host, port, and user name are the same
        """
        return self.conn_info == other.conn_info

    def is_same(self, host, user):
        """
        true if the connection host and user name are the same
        """
        if host is None:
            host = self.conn_info['host']
            port = self.conn_info['port']
        else:
            try:
                host, port = host.split(':')
                port = int(port)
            except ValueError:
                port = default_port

        if user is None:
            user = self.conn_info['user']

        return self.conn_info['host'] == host and \
               self.conn_info['port'] == port and \
               self.conn_info['user'] == user


    @property
    def is_connected(self):
        return self._conn.ping()

    def get_full_module_name(self, module):
        return '.'.join(self.root_package, module)

    def bind(self, module, dbname):
        """
        Binds the `module` name to the database named `dbname`.
        Throws an error if `dbname` is already bound to another module.

        If the database `dbname` does not exist in the server, attempts
        to create the database and then bind the module.

        `dbname` should be a valid database identifier and not a match pattern.
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
            logger.warning("Database `{dbname}` could not be found. "
                           "Attempting to create the database.".format(dbname=dbname))
            try:
                cur = self.query("CREATE DATABASE `{dbname}`".format(dbname=dbname))
                logger.warning('Created database `{dbname}`.'.format(dbname=dbname))
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
        tables within dbname by examining respective TABLE STATUS

        If dbname is not specified or None, will load headings for all
        databases that are bound to a module.

        By default, the heading is NOT loaded again if it already exists.
        Setting force=True will result in reloading of the heading even if one
        already exists.
        """
        if dbname:
            self._load_headings(dbname, force)
            return

        for dbname in self.db_to_mod:
            self._load_headings(dbname, force)

    def _load_headings(self, dbname, force=False):
        """
        Load table information including roles and list of attributes for all
        tables within dbname by examining respective TABLE STATUS

        By default, the heading is NOT loaded again if it already exists.
        Setting force=True will result in reloading of the heading even if one
        already exists.
        """
        if not dbname in self.headings or force:
            logger.info('Loading table definitions from `{dbname}`...'.format(dbname=dbname))
            self.table_names[dbname] = {}
            self.headings[dbname] = {}
            self.tableInfo[dbname] = {}

            cur = self.query('SHOW TABLE STATUS FROM `{dbname}` WHERE name REGEXP "{sqlPtrn}"'.format(
                dbname=dbname, sqlPtrn=table_name_regexp_sql.pattern), asDict=True)

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
        load dependencies (foreign keys) between tables by examnining their
        respective CREATE TABLE statements.
        """

        ptrn = r"""
        FOREIGN\ KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[^\s]+)\s+             # table referenced
        \((?P<attr2>[`\w ,]+)\)                     # list of keys in the referenced table
        """

        logger.info('Loading dependencies for `{dbname}`'.format(dbname=dbname))

        for tabName in self.tableInfo[dbname]:
            cur = self.query('SHOW CREATE TABLE `{dbname}`.`{tabName}`'.format(dbname=dbname, tabName=tabName),
                             asDict=True)
            table_def = cur.fetchone()
            full_table_name = '`%s`.`%s`' % (dbname, tabName)
            self.parents[full_table_name] = []
            self.referenced[full_table_name] = []

            for m in re.finditer(ptrn, table_def["Create Table"], re.X):  # iterate through foreign key statements
                assert m.group('attr1') == m.group('attr2'), 'Foreign keys must link identically named attributes'
                attrs = m.group('attr1')
                attrs = re.split(r',\s+', re.sub(r'`(.*?)`', r'\1', attrs))  # remove ` around attrs and split into list
                pk = self.headings[dbname][tabName].primary_key
                is_primary = all([k in pk for k in attrs])
                ref = m.group('ref')  # referenced table

                if not re.search(r'`\.`', ref):  # if referencing other table in same schema
                    ref = '`%s`.%s' % (dbname, ref)  # convert to full-table name

                if is_primary:
                    self.parents[full_table_name].append(ref)
                else:
                    self.referenced[full_table_name].append(ref)

                self.parents.setdefault(ref, [])
                self.referenced.setdefault(ref, [])

    def clear_dependencies(self, dbname=None):
        """
        Clears dependency mapping originating from `dbname`. If `dbname` is not
        specified, dependencies for all databases will be cleared.
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

    def parents_of(self, child_table):
        """
        Returns a list of tables that are parents for the childTable based on
        primary foreign keys.
        """
        return self.parents.get(child_table, []).copy()

    def children_of(self, parent_table):
        """
        Returnis a list of tables for which parentTable is a parent (primary foreign key)
        """
        return [child_table for child_table, parents in self.parents.items() if parent_table in parents]

    def referenced_by(self, referencing_table):
        """
        Returns a list of tables that are referenced by non-primary foreign key
        by the referencingTable.
        """
        return self.referenced.get(referencing_table, []).copy()

    def referencing(self, referenced_table):
        """
        Returns a list of tables that references referencedTable as non-primary foreign key
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

    def query(self, query, args=(), asDict=False):
        """
        Execute the specified query and return the tuple generator.

        If asDict is set to True, the returned cursor objects returns
        query results as dictionary.
        """
        cursor = pymysql.cursors.DictCursor if asDict else pymysql.cursors.Cursor
        cur = self._conn.cursor(cursor=cursor)

        # Log the query
        logger.debug("Executing SQL:" + query)
        cur.execute(query, args)
        return cur

    def start_transaction(self):
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')

    def cancel_transaction(self):
        self.query('ROLLBACK')

    def commit_transaction(self):
        self.query('COMMIT')
