import pymysql
import re
from .core import log, DataJointError, camelCase
import os
from .heading import Heading
from .base import prefixRole

# The following two regular expression are equivalent but one works in python
# and the other works in MySQL
tableNameRegExpSQL = re.compile('^(#|_|__|~)?[a-z][a-z0-9_]*$')
tableNameRegExp = re.compile('^(|#|_|__|~)[a-z][a-z0-9_]*$')    # MySQL does not accept this by MariaDB does

_connObj = None   # persitent connection object used by dj.conn()


def conn(host=None, user=None, passwd=None, initFun=None):
    """
    Manage a persistent connection object.
    This is one of several ways to configure and access a datajoint connection.
    Users may customize their own connection manager.
    """
    global _connObj
    if not _connObj:
        host = host or os.getenv('DJ_HOST') or input('Enter datajoint server address >> ')
        user = user or os.getenv('DJ_USER') or input('Enter datajoint user name >> ')
        # had trouble with getpass
        passwd = passwd or os.getenv('DJ_PASS') or input('Enter datajoint password >> ')
        initFun = initFun or os.getenv('DJ_INIT')
        _connObj = Connection(host, user, passwd, initFun)
    return _connObj


class Connection:
    """
    A dj.Connection object manages a connection to a database server.
    It also catalogues modules, schemas, tables, and their dependencies (foreign keys)
    """
    def __init__(self, host, user, passwd, initFun):
        try:
            host, port = host.split(':')
            port = int(port)
        except ValueError:
            port = 3306   # default MySQL port
        self.connInfo = dict(host=host, port=port, user=user, passwd=passwd)
        self._conn = pymysql.connect(init_command=initFun, **self.connInfo)
        if self.isConnected:
            print("Connected", user+'@'+host+':'+str(port))
        self._conn.autocommit(True)

        self.schemas    = {}  # database indexed by module names
        self.modules    = {}  # modules indexed by dbnames
        self.dbnames    = {}  # modules indexed by database names
        self.tableNames = {}  # tables names indexed by [dbname][ClassName]
        self.headings   = {}  # contains headings indexed by [dbname][table_name]
        self.tableInfo  = {}  # table information indexed by [dbname][table_name]

        # dependencies from foreign keys
        self.parents    = {}  # maps table names to their parent table names (primary foreign key)
        self.referenced = {}  # maps table names to table names they reference (non-primary foreign key

    @property
    def isConnected(self):
        return self._conn.ping()

    def bind(self, module, dbname):
        """
        binds module to dbname
        """
        if dbname in self.modules:
            raise DataJointError('Database `%s` is already bound to module `%s`'
                %(dbname,self.modules[dbname]))
        self.modules[dbname] = module
        self.dbnames[module] = dbname

    def loadHeadings(self, dbname, force=False):
        """
        Load table information including roles and list of attributes for all
        tables within dbname by examining respective TABLE STATUS
        """
        if not dbname in self.headings or force:
            log('Loading table definitions from `%s`...' % dbname)
            self.tableNames[dbname] = {}
            self.headings[dbname] = {}
            self.tableInfo[dbname] = {}

            cur = self.query('SHOW TABLE STATUS FROM `{dbname}` WHERE name REGEXP "{sqlPtrn}"'.format(
                dbname=dbname, sqlPtrn=tableNameRegExpSQL.pattern), asDict=True)

            for info in cur:
                info = {k.lower():v for k,v in info.items()}  # lowercase it
                tabName = info.pop('name')
                # look up role by table name prefix
                role = prefixRole[tableNameRegExp.match(tabName).group(1)]
                displayName = camelCase(tabName)
                self.tableNames[dbname][displayName] = tabName
                self.tableInfo[dbname][tabName] = dict(info,role=role)
                self.headings[dbname][tabName] = Heading.initFromDatabase(self,dbname,tabName)
            self.loadDependencies(dbname)

    def loadDependencies(self, dbname): # TODO: Perhaps consider making this "private" by preceding with underscore?
        """
        load dependencies (foreign keys) between tables by examnining their
        respective CREATE TABLE statements.
        """

        ptrn = r"""
        FOREIGN\ KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[^\s]+)\s+             # table referenced
        \((?P<attr2>[`\w ,]+)\)                     # list of keys in the referenced table
        """

        log('Loading dependices for %s...' % dbname)

        for tabName in self.tableInfo[dbname]:
            cur = self.query('SHOW CREATE TABLE `{dbname}`.`{tabName}`'.format(dbname = dbname, tabName = tabName), asDict=True)
            tblDef = cur.fetchone()
            fullTblName = '`%s`.`%s`' % (dbname, tabName)
            self.parents[fullTblName] = []
            self.referenced[fullTblName] = []

            for m in re.finditer(ptrn, tblDef['Create Table'], re.X):  # iterate through foreign key statements
                assert m.group('attr1') == m.group('attr2'), 'Foreign keys must link identically named attributes'
                attrs = m.group('attr1')
                attrs = re.split(r',\s+', re.sub(r'`(.*?)`', r'\1', attrs)) # remove ` around attrs and split into list
                pk = self.headings[dbname][tabName].primaryKey
                isPrimary = all([k in pk for k in attrs])
                ref = m.group('ref') # referenced table

                if not re.search(r'`\.`', ref): # if referencing other table in same schema
                    ref = '`%s`.%s' % (dbname, ref) # convert to full-table name

                if isPrimary:
                    self.parents[fullTblName].append(ref)
                else:
                    self.referenced[fullTblName].append(ref)

                self.parents.setdefault(ref, [])
                self.referenced.setdefault(ref, [])

    def clearDependencies(self, dbname=None):
        if dbname is None: # clear out all dependencies
            self.parents.clear()
            self.referenced.clear()
        else:
            tableKeys = ('`%s`.`%s`'%(dbname, tblName) for tblName in self.tableInfo[dbname])
            for key in tableKeys:
                if key in self.parents:
                    self.parents.pop(key)
                if key in self.referenced:
                    self.referenced.pop(key)

    def children(self, parentTable):
        """
        Return a list of tables for which parentTable is a parent (primary foreign key)
        """
        return [childTable for childTable, parents in self.parents.items() if parentTable in parents]

    def referencing(self, referencedTable):
        """
        Return a list of tables that references referencedTable as non-primary foreign key
        """
        return [referencing for referencing, referenced in self.referenced.items()
                if referencedTable in referenced]

    def __repr__(self):
        connected = "connected" if self._conn.ping() else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(
            connected=connected, **self.connInfo)

    def __del__(self):
        print('Disconnecting {user}@{host}:{port}'.format(**self.connInfo))
        self._conn.close()

    def query(self, query, args=(), asDict=False):
        """execute the specified query and return the tuple generator"""
        cursor = pymysql.cursors.DictCursor if asDict else pymysql.cursors.Cursor
        cur = self._conn.cursor(cursor=cursor)
        cur.execute(query, args)
        return cur

    def startTransaction(self):
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')

    def cancelTransaction(self):
        self.query('ROLLBACK')

    def commitTransaction(self):
        self.query('COMMIT')
