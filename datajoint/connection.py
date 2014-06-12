import re
import pymysql
import pymysql.cursors

class Connection:
    """
    dj.Connection objects link a python package with a database schema
    """
    tuple_cursor = pymysql.cursors.Cursor
    dict_cursor = pymysql.cursors.DictCursor

    def __init__(self, host, user, passwd, initFun):
        try:
            host, port = host.split(':')
            port = int(port)
        except ValueError:
            port = 3306
        self.connInfo = dict(host=host, port=port, user=user, passwd=passwd)
        self._conn = pymysql.connect(init_command=initFun, **self.connInfo)
        if self.isConnected:
            print "Connected", user+'@'+host+':'+str(port)
        self._conn.autocommit(True)
        self.parents = {} # map table names to their parent table names (primary foreign key)
        self.referenced = {} # map tales names to table names they reference (non-primary foreign key)
        self.packages = {}

    @property
    def isConnected(self):
        """
        Check the connection status for database
        """
        return self._conn.ping()


    def makeClassName(self, dbname, tableName):
        """
        make a class name from the table name.
        """
        try:
            ret = self.packages[dbname] + '.' + camelCase(tableName)
        except KeyError:
            ret = '$' + dbname + '.' + camelCase(tableName)
        return ret

    def loadDependencies(self, schema):
        import pdb; pdb.set_trace()  # XXX BREAKPOINT
        ptrn = r"""
        FOREIGN KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[`\w ,]+)\s\          # table referenced
        \((?P<attr2>[`\w ,]+)\)                    # list of keys in the referenced table
        """
        for tables in schema: # visit each table in schema
            s = self.query('SHOW CREATE TABLE `{dbName}`.`{tblName}`'.format(
                dbName=schema.dbName, tblName=schema.tblName),
                cursor = self.dict_cursor)
        print ptrn
        print s

    def clearDependencies(self, schema=None):
        if schema is None:
            self.parents.clear
            self.referenced.clear
        else:
            tableKeys = ('`%s`.`%s`'%(schema.dbName, tblName) for tblName in schema)
            for key in tableKeys:
                if key in self.parents:
                    self.parents.pop(key)
                if key in self.refernced:
                    self.referenced.pop(key)

    def addPackage(self, dbname, package):
        self.packages[dbname] = package

    def children(self, parentTable):
        """
        Return a list of tables for which parentTable is a parent (primary foreign key)
        """
        return [childTable for childTable, parents in self.parents.iteritems() if parentTable in parents]

    def referencing(self, referencedTable):
        """
        Return a list of tables that references referencedTable as non-primary foreign key
        """
        return [referencing for referencing, referenced in self.referenced.iteritems()
                if referencedTable in referenced]


    def __repr__(self):
        connected = "connected" if self._conn.ping() else "disconnected"
        return "DataJoint connection ({connected}) {user}@{host}:{port}".format(
            connected=connected, **self.connInfo)


    def __del__(self):
        print 'Disconnecting {user}@{host}:{port}'.format(**self.connInfo)
        self._conn.close()


    def query(self, query, args=(), cursor=pymysql.cursors.Cursor):
        """execute the specified query and return its cursor"""
        cur = self._conn.cursor(cursor=cursor)
        cur.execute(query, args)
        return cur


    def startTransaction(self):
        self.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')


    def cancelTransaction(self):
        self.query('ROLLBACK')


    def commitTransaction(self):
        self.query('COMMIT')


def camelCase(s):
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)

