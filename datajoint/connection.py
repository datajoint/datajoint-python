import pymysql
import pymysql.cursors
from core import camelCase
import re

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

    def makeClassName(self, dbName, tableName):
        """
        make a class name from the table name.
        """
        return  (self.packages[dbName] if dbName in self.packages else '$'+dbName) + '.' + camelCase(tableName)

    def loadDependencies(self, schema):
        ptrn = r"""
        FOREIGN\ KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[^\s]+)\s+           # table referenced
        \((?P<attr2>[`\w ,]+)\)                     # list of keys in the referenced table
        """

        for tblName in schema: # visit each table in schema
            cur = self.query('SHOW CREATE TABLE `{dbName}`.`{tblName}`'.format(
                dbName=schema.dbName, tblName=tblName),
                cursor = self.dict_cursor)

            tblDef = cur.fetchone()
            fullTblName = '`%s`.`%s`' % (schema.dbName, tblName)
            self.parents[fullTblName] = []
            self.referenced[fullTblName] = []
            m_fk = re.finditer(ptrn, tblDef['Create Table'], re.X) # find all foreign key statements

            for m in m_fk:
                assert m.group('attr1') == m.group('attr2'), 'Foreign keys must link identically named attributes'
                attrs = m.group('attr1')
                attrs = re.split(r',\s+', re.sub(r'`(.*?)`', r'\1', attrs)) # remove ` around attrs and split into list
                pk = schema.headers[tblName].primaryKey
                isPrimary = all([k in pk for k in attrs])
                ref = m.group('ref') # referenced table

                if not re.search(r'`\.`', ref): # if referencing other table in same schema
                    ref = '`%s`.%s' % (schema.dbName, ref) # convert to full-table name

                if isPrimary:
                    self.parents[fullTblName].append(ref)
                else:
                    self.referenced[fullTblName].append(ref)

                self.parents.setdefault(ref, [])
                self.referenced.setdefault(ref, [])

    def clearDependencies(self, schema=None):
        if schema is None:
            self.parents.clear()
            self.referenced.clear()
        else:
            tableKeys = ('`%s`.`%s`'%(schema.dbName, tblName) for tblName in schema)
            for key in tableKeys:
                if key in self.parents:
                    self.parents.pop(key)
                if key in self.referenced:
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

    def tableToClass(self, fullTableName, strict = False):
        m = re.match(r'^`(?P<dbName>.+)`.`(?P<tblName>[#~\w\d]+)`$', fullTableName)
        assert  m, 'Invalid table name %s' % fullTableName
        dbName = m.group('dbName')
        tblName = m.group('tblName')
        if dbName in self.packages:
            className = '%s.%s' % (self.packages[dbName], camelCase(tblName))
        elif strict:
            raise ValueError('Unknown package for "%s". Activate its schema first.' % dbName)
        else:
            className = fullTableName
        return className




#def camelCase(s):
#    def toUpper(matchobj):
#        return matchobj.group(0)[-1].upper()
#    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)

