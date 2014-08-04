import pymysql
from core import camelCase, settings
from heading import Heading
import re


tableTiers = {
    '': 'manual',      # manual tables have no prefix
    '#': 'lookup',     # lookup tables start with a #
    '_': 'imported',   # imported tables start with _
    '__': 'computed',  # computed tables start with __
    '~': 'job'         # job tables start with ~
}

tableNameRegExp = re.compile(r'^(|#|_|__|~)[a-z][a-z0-9_]*$')


class Connection:
    """
    a dj.Connection manages a connection to a database server.
    It also contains headers 
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
        self.parents = {} # maps table names to their parent table names (primary foreign key)
        self.referenced = {} # maps table names to table names they reference (non-primary foreign key)
        self.schemas = {}  # 

    @property
    def isConnected(self):
        return self._conn.ping()
        
        
    def activateSchema(self, module, dbName):
        """
        loads or reloads a schema and associates it with a module
        The module may define classes for existing or new tables.
        """
        self.schemas[dbName] = module;        
        if settings['verbose']: 
            print('Loading table definitions from %s...' % dbName)
    
        cur = self.conn.query('SHOW TABLE STATUS FROM `{dbName}` WHERE name REGEXP "{sqlPtrn}"'.format(
            dbName=self.dbName, sqlPtrn = tableNameRegExp.pattern),  asDict=True)
        tableInfo = cur.fetchall()
    
        # fields to lowercase
        tableInfo = [{k.lower():v for k,v in info.iteritems()} for info in tableInfo]
        
        # rename fields
        for info in tableInfo:
            info['tier'] = tableTiers[tableNameRegExp.match(info['name']).group(1)] # lookup tier for the table based on tableName
            self._tableNames['%s.%s'%(self.package, camelCase(info['name']))] = info['name']
            self._headers[info['name']] = Heading.initFromDatabase(self, info)
    
        if settings['verbose']:
            print('Loading dependices...')
        self.conn.loadDependencies(self)
            
        


    def loadDependencies(self, schema):
        """
        load dependencies (foreign keys) between tables by examnining their 
        respective CREATE TABLE statements.
        """

        ptrn = r"""
        FOREIGN\ KEY\s+\((?P<attr1>[`\w ,]+)\)\s+   # list of keys in this table
        REFERENCES\s+(?P<ref>[^\s]+)\s+             # table referenced
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

            for m in re.finditer(ptrn, tblDef['Create Table'], re.X):  # iterate through foreign key statements
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
        print('Disconnecting {user}@{host}:{port}'.format(**self.connInfo))
        self._conn.close()

    def query(self, query, args=(), asDict=False):
        """execute the specified query and return its cursor"""
        cursor = pymysql.cursors.DictCursor if asDict else pymysql.cursors.Cursor;
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
        