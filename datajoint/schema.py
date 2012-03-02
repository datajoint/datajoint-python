import re, collections
from conn import conn as djconn

# table tiers are encoded by naming convention as follows:
tableTiers = {
    '':'manual',      # manual tables have no prefix
    '#':'lookup',     # lookup tables start with a #
    '_':'imported',   # imported tables start with _
    '__':'computed'   # computed tables start with __
}

# regular expression to match valid table names
tierRe = re.compile('^(|#|_|__)[a-z]\w+$')

def camelCase(s):
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)



class Schema(object):
    """
    dj.Schema objects link a python module with a database schema
    """
    conn = None
    package = None
    dbname = None


    def __init__(self, package, dbname, conn=None):
        if conn is None:
            conn = djconn()
        self.conn = conn
        self.package = package 
        self.dbname = dbname
        self.reload()


    def __repr__(self):
        str = 'dj.Schema "{package}" -> "{dbname}" at {host}:{port}\n ({nTables} tables)'.format(
            package=self.package, dbname=self.dbname, 
            nTables=len(self.tables), **self.conn.connInfo)
        return str 


    def makeClassName(self, tableName):
        return self.package + '.' + camelCase(tableName)


    def reload(self):
        """
        load table definitions and dependencies   
        """

        print 'Loading table info...'
        cur = self.conn.query('''
            SELECT table_name, table_comment
            FROM information_schema.tables WHERE table_schema="{schema}" 
            '''.format(schema=self.dbname))

        TableTuple = collections.namedtuple('TableTuple',
            ('name','comment','tier','attrs','level','parents','children'))

        self.tables = collections.OrderedDict()
        for s in cur.fetchall():
            if tierRe.match(s[0]):
                self.tables[self.makeClassName(s[0])] = TableTuple( 
                    name = s[0], 
                    comment = s[1].split('$')[0], 
                    tier = tableTiers[tierRe.match(s[0]).group(1)],
                    attrs = collections.OrderedDict(),
                    level = 0,
                    parents = [],
                    children = [])


        print 'Loading column info...'
        cur = self.conn.query('''
            SELECT table_name, column_name, (column_key="PRI") AS `iskey`,
                column_type, (is_nullable="YES") AS isnullable, 
                column_comment, column_default 
            FROM information_schema.columns 
            WHERE table_schema="{schema}"
            '''.format(schema=self.dbname))

        FieldTuple = collections.namedtuple('FieldTuple',
            ('isKey','type','isNullable','comment','default','isNumeric','isString','isBlob'))

        for s in cur.fetchall():
            if tierRe.match(s[0]):
                tup = FieldTuple(
                    isKey = s[2]!=0,
                    type = s[3],
                    isNullable = s[4]!=0,
                    comment = s[5],
                    default = s[6],
                    isNumeric = None != re.match('^((tiny|small|medium|big)?int|decimal|double|float)', s[3]),
                    isString = None != re.match('^((var)?char|enum|date|timestamp)', s[3]),
                    isBlob = None != re.match('^(tiny|medium|long)?blob', s[3])
                )
                # check for unsupported datatypes
                if not (tup.isNumeric or tup.isString or tup.isBlob):
                    raise TypeError('Unsupported DataJoint datatype ' + tup.type)
                self.tables[self.makeClassName(s[0])].attrs[s[1]] = tup


        print 'Loading table dependencies...'
        tableList = repr(tuple([str(t.name) for t in self.tables.values()]))
        cur.execute(''' 
            SELECT table_name, referenced_table_name, 
                min((table_schema, table_name, column_name) in (
                    SELECT table_schema, table_name, column_name 
                    FROM information_schema.columns WHERE column_key="PRI")) 
                AS parental
            FROM information_schema.key_column_usage 
            WHERE table_schema="{schema}" AND table_name in {tables}
            AND referenced_table_schema="{schema}" AND referenced_table_name in {tables} 
            GROUP BY table_name, referenced_table_name
        '''.format(schema=self.dbname, tables=tableList))

        for s in cur.fetchall():
            self.tables[self.makeClassName(s[0])].parents.append(self.makeClassName(s[1]))
            self.tables[self.makeClassName(s[1])].children.append(self.makeClassName(s[0]))

        # compute hierarchy levels 
        changed = True
        while changed:
            changed = False
            for (k,v) in self.tables.items():
                if v.parents:
                    newLevel = max([self.tables[p].level for p in v.parents])+1
                    if newLevel>1000:
                        raise(Exception('Found a cyclical dependency'))
                    if self.tables[k].level < newLevel:
                        changed = True
                        self.tables[k] = self.tables[k]._replace(level = newLevel)
