import re
from conn import conn as djconn
from core import DataJointError
import networkx as nx
import matplotlib.pyplot as plt
from copy import deepcopy
import core
from core import camelCase

# table tiers are encoded by naming convention as follows:
tableTiers = {
    '': 'manual',      # manual tables have no prefix
    '#': 'lookup',     # lookup tables start with a #
    '_': 'imported',   # imported tables start with _
    '__': 'computed',  # computed tables start with __
    '~': 'job'         # job tables start with ~
}

# regular expression to match valid table names
sqlPtrn = r'^(#|_|__|~)?[a-z][a-z0-9]*$'
tierRe = re.compile(r'^(|#|_|__|~)[a-z]\w+$')


# HeaderEntry = collections.namedtuple('HeaderEntry',
# ('isKey','type','isNullable','comment','default','isNumeric','isString','isBlob','alias'))

class Header(object):
    """
    Package-private class for handling table header information
    """
    @property
    def names(self):
        return [atr['name'] for atr in self.attrs]

    @property
    def primaryKey(self):
        return [atr['name'] for atr in self.attrs if atr['isKey']]

    @property
    def dependentFields(self):
        return [atr['name'] for atr in self.attrs if not atr['isKey']]

    @property
    def blobNames(self):
        return [atr['name'] for atr in self.attrs if atr['isBlob']]

    @property
    def notBlobs(self):
        return [atr['name'] for atr in self.attrs if not atr['isBlob']]

    @property
    def hasAliases(self):
        return any((bool(atr['alias']) for atr in self.attrs))

    @property
    def count(self):
        return len(self.attrs)

    @property
    def byName(self, name):
        for attr in self.attrs:
            if attr['name'] == name:
                return attr
        raise KeyError('Field with name %s not found' % name)

    def __init__(self, info, attrs):
        self.info = info
        self.attrs = attrs

    def derive(self):
        return Header(None, deepcopy(self.attrs))

    # Class methods
    @classmethod
    def initFromDatabase(cls, schema, tableInfo):
        cur = schema.conn.query(
            """
            SHOW FULL COLUMNS FROM `{tblName}` IN `{dbName}`
            """.format(tblName=tableInfo['name'], dbName=schema.dbName),
            cursor=schema.conn.dict_cursor)

        attrs = cur.fetchall()

        renameMap = {
            'Field': 'name',
            'Type': 'type',
            'Null': 'isNullable',
            'Default': 'default',
            'Key': 'isKey',
            'Comment': 'comment'}

        dropFields = ['Privileges', 'Collation']

        # rename fields using renameMap and drop unwanted fields
        attrs = [{renameMap[k] if k in renameMap else k: v
                  for k, v in x.iteritems() if k not in dropFields}
                 for x in attrs]

        for attr in attrs:
            attr['isNullable'] = attr['isNullable'] == 'YES'
            attr['isKey'] = attr['isKey'] == 'PRI'
            attr['isAutoincrement'] = bool(re.search(r'auto_increment', attr['Extra'], flags=re.IGNORECASE))
            attr['isNumeric'] = bool(re.match(r'(tiny|small|medium|big)?int|decimal|double|float', attr['type']))
            attr['isString'] = bool(re.match(r'(var)?char|enum|date|time|timestamp', attr['type']))
            attr['isBlob'] = bool(re.match(r'(tiny|medium|long)?blob', attr['type']))

            # strip field lengths off of integer types
            attr['type'] = re.sub(r'((tiny|small|medium|big)?int)\(\d+\)', r'\1', attr['type'])
            attr['alias'] = ''
            if not (attr['isNumeric'] or attr['isString'] or attr['isBlob']):
                raise DataJointError('Unsupported field type {field} in `{dbName}`.`{tblName}`'.format(
                    field=attr['type'], dbName=schema.dbName, tblName=tableInfo['name']))
            attr.pop('Extra')

        return cls(tableInfo, attrs)

    def pro(self, attrs):
        """
        project header onto a list of attributes.
        Alway include primary keys.
        """
        if '*' in attrs:
            ret = Header(self)
        else:
            ret = Header({k:v for k,v in self.items() if k in attrs or v.isKey})
        # TODO: add computed and renamed attributes
        return ret


class Schema(object):
    """
    datajoint.Schema objects link a python module (package) with a database schema
    """
    #conn = None
    #package = None
    #dbname = None

    @property
    def headers(self):
        self.reload() # load headers if necessary
        return self._headers

    @property
    def tableNames(self):
        self.reload() # load table names if necessary
        return self._tableNames

    def __init__(self, package, dbName, conn=None):
        if conn is None:
            conn = djconn() # open up new connection
        self.conn = conn
        conn.packages[dbName] = package # register this schema
        self.package = package
        self.dbName = dbName
        self._loaded = False # indicates loading status
        self._headers = {} # header object indexed by table name
        self._tableNames = {} # mapping from full class name to table name

    def __repr__(self):
        ret = 'datajoint.Schema "{package}" -> "{dbName}" at {host}:{port}\n{tableList}\n({nTables} tables)'.format(
            package=self.package,
            dbName=self.dbName,
            tableList='\n'.join(self.tableNames.keys()),
            nTables=len(self.tableNames), **self.conn.connInfo)
        return ret

    def reload(self, force=False):
        """
        load table definitions and dependencies
        """
        if self._loaded and not force:
            return # nothing to do here

        # Cleanup beofre reloading
        self._loaded = True
        self.conn.clearDependencies(self)
        self._headers.clear()
        self._tableNames.clear()

        if core.VERBOSE: #TODO Come up with a better place to keep package wide config variables
            print 'Loading table definitions from %s...' % self.dbName

        cur = self.conn.query('SHOW TABLE STATUS FROM `{dbName}` WHERE name REGEXP "{sqlPtrn}"'.format(
            dbName=self.dbName, sqlPtrn = sqlPtrn),
            cursor = self.conn.dict_cursor)

        tableInfo = cur.fetchall()

        # rename fields
        tableInfo = [{k.lower() : v for k,v in info.iteritems()} for info in tableInfo]

        for info in tableInfo:
            info['tier'] = tableTiers[tierRe.match(info['name']).group(1)] # lookup tier for the table based on tableName
            self._tableNames['%s.%s'%(self.package, camelCase(info['name']))] = info['name']
            self._headers[info['name']] = Header.initFromDatabase(self, info)

        if core.VERBOSE:
            print 'Loading dependices...'
        self.conn.loadDependencies(self)

    def erd(self, subset=None, prog='dot'):
        """
        plot the schema's entity relationship diagram (ERD).
        The layout programs can be 'dot' (default), 'neato', 'fdp', 'sfdp', 'circo', 'twopi'
        """
        if not subset:
            g = self.graph
        else:
            g = self.graph.copy()
            for i in g.nodes():
                if i not in subset:
                    g.remove_node(i)

        def tableList(tier):
            return [i for i in g if self.tables[i].tier==tier]

        pos=nx.graphviz_layout(g,prog=prog,args='')
        plt.figure(figsize=(8,8))
        nx.draw_networkx_edges(g, pos, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tableList('manual'),
                               node_color='g', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tableList('computed'),
                               node_color='r', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tableList('imported'),
                               node_color='b', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tableList('lookup'),
                               node_color='gray', node_size=120, alpha=0.3)
        nx.draw_networkx_labels(g, pos, nodelist = subset, font_weight='bold', font_size=9)
        nx.draw(g,pos,alpha=0,with_labels=False)
        plt.show()


    def __iter__(self):
        return self._headers.iterkeys()
