import re, collections
from conn import conn as djconn
import networkx as nx
import matplotlib.pyplot as plt

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
    datajoint.Schema objects link a python module (package) with a database schema
    """
    conn = None
    package = None
    dbname = None


    def __init__(self, package=package, dbname=dbname, conn=None):
        if conn is None:
            conn = djconn()
        self.conn = conn
        self.package = package 
        self.dbname = dbname
        self.reload()


    def __repr__(self):
        ret = 'datajoint.Schema "{package}" -> "{dbname}" at {host}:{port}\n {tableList}\n ({nTables} tables)'.format(
            package=self.package, dbname=self.dbname,
            tableList = '\n'.join(self.tables.keys()),
            nTables=len(self.tables), **self.conn.connInfo)
        return ret


    def makeClassName(self, tableName):
        """
        make a class name from the table name.
        """
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
            ('name','comment','tier','header','level','parents','children'))

        self.tables = collections.OrderedDict()
        for s in cur.fetchall():
            if tierRe.match(s[0]):
                self.tables[self.makeClassName(s[0])] = TableTuple( 
                    name = s[0], 
                    comment = s[1].split('$')[0], 
                    tier = tableTiers[tierRe.match(s[0]).group(1)],
                    header = collections.OrderedDict(),
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
                self.tables[self.makeClassName(s[0])].header[s[1]] = tup


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

        self.graph = nx.DiGraph()
        for k,v in self.tables.iteritems():
            for child in v.children:
                self.graph.add_edge(k,child)


    def erd(self, prog='dot'):
        """
        plot the schema's entity relationship diagram (ERD).
        The layout programs can be 'dot' (default), 'neato', 'fdp', 'sfdp', 'circo', 'twopi'
        """
        def tableList(tier):
            return [i for i in self.graph if self.tables[i].tier==tier]
        pos=nx.graphviz_layout(self.graph,prog=prog,args='')
        plt.figure(figsize=(8,8))
        nx.draw_networkx_edges(self.graph,pos, alpha=0.3)
        nx.draw_networkx_nodes(self.graph, pos, nodelist=tableList('manual'),
                               node_color='g', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(self.graph, pos, nodelist=tableList('computed'),
                               node_color='r', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(self.graph, pos, nodelist=tableList('imported'),
                               node_color='b', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(self.graph, pos, nodelist=tableList('lookup'),
                               node_color='gray', node_size=120, alpha=0.3)
        nx.draw_networkx_labels(self.graph,pos,font_weight='bold',font_size=9)
        nx.draw(self.graph,pos,alpha=0,with_labels=False)
        plt.show()
