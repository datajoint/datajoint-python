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
        conn.packages[dbname] = package
        self.package = package 
        self.dbname = dbname
        self.reload()


    def __repr__(self):
        ret = 'datajoint.Schema "{package}" -> "{dbname}" at {host}:{port}\n {tableList}\n ({nTables} tables)'.format(
            package=self.package, dbname=self.dbname,
            tableList = '\n'.join(self.tables.keys()),
            nTables=len(self.tables), **self.conn.connInfo)
        return ret



    def reload(self):
        """
        load table definitions and dependencies   
        """

        print 'Loading table dependencies...'
        cur = self.conn.query('''
        SELECT DISTINCT table_schema, table_name, referenced_table_schema, referenced_table_name
            FROM information_schema.key_column_usage
            WHERE table_schema="{schema}" AND referenced_table_name is not NULL
            OR referenced_table_schema="{schema}"
        '''.format(schema=self.dbname))
        deps = cur.fetchall()
        allTables = tuple({(str(a[0]),str(a[1])) for a in deps if tierRe.match(a[1])}.union(
            {(str(a[2]),str(a[3])) for a in deps if tierRe.match(a[1])}))

        print 'Loading table info...'
        cur = self.conn.query('''
            SELECT table_schema, table_name, table_comment
            FROM information_schema.tables WHERE (table_schema, table_name) in {allTables}
            '''.format(allTables=allTables))

        TableTuple = collections.namedtuple('TableTuple',
            ('name','comment','tier','header','parents','children'))

        self.tables = collections.OrderedDict()
        for s in cur.fetchall():
            self.tables[self.conn.makeClassName(s[0],s[1])] = TableTuple(
                name = s[1],
                comment = s[2].split('$')[0],
                tier = tableTiers[tierRe.match(s[1]).group(1)],
                header = collections.OrderedDict(),
                parents = [],
                children = [])

        # construct the dependencies
        for s in deps:
            self.tables[self.conn.makeClassName(s[0],s[1])].parents.append(
                self.conn.makeClassName(s[2], s[3]))
            self.tables[self.conn.makeClassName(s[2],s[3])].children.append(
                self.conn.makeClassName(s[0], s[1]))

        self.graph = nx.DiGraph()
        for k,v in self.tables.iteritems():
            for child in v.children:
                self.graph.add_edge(k,child)

        print 'Loading column info...'
        cur = self.conn.query('''
            SELECT table_schema, table_name, column_name, (column_key="PRI") AS `iskey`,
                column_type, (is_nullable="YES") AS isnullable,
                column_comment, column_default
            FROM information_schema.columns
            WHERE (table_schema, column_name) in {allTables}
            '''.format(allTables=allTables))

        FieldTuple = collections.namedtuple('FieldTuple',
            ('isKey','type','isNullable','comment','default','isNumeric','isString','isBlob'))

        for s in cur.fetchall():
            tup = FieldTuple(
                isKey = s[3]!=0,
                type = s[4],
                isNullable = s[5]!=0,
                comment = s[6],
                default = s[7],
                isNumeric = None != re.match('^((tiny|small|medium|big)?int|decimal|double|float)', s[4]),
                isString = None != re.match('^((var)?char|enum|date|timestamp)', s[4]),
                isBlob = None != re.match('^(tiny|medium|long)?blob', s[4])
            )
            # check for unsupported datatypes
            if not (tup.isNumeric or tup.isString or tup.isBlob):
                raise TypeError('Unsupported DataJoint datatype ' + tup.type)
            self.tables[self.conn.makeClassName(s[0],s[1])].header[s[2]] = tup



    def erd(self, prog='dot', subset=None):
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
