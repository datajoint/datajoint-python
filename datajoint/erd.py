import logging
import pyparsing as pp
import re
import networkx as nx
from networkx import DiGraph
from networkx import pygraphviz_layout
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import transforms
from collections import defaultdict

from . import DataJointError


logger = logging.getLogger(__name__)


class ERD:
    _parents = defaultdict(list)
    _children = defaultdict(list)
    _references = defaultdict(list)
    _referenced = defaultdict(list)

    def load_dependencies(self, connection, full_table_name):
        # fetch the CREATE TABLE statement
        cur = connection.query('SHOW CREATE TABLE %s' % full_table_name)
        create_statement = cur.fetchone()
        if not create_statement:
            raise DataJointError('Could not load the definition table %s' % full_table_name)
        create_statement = create_statement[1].split('\n')

        # build foreign key fk_parser
        database = full_table_name.split('.')[0].strip('`')
        add_database = lambda string, loc, toc: ['`{database}`.`{table}`'.format(database=database, table=toc[0])]

        # primary key parser
        pk_parser = pp.CaselessLiteral('PRIMARY KEY')
        pk_parser += pp.QuotedString('(', endQuoteChar=')').setResultsName('primary_key')

        # foreign key parser
        fk_parser = pp.CaselessLiteral('CONSTRAINT').suppress()
        fk_parser += pp.QuotedString('`').suppress()
        fk_parser += pp.CaselessLiteral('FOREIGN KEY').suppress()
        fk_parser += pp.QuotedString('(', endQuoteChar=')').setResultsName('attributes')
        fk_parser += pp.CaselessLiteral('REFERENCES')
        fk_parser += pp.Or([
            pp.QuotedString('`').setParseAction(add_database),
            pp.Combine(pp.QuotedString('`', unquoteResults=False) +
                       '.' + pp.QuotedString('`', unquoteResults=False))
            ]).setResultsName('referenced_table')
        fk_parser += pp.QuotedString('(', endQuoteChar=')').setResultsName('referenced_attributes')

        # parse foreign keys
        primary_key = None
        for line in create_statement:
            if primary_key is None:
                try:
                    result = pk_parser.parseString(line)
                except pp.ParseException:
                    pass
                else:
                    primary_key = [s.strip(' `') for s in result.primary_key.split(',')]
            try:
                result = fk_parser.parseString(line)
            except pp.ParseException:
                pass
            else:
                if not primary_key:
                    raise DataJointErrr('No primary key found %s' % full_table_name)
                if result.referenced_attributes != result.attributes:
                    raise DataJointError(
                        "%s's foreign key refers to differently named attributes in %s"
                        % (self.__class__.__name__, result.referenced_table))
                if all(q in primary_key for q in [s.strip('` ') for s in result.attributes.split(',')]):
                    self._parents[full_table_name].append(result.referenced_table)
                    self._children[result.referenced_table].append(full_table_name)
                else:
                    self._referenced[full_table_name].append(result.referenced_table)
                    self._references[result.referenced_table].append(full_table_name)

    @property
    def parents(self):
        return self._parents

    @property
    def children(self):
        return self._children

    @property
    def references(self):
        return self._references

    @property
    def referenced(self):
        return self._referenced


def to_camel_case(s):
    """
    Convert names with under score (_) separation
    into camel case names.
    Example:
    >>>to_camel_case("table_name")
        "TableName"
    """
    def to_upper(match):
        return match.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', to_upper, s)



class RelGraph(DiGraph):
    """
    A directed graph representing relations between tables within and across
    multiple databases found.
    """

    @property
    def node_labels(self):
        """
        :return: dictionary of key : label pairs for plotting
        """
        return {k: attr['label'] for k, attr in self.node.items()}

    @property
    def pk_edges(self):
        """
        :return: list of edges representing primary key foreign relations
        """
        return [edge for edge in self.edges()
                if self[edge[0]][edge[1]].get('rel')=='parent']

    @property
    def non_pk_edges(self):
        """
        :return: list of edges representing non primary key foreign relations
        """
        return [edge for edge in self.edges()
                if self[edge[0]][edge[1]].get('rel')=='referenced']

    def highlight(self, nodes):
        """
        Highlights specified nodes when plotting
        :param nodes: list of nodes to be highlighted
        """
        for node in nodes:
            self.node[node]['highlight'] = True

    def remove_highlight(self, nodes=None):
        """
        Remove highlights from specified nodes when plotting. If specified node is not
        highlighted to begin with, nothing happens.
        :param nodes: list of nodes to remove highlights from
        """
        if not nodes:
            nodes = self.nodes_iter()
        for node in nodes:
            self.node[node]['highlight'] = False

    # TODO: make this take in various config parameters for plotting
    def plot(self):
        """
        Plots an entity relation diagram (ERD) among all nodes that is part
        of the current graph.
        """
        if not self.nodes(): # There is nothing to plot
            logger.warning('Nothing to plot')
            return
        pos = pygraphviz_layout(self, prog='dot')
        fig = plt.figure(figsize=[10, 7])
        ax = fig.add_subplot(111)
        nx.draw_networkx_nodes(self, pos, node_size=200, node_color='g')
        text_dict = nx.draw_networkx_labels(self, pos, self.node_labels)
        trans = ax.transData + \
            transforms.ScaledTranslation(12/72, 0, fig.dpi_scale_trans)
        for text in text_dict.values():
            text.set_horizontalalignment('left')
            text.set_transform(trans)
        # draw primary key relations
        nx.draw_networkx_edges(self, pos, self.pk_edges, arrows=False)
        # draw non-primary key relations
        nx.draw_networkx_edges(self, pos, self.non_pk_edges, style='dashed', arrows=False)
        apos = np.array(list(pos.values()))
        xmax = apos[:, 0].max() + 200 #TODO: use something more sensible then hard fixed number
        xmin = apos[:, 0].min() - 100
        ax.set_xlim(xmin, xmax)
        ax.axis('off')  # hide axis

    def __repr__(self):
        pass

    def restrict_by_modules(self, modules, fill=False):
        """
        Creates a subgraph containing only tables in the specified modules.
        :param modules: list of module names
        :param fill: set True to automatically include nodes connecting two nodes in the specified modules
        :return: a subgraph with specified nodes
        """
        nodes = [n for n in self.nodes() if self.node[n].get('mod') in modules]
        if fill:
            nodes = self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables(self, tables, fill=False):
        """
        Creates a subgraph containing only specified tables.
        :param tables: list of tables to keep in the subgraph
        :param fill: set True to automatically include nodes connecting two nodes in the specified list
        of tables
        :return: a subgraph with specified nodes
        """
        nodes = [n for n in self.nodes() if self.node[n].get('label') in tables]
        if fill:
            nodes =  self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables_in_module(self, module, tables, fill=False):
        nodes = [n for n in self.nodes() if self.node[n].get('mod') and \
                 self.node[n].get('cls') in tables]
        if fill:
            nodes =  self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def fill_connection_nodes(self, nodes):
        """
        For given set of nodes, find and add nodes that serves as
        connection points for two nodes in the set.
        :param nodes: list of nodes for which connection nodes are to be filled in
        """
        graph = self.subgraph(self.ancestors_of_all(nodes))
        return graph.descendants_of_all(nodes)

    def ancestors_of_all(self, nodes):
        """
        Find and return a set of  all ancestors of the given
        nodes. The set will also contain the specified nodes.
        :param nodes: list of nodes for which ancestors are to be found
        :return: a set containing passed in nodes and all of their ancestors
        """
        s = set()
        for n in nodes:
            s.update(self.ancestors(n))
        return s

    def descendants_of_all(self, nodes):
        """
        Find and return a set including all descendants of the given
        nodes. The set will also contain the given nodes as well.
        :param nodes: list of nodes for which descendants are to be found
        :return: a set containing passed in nodes and all of their descendants
        """
        s = set()
        for n in nodes:
            s.update(self.descendants(n))
        return s

    def ancestors(self, node):
        """
        Find and return a set containing all ancestors of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        :param node: node for which all ancestors are to be discovered
        :return: a set containing the node and all of its ancestors
        """
        s = {node}
        for p in self.predecessors_iter(node):
            s.update(self.ancestors(p))
        return s

    def descendants(self, node):
        """
        Find and return a set containing all descendants of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        :param node: node for which all descendants are to be discovered
        :return: a set containing the node and all of its descendants
        """
        s = {node}
        for c in self.successors_iter(node):
            s.update(self.descendants(c))
        return s

    def up_down_neighbors(self, node, ups, downs, prev=None):
        """
        Returns a set of all nodes that can be reached from the specified node by
        moving up and down the ancestry tree with specific number of ups and downs.

        Example:
        up_down_neighbors(node, ups=2, downs=1) will return all nodes that can be reached by
        any combinations of two up tracing and 1 down tracing of the ancestry tree. This includes
        all children of a grand-parent (two ups and one down), all grand parents of all children (one down
        and then two ups), and all siblings parents (one up, one down, and one up).

        It must be noted that except for some special cases, there is no generalized interpretations for
        the relationship among nodes captured by this method. However, it does tend to produce a fairy
        good concise view of the relationships surrounding the specified node.


        :param node: node to base all discovery on
        :param ups: number of times to go up the ancestry tree (go up to parent)
        :param downs: number of times to go down the ancestry tree (go down to children)
        :param prev: previously visited node. This will be excluded from up down search in this recursion
        :return: a set of all nodes that can be reached within specified numbers of ups and downs from the source node
        """
        s = {node}
        if ups > 0:
            for x in self.predecessors_iter(node):
                if x == prev:
                    continue
                s.update(self.up_down_neighbors(x, ups-1, downs, node))
        if downs > 0:
            for x in self.successors_iter(node):
                if x == prev:
                    continue
                s.update(self.up_down_neighbors(x, ups, downs-1, node))
        return s

    def n_neighbors(self, node, n, prev=None):
        """
        Returns a set of n degree neighbors for the
        specified node. The set with contain the node itself.

        n degree neighbors are defined as node that can be reached
        within n edges from the root node.

        By default all edges (incoming and outgoing) will be followed.
        Set directed=True to follow only outgoing edges.
        """
        s = {node}
        if n < 1:
            return s
        if n == 1:
            s.update(self.predecessors(node))
            s.update(self.successors(node))
            return s
        for x in self.predecesors_iter():
            if x == prev:  # skip prev point
                continue
            s.update(self.n_neighbors(x, n-1, prev))
        for x in self.succesors_iter():
            if x == prev:
                continue
            s.update(self.n_neighbors(x, n-1, prev))
        return s



class DBConnGraph(RelGraph):
    """
    Represents relational structure of the databases and tables associated with a connection object
    """
    def __init__(self, conn, *args, **kwargs):
        """
        Initializes graph associated with a connection object
        :param conn: connection object for which the relational graph is to be constructed
        """
        # this is calling the networkx.DiGraph initializer
        super().__init__(*args, **kwargs)
        if conn.is_connected:
            self._conn = conn
        else:
            raise DataJointError('The connection is broken') #TODO: make better exception message
        self.update_graph()

    def full_table_to_class(self, full_table_name):
        """
        Converts full table reference of form `database`.`table` into the corresponding
        module_name.class_name format. For the module name, only the actual name of the module is used, with
        all of its package reference removed.
        :param full_table_name: full name of the table in the form `database`.`table`
        :return: name in the form of module_name.class_name if corresponding module and class exists
        """
        full_table_ptrn = re.compile(r'`(.*)`\.`(.*)`')
        m = full_table_ptrn.match(full_table_name)
        dbname = m.group(1)
        table_name = m.group(2)
        mod_name = self._conn.db_to_mod[dbname]
        mod_name = mod_name.split('.')[-1]
        class_name = to_camel_case(table_name)
        return '{}.{}'.format(mod_name, class_name)

    def update_graph(self, reload=False):
        """
        Update the connection graph. Set reload=True to cause the connection object's
        table heading information to be reloaded as well
        """
        if reload:
            self._conn.load_headings(force=True)

        self.clear()

        # create primary key foreign connections
        for table, parents in self._conn.parents.items():
            label = self.full_table_to_class(table)
            mod, cls = label.split('.')

            self.add_node(table, label=label,
                          mod=mod, cls=cls)
            for parent in parents:
                self.add_edge(parent, table, rel='parent')

        # create non primary key foreign connections
        for table, referenced in self._conn.referenced.items():
            for ref in referenced:
                self.add_edge(ref, table, rel='referenced')

    def copy_graph(self):
        """
        Return copy of the graph represented by this object at the
        time of call. Note that the returned graph is no longer
        bound to a connection.
        """
        return RelGraph(self)

    def subgraph(self, *args, **kwargs):
        return RelGraph(self).subgraph(*args, **kwargs)

