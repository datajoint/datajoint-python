from .core import DataJointError, camelCase
import networkx as nx
from networkx import DiGraph
from networkx import pygraphviz_layout
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import transforms
import re
import logging

logger = logging.getLogger(__name__)

class RelGraph(DiGraph):
    """
    Represents relations between tables and databases
    """

    @property
    def node_labels(self):
        return {k:attr['label'] for k,attr in self.node.items()}

    @property
    def pk_edges(self):
        return [edge for edge in self.edges() \
                if self[edge[0]][edge[1]].get('rel')=='parent']

    @property
    def nonpk_edges(self):
        return [edge for edge in self.edges() \
                if self[edge[0]][edge[1]].get('rel')=='referenced']

    def highlight(nodes):
        for node in nodes:
            self.node[node]['highlight'] = True

    def remove_highlight(nodes=None):
        if not nodes:
            nodes = self.nodes_iter()
        for node in nodes:
            self.node[node]['highlight'] = False

    def plot(self):
        if not self.nodes(): # There is nothing to plot
            logger.warning('No table to plot in ERD')
            return
        pos = pygraphviz_layout(self, prog='dot')
        fig = plt.figure(figsize=[10,7])
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
        nx.draw_networkx_edges(self, pos, self.nonpk_edges, style='dashed', arrows=False)
        apos = np.array(list(pos.values()))
        xmax = apos[:,0].max() + 200 #TODO: use something more sensible then hard fixed number
        xmin = apos[:,0].min() - 100
        ax.set_xlim(xmin, xmax)
        ax.axis('off') # hide axis

    def restrict_by_modules(self, modules, fill=False):
        nodes = [n for n in self.nodes() if self.node[n].get('mod') in modules]
        if fill:
            nodes =  self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables(self, tables, fill=False):
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
        """
        H = self.subgraph(self.ancestors_of_all(nodes))
        return H.descendents_of_all(nodes)

    def ancestors_of_all(self, nodes):
        """
        Find and return a set including all ancestors of the given
        nodes. The set will also contain the given nodes as well.
        """
        s = set()
        for n in nodes:
            s.update(self.ancestors(n))
        return s

    def descendents_of_all(self, nodes):
        """
        Find and return a set including all descendents of the given
        nodes. The set will also contain the given nodes as well.
        """
        s = set()
        for n in nodes:
            s.update(self.descendents(n))
        return s

    def ancestors(self, node):
        """
        Find and return a set containing all ancestors of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        """
        s = {node}
        for p in self.predecessors_iter(node):
            s.update(self.ancestors(p))
        return s

    def descendents(self, node):
        """
        Find and return a set containing all descendents of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        """
        s = {node}
        for c in self.successors_iter(node):
            s.update(self.descendents(c))
        return s

    def updown_neighbors(self, node, ups, downs, prev=None):
        s = {node}
        if ups > 0:
            for x in G.predecessors_iter(node):
                if x==prev:
                    continue
                s.update(self.updown_neighbors(x, ups-1, downs, node))
        if downs > 0:
            for x in G.successors_iter(node):
                if x==prev:
                    continue
                s.update(self.updown_neighbors(x, ups, downs-1, node))
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
            s.update(G.predecessors(node))
            s.update(G.successors(node))
            return s
        for x in G.predecesors_iter():
            if x == prev:  # skip prev point
                continue
            s.update(n_neighbors(G, x, n-1, prev))
        for x in G.succesors_iter():
            if x == prev:
                continue
            s.update(n_neighbors(G, x, n-1, prev))
        return s

full_table_ptrn = re.compile(r'`(.*)`\.`(.*)`')

class DBConnGraph(RelGraph):
    """
    Represents relational structure of the
    connected databases
    """
    def __init__(self, conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if conn.isConnected:
            self._conn = conn
        else:
            raise DataJointError('The connection is broken') #TODO: make better exception message
        self.update_graph()

    def full_table_to_class(self, full_table_name):
        m = full_table_ptrn.match(full_table_name)
        dbname = m.group(1)
        table_name = m.group(2)
        mod_name = self._conn.modules[dbname]
        class_name = camelCase(table_name)
        return '{}.{}'.format(mod_name, class_name)

    def update_graph(self):
        self.clear()
        for table, parents in self._conn.parents.items():
            label = self.full_table_to_class(table)
            mod, cls = label.split('.')

            self.add_node(table, label=label, \
                          mod=mod, cls=cls)
            for parent in parents:
                self.add_edge(parent, table, rel='parent')

        for table, referenced in self._conn.referenced.items():
            for ref in referenced:
                self.add_edge(ref, table, rel='referenced')

    def copy_graph(self):
        """
        Return copy of the graph represented by this object at the
        time of call. Note that the returned graph is no longer
        bound to connection.
        """
        return RelGraph(self)

    def subgraph(self, *args, **kwargs):
        return  RelGraph(self).subgraph(*args, **kwargs)

