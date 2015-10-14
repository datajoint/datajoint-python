from matplotlib import transforms

import numpy as np

import logging
from collections import defaultdict
import pyparsing as pp
import networkx as nx
from networkx import DiGraph
from functools import cmp_to_key
import operator

from collections import OrderedDict

# use pygraphviz if available
try:
    from networkx import pygraphviz_layout
except:
    pygraphviz_layout = None

import matplotlib.pyplot as plt
from . import DataJointError
from functools import wraps
from .utils import to_camel_case
from .base_relation import BaseRelation

logger = logging.getLogger(__name__)

from inspect import isabstract


def get_concrete_descendants(cls):
    desc = []
    child= cls.__subclasses__()
    for c in child:
        if not isabstract(c):
            desc.append(c)
        desc.extend(get_concrete_descendants(c))
    return desc


def parse_base_relations(rels):
    name_map = {}
    for r in rels:
        try:
            name_map[r().full_table_name] = '{module}.{cls}'.format(module=r.__module__, cls=r.__name__)
        except TypeError:
            # skip if failed to instantiate BaseRelation derivative
            pass
    return name_map


def get_table_relation_name_map():
    rels = get_concrete_descendants(BaseRelation)
    return parse_base_relations(rels)


class ERD(DiGraph):
    """
    A directed graph representing dependencies between Relations within and across
    multiple databases.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def node_labels(self):
        """
        :return: dictionary of key : label pairs for plotting
        """
        name_map = get_table_relation_name_map()
        return {k: self.get_label(k, name_map) for k in self.nodes()}

    def get_label(self, node, name_map=None):
        label = self.node[node].get('label', '')
        if label.strip():
            return label

        # it's not efficient to recreate name-map on every call!
        if name_map is not None and node in name_map:
            return name_map[node]
        # no other name exists, so just use full table now
        return node

    @property
    def lone_nodes(self):
        """
        :return: list of nodes that are not connected to any other node
        """
        return list(x for x in self.root_nodes if len(self.out_edges(x)) == 0)

    @property
    def pk_edges(self):
        """
        :return: list of edges representing primary key foreign relations
        """
        return [edge for edge in self.edges()
                if self[edge[0]][edge[1]].get('rel') == 'parent']

    @property
    def non_pk_edges(self):
        """
        :return: list of edges representing non primary key foreign relations
        """
        return [edge for edge in self.edges()
                if self[edge[0]][edge[1]].get('rel') == 'referenced']

    def highlight(self, nodes):
        """
        Highlights specified nodes when plotting
        :param nodes: list of nodes, specified by full table names, to be highlighted
        """
        for node in nodes:
            self.node[node]['highlight'] = True

    def remove_highlight(self, nodes=None):
        """
        Remove highlights from specified nodes when plotting. If specified node is not
        highlighted to begin with, nothing happens.
        :param nodes: list of nodes, specified by full table names, to remove highlights from
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
        if pygraphviz_layout is None:
            logger.warning('Failed to load Pygraphviz - plotting not supported at this time')
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
        return self.repr_path()

    def restrict_by_databases(self, databases, fill=False):
        """
        Creates a subgraph containing only tables in the specified database.
        :param databases: list of database names
        :param fill: if True, automatically include nodes connecting two nodes in the specified modules
        :return: a subgraph with specified nodes
        """
        nodes = [n for n in self.nodes() if n.split('.')[0].strip('`') in databases]
        if fill:
            nodes = self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables(self, tables, fill=False):
        """
        Creates a subgraph containing only specified tables.
        :param tables: list of tables to keep in the subgraph. Tables are specified using full table names
        :param fill: set True to automatically include nodes connecting two nodes in the specified list
        of tables
        :return: a subgraph with specified nodes
        """
        nodes = [n for n in self.nodes() if n in tables]
        if fill:
            nodes = self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables_in_module(self, module, tables, fill=False):
        nodes = [n for n in self.nodes() if self.node[n].get('mod') in module and
                 self.node[n].get('cls') in tables]
        if fill:
            nodes = self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def fill_connection_nodes(self, nodes):
        """
        For given set of nodes, find and add nodes that serves as
        connection points for two nodes in the set.
        :param nodes: list of nodes for which connection nodes are to be filled in
        """
        graph = self.subgraph(self.ancestors_of_all(nodes))
        return graph.descendants_of_all(nodes)

    def ancestors_of_all(self, nodes, n=-1):
        """
        Find and return a set of  all ancestors of the given
        nodes. The set will also contain the specified nodes.
        :param nodes: list of nodes for which ancestors are to be found
        :param n: maximum number of generations to go up for each node.
        If set to a negative number, will return all ancestors.
        :return: a set containing passed in nodes and all of their ancestors
        """
        s = set()
        for node in nodes:
            s.update(self.ancestors(node, n))
        return s

    def descendants_of_all(self, nodes, n=-1):
        """
        Find and return a set including all descendants of the given
        nodes. The set will also contain the given nodes as well.
        :param nodes: list of nodes for which descendants are to be found
        :param n: maximum number of generations to go down for each node.
        If set to a negative number, will return all descendants.
        :return: a set containing passed in nodes and all of their descendants
        """
        s = set()
        for node in nodes:
            s.update(self.descendants(node, n))
        return s

    def copy_graph(self, *args, **kwargs):
        return self.__class__(self, *args, **kwargs)

    def ancestors(self, node, n=-1):
        """
        Find and return a set containing all ancestors of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        :param node: node for which all ancestors are to be discovered
        :param n: maximum number of generations to go up. If set to a negative number,
        will return all ancestors.
        :return: a set containing the node and all of its ancestors
        """
        s = {node}
        if n == 0:
            return s
        for p in self.predecessors_iter(node):
            s.update(self.ancestors(p, n-1))
        return s

    def descendants(self, node, n=-1):
        """
        Find and return a set containing all descendants of the specified
        node. For convenience in plotting, this set will also include
        the specified node as well (may change in future).
        :param node: node for which all descendants are to be discovered
        :param n: maximum number of generations to go down. If set to a negative number,
        will return all descendants
        :return: a set containing the node and all of its descendants
        """
        s = {node}
        if n == 0:
            return s
        for c in self.successors_iter(node):
            s.update(self.descendants(c, n-1))
        return s

    def up_down_neighbors(self, node, ups=2, downs=2, _prev=None):
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
        :param _prev: previously visited node. This will be excluded from up down search in this recursion
        :return: a set of all nodes that can be reached within specified numbers of ups and downs from the source node
        """
        s = {node}
        if ups > 0:
            for x in self.predecessors_iter(node):
                if x != _prev:
                    s.update(self.up_down_neighbors(x, ups-1, downs, node))
        if downs > 0:
            for x in self.successors_iter(node):
                if x != _prev:
                    s.update(self.up_down_neighbors(x, ups, downs-1, node))
        return s

    def n_neighbors(self, node, n, directed=False, prev=None):
        """
        Returns a set of n degree neighbors for the
        specified node. The set will contain the node itself.

        n degree neighbors are defined as node that can be reached
        within n edges from the root node.

        By default all edges (incoming and outgoing) will be followed.
        Set directed=True to follow only outgoing edges.
        """
        s = {node}
        if n == 1:
            s.update(self.predecessors(node))
            s.update(self.successors(node))
        elif n > 1:
            if not directed:
                for x in self.predecesors_iter():
                    if x != prev:  # skip prev point
                        s.update(self.n_neighbors(x, n-1, prev))
            for x in self.succesors_iter():
                if x != prev:
                    s.update(self.n_neighbors(x, n-1, prev))
        return s

    @property
    def root_nodes(self):
        return {node for node in self.nodes() if len(self.predecessors(node)) == 0}

    @property
    def leaf_nodes(self):
        return {node for node in self.nodes() if len(self.successors(node)) == 0}

    def nodes_by_depth(self):
        """
        Return all nodes, ordered by their depth in the hierarchy
        :returns: list of nodes, ordered by depth from shallowest to deepest
        """
        ret = defaultdict(lambda: 0)
        roots = self.root_nodes

        def recurse(node, depth):
            if depth > ret[node]:
                ret[node] = depth
            for child in self.successors_iter(node):
                recurse(child, depth+1)

        for root in roots:
            recurse(root, 0)

        return sorted(ret.items(), key=operator.itemgetter(1))

    def get_longest_path(self):
        """
        :returns: a list of graph nodes defining th longest path in the graph
        """
        # no path exists if there is not an edge!
        if not self.edges():
            return []

        node_depth_list = self.nodes_by_depth()
        node_depth_lookup = dict(node_depth_list)
        path = []

        leaf = node_depth_list[-1][0]
        predecessors = [leaf]
        while predecessors:
            leaf = sorted(predecessors, key=node_depth_lookup.get)[-1]
            path.insert(0, leaf)
            predecessors = self.predecessors(leaf)

        return path

    def remove_edges_in_path(self, path):
        """
        Removes all shared edges between this graph and the path
        :param path: a list of nodes defining a path. All edges in this path will be removed from the graph if found
        """
        if len(path) <= 1: # no path exists!
            return
        for a, b in zip(path[:-1], path[1:]):
            self.remove_edge(a, b)

    def longest_paths(self):
        """
        :return: list of paths from longest to shortest. A path is a list of nodes.
        """
        g = self.copy_graph()
        paths = []
        path = g.get_longest_path()
        while path:
            paths.append(path)
            g.remove_edges_in_path(path)
            path = g.get_longest_path()
        return paths

    def repr_path(self):
        """
        Construct string representation of the erm, summarizing dependencies between
        tables
        :return: string representation of the erm
        """
        if len(self) == 0:
            return "No relations to show"

        paths = self.longest_paths()

        # turn comparator into Key object for use in sort
        k = cmp_to_key(self.compare_path)
        sorted_paths = sorted(paths, key=k)

        # table name will be padded to match the longest table name
        node_labels = self.node_labels
        n = max([len(x) for x in node_labels.values()]) + 1
        rep = ''
        for path in sorted_paths:
            rep += self.repr_path_with_depth(path, n)

        for node in self.lone_nodes:
            rep += node_labels[node] + '\n'

        return rep

    def compare_path(self, path1, path2):
        """
        Comparator between two paths: path1 and path2 based on a combination of rules.
        Path 1 is greater than path2 if:
        1) i^th node in path1 is at greater depth than the i^th node in path2 OR
        2) if i^th nodes are at the same depth, i^th node in path 1 is alphabetically less than i^th node
        in path 2
        3) if neither of the above statement is true even if path1 and path2 are switched, proceed to i+1^th node
        If path2 is a subpath start at node 1, then path1 is greater than path2
        :param path1: path 1 of 2 to be compared
        :param path2: path 2 of 2 to be compared
        :return: return 1 if path1 is greater than path2, -1 if path1 is less than path2, and 0 if they are identical
        """
        node_depth_lookup = dict(self.nodes_by_depth())
        for node1, node2 in zip(path1, path2):
            if node_depth_lookup[node1] != node_depth_lookup[node2]:
                return -1 if node_depth_lookup[node1] < node_depth_lookup[node2] else 1
            if node1 != node2:
                return -1 if node1 < node2 else 1
        if len(node1) != len(node2):
            return -1 if len(node1) < len(node2) else 1
        return 0

    def repr_path_with_depth(self, path, n=20, m=2):
        node_depth_lookup = dict(self.nodes_by_depth())
        node_labels = self.node_labels
        space = '-' * n
        rep = ''
        prev_depth = 0
        first = True
        for (i, node) in enumerate(path):
            depth = node_depth_lookup[node]
            label = node_labels[node]
            if first:
                rep += (' '*(n+m))*(depth-prev_depth)
            else:
                rep += space.join(['-'*m]*(depth-prev_depth))[:-1] + '>'
            first = False
            prev_depth = depth
            if i == len(path)-1:
                rep += label
            else:
                rep += label.ljust(n, '-')
        rep += '\n'
        return rep

    @classmethod
    def create_from_dependencies(cls, dependencies, *args, **kwargs):
        obj = cls(*args, **kwargs)

        for full_table, parents in dependencies.parents.items():
            database, table = (x.strip('`') for x in full_table.split('.'))
            obj.add_node(full_table, database=database, table=table)
            for parent in parents:
                obj.add_edge(parent, full_table, rel='parent')

        # create non primary key foreign connections
        for full_table, referenced in dependencies.referenced.items():
            for ref in referenced:
                obj.add_edge(ref, full_table, rel='referenced')

        return obj
