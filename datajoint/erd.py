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
            module = r.__module__
            parts = []
            if module != '__main__':
                parts.append(module.split('.')[-1])
            parts.append(r.__name__)
            name_map[r().full_table_name] = '.'.join(parts)
        except:
            pass
    return name_map


def get_table_relation_name_map():
    rels = get_concrete_descendants(BaseRelation)
    return parse_base_relations(rels)


class RelGraph(DiGraph):
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

        return '.'.join(x.strip('`') for x in node.split('.'))

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

    def restrict_by_modules(self, modules, fill=False):
        """
        DEPRECATED - to be removed
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
        :param tables: list of tables to keep in the subgraph. Tables are specified using full table names
        :param fill: set True to automatically include nodes connecting two nodes in the specified list
        of tables
        :return: a subgraph with specified nodes
        """
        nodes = [n for n in self.nodes() if self.node[n].get('label') in tables]
        if fill:
            nodes = self.fill_connection_nodes(nodes)
        return self.subgraph(nodes)

    def restrict_by_tables_in_module(self, module, tables, fill=False):
        nodes = [n for n in self.nodes() if self.node[n].get('mod') in module and
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

    def copy_graph(self, *args, **kwargs):
        return self.__class__(self, *args, **kwargs)

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


def require_dep_loading(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        self.load_dependencies()
        return f(self, *args, **kwargs)
    return wrapper

class ERM(RelGraph):
    """
    Entity Relation Map

    Represents known relation between tables
    """
    # _checked_dependencies = set()

    def __init__(self, conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._databases = set()
        self._conn = conn
        self._parents = dict()
        self._referenced = dict()
        self._children = defaultdict(list)
        self._references = defaultdict(list)
        if conn.is_connected:
            self._conn = conn
        else:
            raise DataJointError('The connection is broken') #TODO: make better exception message

    def update_graph(self, reload=False):
        self.clear()
        # create primary key foreign connections
        for table, parents in self._parents.items():
            mod, cls = (x.strip('`') for x in table.split('.'))
            self.add_node(table)
            for parent in parents:
                self.add_edge(parent, table, rel='parent')

        # create non primary key foreign connections
        for table, referenced in self._referenced.items():
            for ref in referenced:
                self.add_edge(ref, table, rel='referenced')

    @require_dep_loading
    def copy_graph(self, *args, **kwargs):
        """
        Return copy of the graph represented by this object at the
        time of call. Note that the returned graph is no longer
        bound to a connection.
        """
        return RelGraph(self, *args, **kwargs)

    @require_dep_loading
    def subgraph(self, *args, **kwargs):
        return RelGraph(self).subgraph(*args, **kwargs)

    def register_database(self, database):
        """
        Register the database to be monitored
        :param database: name of database to be monitored
        """
        self._databases.add(database)

    def load_dependencies(self):
        """
        Load dependencies for all monitored databases
        """
        for database in self._databases:
            self.load_dependencies_for_database(database)

    def load_dependencies_for_database(self, database):
        """
        Load dependencies for all tables found in the specified database
        :param database: database for which dependencies will be loaded
        """
        #sql_table_name_regexp = re.compile('^(#|_|__|~)?[a-z][a-z0-9_]*$')

        cur = self._conn.query('SHOW TABLES FROM `{database}`'.format(database=database))

        for info in cur:
            table_name = info[0]
            # TODO: fix this criteria! It will exclude ANY tables ending with 'jobs'
            # exclude tables ending with 'jobs' from erd
            if not table_name == '~jobs':
                full_table_name = '`{database}`.`{table_name}`'.format(database=database, table_name=table_name)
                self.load_dependencies_for_table(full_table_name)

    def load_dependencies_for_table(self, full_table_name):
        """
        Load dependencies for the specified table
        :param full_table_name: table for which dependencies will be loaded, specified in full table name
        """
        # check if already loaded.  Use clear_dependencies before reloading
        if full_table_name in self._parents:
            return
        self._parents[full_table_name] = list()
        self._referenced[full_table_name] = list()

        # fetch the CREATE TABLE statement
        cur = self._conn.query('SHOW CREATE TABLE %s' % full_table_name)
        create_statement = cur.fetchone()
        if not create_statement:
            raise DataJointError('Could not load the definition for %s' % full_table_name)
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
                    raise DataJointError('No primary key found %s' % full_table_name)
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
        self.update_graph()

    def __repr__(self):
        # Make sure that all dependencies are loaded before printing repr
        self.load_dependencies()
        return super().__repr__()

    def clear_dependencies(self):
        pass

    def clear_dependencies_for_database(self, database):
        pass

    def clear_dependencies_for_table(self, full_table_name):
        for ref in self._parents.pop(full_table_name, []):
            if full_table_name in self._children[ref]:
                self._children[ref].remove(full_table_name)
        for ref in self._referenced.pop(full_table_name, []):
            if full_table_name in self._references[ref]:
                self._references[ref].remove(full_table_name)

    @property
    @require_dep_loading
    def parents(self):
        return self._parents

    @property
    @require_dep_loading
    def children(self):
        self.load_dependencies()
        return self._children

    @property
    @require_dep_loading
    def references(self):
        return self._references

    @property
    @require_dep_loading
    def referenced(self):
        return self._referenced

    @require_dep_loading
    def get_descendants(self, full_table_name):
        """
        :param full_table_name: a table name in the format `database`.`table_name`
        :return: list of all children and references, in order of dependence.
        This is helpful for cascading delete or drop operations.
        """
        ret = defaultdict(lambda: 0)

        def recurse(full_table_name, level):
            if level > ret[full_table_name]:
                ret[full_table_name] = level
            for child in self.children[full_table_name] + self.references[full_table_name]:
                recurse(child, level+1)

        recurse(full_table_name, 0)
        return sorted(ret.keys(), key=ret.__getitem__)


