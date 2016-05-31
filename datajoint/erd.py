import networkx as nx
import numpy as np
import re
from scipy.optimize import basinhopping
import inspect
from . import Manual, Imported, Computed, Lookup, Part, DataJointError

user_relation_classes = (Manual, Lookup, Computed, Imported, Part)

def _get_concrete_subclasses(class_list):
    for cls in class_list:
        for subclass in cls.__subclasses__():
            if not inspect.isabstract(subclass):
                yield subclass
            yield from _get_concrete_subclasses([subclass])


def _get_tier(table_name):
    try:
        return next(tier for tier in user_relation_classes
                    if re.fullmatch(tier.tier_regexp, table_name))
    except StopIteration:
        return None


class ERD(nx.DiGraph):
    """
    Entity relationship diagram.

    Usage:
    >>>  erd = Erd(source)
    source can be a base relation object, a base relation class, a schema, or a module that has a schema
    or source can be a sequence of such objects.

    >>> erd.draw()
    draws the diagram using pyplot

    erd1 + erd2  - combines the two ERDs.
    erd + n   - adds n levels of successors
    erd - n   - adds n levens of predecessors
    Thus dj.ERD(schema.Table)+1-1 defines the diagram of immediate ancestors and descendants of schema.Table

    Note that erd + 1 - 1  may differ from erd - 1 + 1 and so forth.
    Only those tables that are loaded in the connection object are displayed
    """
    def __init__(self, source, include_parts=True):
        try:
            source[0]
        except TypeError:
            source = [source]

        try:
            connection = source[0].connection
        except AttributeError:
            try:
                connection = source[0].schema.connection
            except AttributeError:
                raise DataJointError('Could find database connection in %s' % repr(source[0]))

        connection.dependencies.load()
        super().__init__(connection.dependencies)

        self.nodes_to_show = set()
        for source in source:
            try:
                self.nodes_to_show.add(source.full_table_name)
            except AttributeError:
                try:
                    database = source.database
                except AttributeError:
                    try:
                        database = source.schema.database
                    except AttributeError:
                        raise DataJointError('Cannot plot ERD for %s' % repr(source))
                for node in self:
                    if node.startswith('`%s`' % database):
                        self.nodes_to_show.add(node)
        if not include_parts:
            self.nodes_to_show = set(n for n in self.nodes_to_show
                                     if not re.fullmatch(Part.tier_regexp, n.split('`')[-2]))

    def __sub__(self, other):
        try:
            self.nodes_to_show.difference_update(other.nodes_to_show)
        except AttributeError:
            nsteps = other
            for i in range(nsteps):
                new = nx.algorithms.boundary.node_boundary(nx.DiGraph(self).reverse(), self.nodes_to_show)
                if not new:
                    break
                self.nodes_to_show.update(new)
        return self

    def __add__(self, other):
        try:
            self.nodes_to_show.update(other.nodes_to_show)
        except AttributeError:
            nsteps = other
            for i in range(nsteps):
                new = nx.algorithms.boundary.node_boundary(self, self.nodes_to_show)
                if not new:
                    break
                self.nodes_to_show.update(new)
        return self

    def __mul__(self, other):
        self.nodes_to_show.intersection_update(other.nodes_to_show)
        return self

    def _make_graph(self, prefix_module):
        """
        Make the self.graph - a graph object ready for drawing
        """
        graph = nx.DiGraph(self).subgraph(self.nodes_to_show)
        node_colors = {   # http://matplotlib.org/examples/color/named_colors.html
            None: 'y',
            Manual: 'forestgreen',
            Lookup: 'gray',
            Computed: 'r',
            Imported: 'darkblue',
            Part: 'thistle'
        }
        color_mapping = {n: node_colors[_get_tier(n.split('`')[-2])] for n in graph};
        nx.set_node_attributes(graph, 'color', color_mapping)
        # relabel nodes to class names
        class_list = list(cls for cls in _get_concrete_subclasses(user_relation_classes))
        mapping = {cls.full_table_name: (cls._context['__name__'] + '.' if prefix_module else '') +
                                        (cls._master.__name__+'.' if issubclass(cls, Part) else '') + cls.__name__
                   for cls in class_list if cls.full_table_name in graph}
        new_names = [mapping.values()]
        if len(new_names) > len(set(new_names)):
            raise DataJointError('Some classes have identifical names. The ERD cannot be plotted.')
        nx.relabel_nodes(graph, mapping, copy=False)
        return graph

    def draw(self, pos=None, layout=None, prefix_module=True):
        if not self.nodes_to_show:
            print('There is nothing to plot')
            return
        graph = self._make_graph(prefix_module)
        if pos is None:
            pos = self._layout(graph) if layout is None else layout(graph)
        import matplotlib.pyplot as plt

        # plot manual
        nodelist = graph.nodes()
        node_colors = [graph.node[n]['color'] for n in nodelist]
        edge_list = graph.edges(data=True)
        edge_styles = ['solid' if e[2]['primary'] else 'dashed' for e in edge_list]
        nx.draw_networkx_edges(graph, pos=pos, edgelist=edge_list, style=edge_styles, alpha=0.2)
        for c in set(node_colors):
            bbox = dict(boxstyle='round', facecolor=c, alpha=0.3)
            nx.draw_networkx_labels(graph.subgraph([n for n in nodelist if graph.node[n]['color'] == c]),
                                    pos=pos, bbox=bbox, horizontalalignment='right')
        ax = plt.gca()
        ax.axis('off')
        ax.set_xlim([-0.4, 1.4])  # allow a margin for labels
        plt.show()

    @staticmethod
    def _layout(graph):
        """
        :param graph:  a networkx.DiGraph object
        :return: position dict keyed by node names
        """
        if not nx.is_directed_acyclic_graph(graph):
            DataJointError('This layout only works for acyclic graphs')

        # assign depths
        nodes = set(node for node in graph.nodes() if not graph.in_edges(node))  # root
        depth = 0
        depths = {}
        while nodes:
            depths = dict(depths, **dict.fromkeys(nodes, depth))
            nodes = set(edge[1] for edge in graph.out_edges(nodes))
            depth += 1

        # push depth down as far as possible
        updated = True
        while updated:
            updated = False
            for node in graph.nodes():
                if graph.successors(node):
                    m = min(depths[n] for n in graph.successors(node)) - 1
                    updated = m > depths[node]
                    depths[node] = m
        longest_path = nx.dag_longest_path(graph)  # place at x=0

        # assign initial x positions
        x = dict.fromkeys(graph, 0)
        unplaced = set(node for node in graph if node not in longest_path)
        for node in sorted(unplaced, key=graph.degree, reverse=True):
            neighbors = set(nx.all_neighbors(graph, node))
            placed_neighbors = neighbors.difference(unplaced)
            placed_other = set(graph.nodes()).difference(unplaced).difference(neighbors)
            x[node] = (sum(x[n] for n in placed_neighbors) -
                       sum(x[n] for n in placed_other) +
                       0.05*(np.random.ranf()-0.5))/(len(placed_neighbors) + len(placed_other) + 0.01)
            x[node] += 2*(x[node] > 0)-1
            unplaced.remove(node)

        n = graph.number_of_nodes()
        nodes = list(depths.keys())
        x = np.array([x[n] for n in nodes])
        depths = depth - np.array([depths[n] for n in nodes])

        #  minimize layout cost function (for x-coordinate only)
        A = np.asarray(nx.to_numpy_matrix(graph, dtype=bool))
        A = np.logical_or(A, A.transpose())
        D = np.zeros_like(A,dtype=bool)
        for d in set(depths):
            ix = depths == d
            D[np.outer(ix,ix)]=True
        D = np.logical_xor(D, np.identity(n, bool))

        def cost(xx):
            xx = np.expand_dims(xx, 1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            return h[A].sum() + 2*h[D].sum() + (1/h[D]).sum()

        def grad(xx):
            xx = np.expand_dims(xx, 1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            return -2*((A*g).sum(axis=1) + (D*g).sum(axis=1) - (D*g/h**2).sum(axis=1))

        x = basinhopping(cost, x, niter=100, T=10, stepsize=0.25, minimizer_kwargs=dict(jac=grad)).x

        # tilt left and up a bit
        y = depths + 0.35*x  # offset nodes slightly
        x -= 0.15*depths

        # normalize
        x -= x.min()
        x /= x.max() + 0.01
        y -= y.min()
        y /= y.max() + 0.01
        return {node: (x, y) for node, x, y in zip(nodes, x, y)}
