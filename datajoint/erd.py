import networkx as nx
import numpy as np
import re
from scipy.optimize import minimize
import inspect
from . import schema, Manual, Imported, Computed, Lookup, Part, DataJointError
from .user_relations import UserRelation


def _get_concrete_subclasses(cls):
    for subclass in cls.__subclasses__():
        print('Subclass: ', subclass.__name__, ('-- abstract ' if inspect.isabstract(subclass) else ''))
        if not inspect.isabstract(subclass):
            yield subclass
        yield from _get_concrete_subclasses(subclass)


def _get_tier(table_name):
    try:
        return next(tier for tier in (Manual, Lookup, Computed, Imported, Part)
                    if re.fullmatch(tier.tier_regexp, table_name))
    except StopIteration:
        return None


class ERD(nx.DiGraph):
    """
    Entity relationship diagram.

    Usage:
    >>>  erd = Erd(source)
    where source may be a datajoint.Schema, a datajoint.BaseRelation, or sequence of such objects.

    >>> erd.draw()
    draws the diagram using pyplot

    erd + n   - adds n levels of successors
    erd - n   - adds n levens of predecessors
    Thus dj.ERD(schema.Table)+1-1 defines the diagram of immediate ancestors and descendants of schema.Table

    Note that erd + 1 - 1  may differ from erd - 1 + 1 and so forth.
    Only those tables that are loaded in the connection object are displayed
    """
    def __init__(self, source):
        try:
            connection = source.connection
            source = [source]
        except AttributeError:
            connection = source[0].connection
        connection.dependencies.load()   # reload all dependencies
        super().__init__(connection.dependencies)

        self.nodes_to_show = set()
        for source in source:
            if isinstance(source, UserRelation):
                self.nodes_to_show.add(source.full_table_name)
            elif isinstance(source, schema):
                for node in self:
                    if node.startswith('`%s`' % source.database):
                        self.nodes_to_show.add(node)

    def __sub__(self, nsteps):
        for i in range(nsteps):
            new = nx.algorithms.boundary.node_boundary(nx.DiGraph(self).reverse(), self.nodes_to_show)
            if not new:
                break
            self.nodes_to_show.update(new)
        return self

    def __add__(self, nsteps):
        for i in range(nsteps):
            new = nx.algorithms.boundary.node_boundary(self, self.nodes_to_show)
            if not new:
                break
            self.nodes_to_show.update(new)
        return self

    def draw(self, pos=None, layout=None):

        graph = nx.DiGraph(self).subgraph(self.nodes_to_show)

        node_colors = {
            None: 'white',
            Manual: 'black',
            Lookup: 'gray',
            Computed: 'red',
            Imported: 'blue',
            Part: 'violet'
        }
        color_mapping = {n: node_colors[_get_tier(n.split('`')[-2])] for n in graph};
        nx.set_node_attributes(graph, 'color', color_mapping)

        # relabel nodes to class names
        rels = list(_get_concrete_subclasses(UserRelation))

        mapping = {rel.full_table_name: rel.__name__
                   for rel in _get_concrete_subclasses(UserRelation)
                   if rel.full_table_name in graph}
        nx.relabel_nodes(graph, mapping, copy=False)

        # generate layout
        if pos is None:
            pos = self._layout(graph) if layout is None else layout(graph)

        import matplotlib.pyplot as plt
        nx.draw_networkx(graph, pos=pos)
        ax = plt.gca()
        ax.axis('off')
        ax.set_xlim([-0.2, 1.2])  # allow a margin for labels

    @staticmethod
    def _layout(graph):
        """
        :param graph:  a networkx.DiGraph object
        :return:
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

        # assign initial x positions
        longest_path = nx.dag_longest_path(graph)   # place at x=0
        x = dict.fromkeys(graph, 0)
        unplaced = set(node for node in graph if node not in longest_path)
        for node in sorted(unplaced, key=graph.degree, reverse=True):
            neighbors = set(nx.all_neighbors(graph, node))
            placed_neighbors = neighbors.difference(unplaced)
            placed_other = set(graph.nodes()).difference(unplaced).difference(neighbors)
            x[node] = (sum(x[n] for n in placed_neighbors) -
                       sum(x[n] for n in placed_other) +
                       0.05*(np.random.ranf()-0.5))/(len(placed_neighbors) + len(placed_other) + 0.01)
            x[node] += (x[node] > 0)-0.5
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
            xx = np.expand_dims(xx,1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            return (
                h[A].sum() +
                2*h[D].sum() +
                (1/h[D]).sum())

        def grad(xx):
            xx = np.expand_dims(xx,1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            return -(
                2*(A*g).sum(axis=1) +
                2*(D*g).sum(axis=1) -
                2*(D*g/h**2).sum(axis=1))

        x = minimize(cost, x, jac=grad, method='BFGS', options={'gtol': 1e-4, 'disp': True}).x

        # normalize
        y = depths + 0.2*np.cos(2*np.pi * x)  # offset nodes slightly
        x -= x.min()
        x /= x.max()
        y -= y.min()
        y /= y.max()
        return {node: (x, y) for node, x, y in zip(nodes, x, y)}

