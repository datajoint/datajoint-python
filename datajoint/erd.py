import networkx as nx
import re
import numpy as np
import functools
from scipy.optimize import basinhopping
import itertools
from . import Manual, Imported, Computed, Lookup, Part, DataJointError
from .base_relation import lookup_class_name


user_relation_classes = (Manual, Lookup, Computed, Imported, Part)


def _get_tier(table_name):
    try:
        return next(tier for tier in user_relation_classes
                    if re.fullmatch(tier.tier_regexp, table_name.split('`')[-2]))
    except StopIteration:
        return None


class ERD(nx.DiGraph):
    """
    Entity relationship diagram.

    Usage:

    >>>  erd = Erd(source)

    source can be a base relation object, a base relation class, a schema, or a module that has a schema.

    >>> erd.draw()

    draws the diagram using pyplot

    erd1 + erd2  - combines the two ERDs.
    erd + n   - adds n levels of successors
    erd - n   - adds n levels of predecessors
    Thus dj.ERD(schema.Table)+1-1 defines the diagram of immediate ancestors and descendants of schema.Table

    Note that erd + 1 - 1  may differ from erd - 1 + 1 and so forth.
    Only those tables that are loaded in the connection object are displayed
    """
    def __init__(self, source):

        if isinstance(source, ERD):
            # copy constructor
            self.nodes_to_show = set(source.nodes_to_show)
            super().__init__(source)
            return

        # find connection in the source
        try:
            connection = source.connection
        except AttributeError:
            try:
                connection = source.schema.connection
            except AttributeError:
                raise DataJointError('Could not find database connection in %s' % repr(source[0]))

        # initialize graph from dependencies
        connection.dependencies.load()
        super().__init__(connection.dependencies)

        # Enumerate nodes from all the items in the list
        self.nodes_to_show = set()
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

    @classmethod
    def from_sequence(cls, sequence):
        """
        The join ERD for all objects in sequence
        :param sequence: a sequence (e.g. list, tuple)
        :return: ERD(arg1) + ... + ERD(argn)
        """
        return functools.reduce(lambda x, y: x+y, map(ERD, sequence))

    def add_parts(self):
        """
        Adds to the diagram the part tables of tables already included in the diagram
        :return:
        """
        def is_part(part, master):
            """
            :param part:  `database`.`table_name`
            :param master:   `database`.`table_name`
            :return: True if part is part of master,
            """
            part = [s.strip('`') for s in part.split('.')]
            master = [s.strip('`') for s in master.split('.')]
            return master[0] == part[0] and master[1] + '__' == part[1][:len(master[1])+2]

        self = ERD(self)  #  copy
        self.nodes_to_show.update(n for n in self.nodes() if any(is_part(n, m) for m in self.nodes_to_show))
        return self

    def __add__(self, arg):
        """
        :param arg: either another ERD or a positive integer.
        :return: Union of the ERDs when arg is another ERD or an expansion downstream when arg is a positive integer.
        """
        self = ERD(self)   # copy
        try:
            self.nodes_to_show.update(arg.nodes_to_show)
        except AttributeError:
            try:
                self.nodes_to_show.add(arg.full_table_name)
            except AttributeError:
                for i in range(arg):
                    new = nx.algorithms.boundary.node_boundary(self, self.nodes_to_show)
                    if not new:
                        break
                    self.nodes_to_show.update(new)
        return self

    def __sub__(self, arg):
        """
        :param arg: either another ERD or a positive integer.
        :return: Difference of the ERDs when arg is another ERD or an expansion upstream when arg is a positive integer.
        """
        self = ERD(self)   # copy
        try:
            self.nodes_to_show.difference_update(arg.nodes_to_show)
        except AttributeError:
            try:
                self.nodes_to_show.remove(arg.full_table_name)
            except AttributeError:
                for i in range(arg):
                    new = nx.algorithms.boundary.node_boundary(nx.DiGraph(self).reverse(), self.nodes_to_show)
                    if not new:
                        break
                    self.nodes_to_show.update(new)
        return self

    def __mul__(self, arg):
        """
        Intersection of two ERDs
        :param arg: another ERD
        :return: a new ERD comprising nodes that are present in both operands.
        """
        self = ERD(self)   # copy
        self.nodes_to_show.intersection_update(arg.nodes_to_show)
        return self

    def _make_graph(self, context):
        """
        Make the self.graph - a graph object ready for drawing
        """
        graph = nx.DiGraph(self).subgraph(self.nodes_to_show)
        nx.set_node_attributes(graph, 'node_type', {n: _get_tier(n) for n in graph})
        # relabel nodes to class names
        mapping = {node: (lookup_class_name(node, context) or node) for node in graph.nodes()}
        new_names = [mapping.values()]
        if len(new_names) > len(set(new_names)):
            raise DataJointError('Some classes have identical names. The ERD cannot be plotted.')
        nx.relabel_nodes(graph, mapping, copy=False)
        return graph

    def draw(self, pos=None, layout=None, context=None, font_scale=1.5, **layout_options):   # pragma: no cover
        """
        Draws the graph of dependencies.
        :param pos: dict with positions for every node.  If None, then layout is called.
        :param layout: the graph layout function. If None, then self._layout is used.
        :param context: the context in which to look for the class names.  If None, the caller's context is used.
        :param font_scale: the scalar used to scale all the fonts.
        :param layout_options:  kwargs passed into the layout function.
        """
        if not self.nodes_to_show:
            print('There is nothing to plot')
            return
        if context is None:
            # get the caller's locals()
            import inspect
            frame = inspect.currentframe()
            try:
                context = frame.f_back.f_locals
            finally:
                del frame

        graph = self._make_graph(context)
        if pos is None:
            pos = (layout if layout else self._layout)(graph, **layout_options)
        import matplotlib.pyplot as plt

        edge_list = graph.edges(data=True)
        edge_styles = ['solid' if e[2]['primary'] else 'dashed' for e in edge_list]
        nx.draw_networkx_edges(graph, pos=pos, edgelist=edge_list, style=edge_styles, alpha=0.2)

        label_props = {  # http://matplotlib.org/examples/color/named_colors.html
            None: dict(bbox=dict(boxstyle='round,pad=0.1', facecolor='yellow', alpha=0.3), size=round(font_scale*8)),
            Manual: dict(bbox=dict(boxstyle='round,pad=0.1', edgecolor='white', facecolor='darkgreen', alpha=0.3), size=round(font_scale*10)),
            Lookup: dict(bbox=dict(boxstyle='round,pad=0.1', edgecolor='white', facecolor='gray', alpha=0.2), size=round(font_scale*8)),
            Computed: dict(bbox=dict(boxstyle='round,pad=0.1', edgecolor='white', facecolor='red', alpha=0.2), size=round(font_scale*10)),
            Imported: dict(bbox=dict(boxstyle='round,pad=0.1', edgecolor='white', facecolor='darkblue', alpha=0.2), size=round(font_scale*10)),
            Part: dict(size=round(font_scale*7))}
        ax = plt.gca()
        for node in graph.nodes(data=True):
            ax.text(pos[node[0]][0], pos[node[0]][1], node[0],
                    horizontalalignment=('right' if pos[node[0]][0] < 0.5 else 'left'),
                    **label_props[node[1]['node_type']])
        ax = plt.gca()
        ax.axis('off')
        ax.set_xlim([-0.4, 1.4])  # allow a margin for labels
        plt.show()

    @staticmethod
    def _layout(graph, quality=2):
        """
        :param graph:  a networkx.DiGraph object
        :param quality: 0=dirty, 1=draft, 2=good, 3=great, 4=publish
        :return: position dict keyed by node names
        """
        if not nx.is_directed_acyclic_graph(graph):  # pragma: no cover
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
                    updated = updated or m > depths[node]
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

        nodes = nx.topological_sort(graph)
        x = np.array([x[n] for n in nodes])

        intersecting_edge_pairs = list(
            [[nodes.index(n) for n in edge1],
             [nodes.index(n) for n in edge2]]
            for edge1, edge2 in itertools.combinations(graph.edges(), 2)
            if len(set(edge1 + edge2)) == 4 and (
                depths[edge1[1]] > depths[edge2[0]] and
                depths[edge2[1]] > depths[edge1[0]]))
        depths = depth - np.array([depths[n] for n in nodes])

        #  minimize layout cost function (for x-coordinate only)
        A = np.asarray(nx.to_numpy_matrix(graph, dtype=bool))   # adjacency matrix
        A = np.logical_or(A, A.transpose())
        D = np.zeros_like(A,dtype=bool)         # neighbor matrix
        for d in set(depths):
            ix = depths == d
            D[np.outer(ix,ix)]=True
        D = np.logical_xor(D, np.identity(len(nodes), bool))

        def cost(xx):
            xx = np.expand_dims(xx, 1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            crossings = sum((xx[edge1[0]][0] > xx[edge2[0]][0]) != (xx[edge1[1]][0] > xx[edge2[1]][0])
                            for edge1, edge2 in intersecting_edge_pairs)
            return crossings*1000 + h[A].sum() + 0.1*h[D].sum() + (1/h[D]).sum()

        def grad(xx):
            xx = np.expand_dims(xx, 1)
            g = xx.transpose()-xx
            h = g**2 + 1e-8
            return -2*((A*g).sum(axis=1) + 0.1*(D*g).sum(axis=1) - (D*g/h**2).sum(axis=1))
        niter = [100, 200, 500, 1000, 3000][quality]
        maxiter = [1, 2, 3, 4, 4][quality]
        x = basinhopping(cost, x, niter=niter, interval=40, T=30, stepsize=1.0, disp=False,
                         minimizer_kwargs=dict(jac=grad, options=dict(maxiter=maxiter))).x
        # normalize coordinates to unit square
        phi = np.pi*20/180   # rotate coordinate slightly
        cs, sn = np.cos(phi), np.sin(phi)
        x, depths = cs*x - sn*depths,  sn*x + cs*depths
        x -= x.min()
        x /= x.max()+0.01
        depths -= depths.min()
        depths = depths/(depths.max()+0.01)
        return {node: (x, y) for node, x, y in zip(nodes, x, depths)}
