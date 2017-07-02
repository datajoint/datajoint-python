import networkx as nx
import re
import functools
from . import Manual, Imported, Computed, Lookup, Part, DataJointError
from .base_relation import lookup_class_name
from networkx.drawing.nx_agraph import graphviz_layout

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

        import matplotlib.pyplot as plt
        import matplotlib.lines as lines
        import matplotlib.patches as patches
        import matplotlib.path as path
        import numpy as np

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


        ax = plt.gca()
        for u, v, d in graph.edges(data=True):
            tail = np.array(pos[u])
            head = np.array(pos[v])
            # ax.add_patch(patches.FancyArrowPatch(
            #     head*0.6+tail*0.4, head*0.4+tail*0.6,
            #     connectionstyle='arc3',
            #     mutation_scale=20,
            #     color='black',
            #     alpha=0.2))
            # #
            # l = lines.Line2D([tail[0], head[0]], [tail[1], head[1]],
            #            color='black',
            #            linewidth=1,
            #            solid_capstyle='round',
            #            linestyle='solid' if d['primary'] else 'dashed',
            #            alpha=0.1)
            # ax.add_line(l)

            p = np.array([0, 0.2])
            q = np.array([0, 0.1])
            ppp = patches.PathPatch(
                path.Path([tail, tail*(1-p)+head*p, tail*p+head*(1-p), tail*q+head*(1-q), tail],
                          [path.Path.MOVETO, path.Path.CURVE3, path.Path.CURVE3, path.Path.CURVE3, path.Path.CLOSEPOLY]),
                color='black', fc="none", transform=ax.transData, alpha=0.2, linestyle=':')
            ax.add_patch(ppp)

        label_props = {  # http://matplotlib.org/examples/color/named_colors.html
            None: dict(fontdict=dict(color='yellow', size=round(font_scale*8))),
            Manual: dict(fontdict=dict(color='darkgreen', size=round(font_scale*10))),
            Lookup: dict(fontdict=dict(color='gray', size=round(font_scale*8))),
            Computed: dict(fontdict=dict(color='darkred', size=round(font_scale*10))),
            Imported: dict(fontdict=dict(color='mediumblue', size=round(font_scale*10))),
            Part: dict(fontdict=dict(color='black', size=round(font_scale*7), weight='light'))}

        for node in graph.nodes(data=True):
            ax.text(pos[node[0]][0], pos[node[0]][1], node[0],
                    horizontalalignment='left', verticalalignment='bottom', rotation=10,
                    **label_props[node[1]['node_type']])
        ax = plt.gca()
        ax.axis('off')
        ax.autoscale()
        plt.show()

    @staticmethod
    def _layout(graph, **kwargs):
        return graphviz_layout(graph, prog='dot', **kwargs)