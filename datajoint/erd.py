import networkx as nx
import re
import functools
import io
import warnings

try:
    from matplotlib import pyplot as plt
    from networkx.drawing.nx_pydot import pydot_layout
    erd_active = True
except:
    erd_active = False

from . import Manual, Imported, Computed, Lookup, Part, DataJointError
from .base_relation import lookup_class_name


user_relation_classes = (Manual, Lookup, Computed, Imported, Part)


class _AliasNode:
    """
    special class to indicate aliased foreign keys
    """
    pass


def _get_tier(table_name):
    if not table_name.startswith('`'):
        return _AliasNode
    else:
        try:
            return next(tier for tier in user_relation_classes
                        if re.fullmatch(tier.tier_regexp, table_name.split('`')[-2]))
        except StopIteration:
            return None


if not erd_active:
    class ERD:
        """
        Entity relationship diagram, currently disabled due to the lack of required packages: matplotlib and pygraphviz.

        To enable ERD feature, please install both matplotlib and pygraphviz. For instructions on how to install
        these two packages, refer to http://docs.datajoint.io/setup/Install-and-connect.html#python and
        http://tutorials.datajoint.io/setting-up/datajoint-python.html
        """

        def __init__(self, *args, **kwargs):
            warnings.warn('ERD functionality depends on matplotlib and pygraphviz. Please install both of these '
                          'libraries to enable the ERD feature.')
else:
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
        def __init__(self, source, context=None):

            if isinstance(source, ERD):
                # copy constructor
                self.nodes_to_show = set(source.nodes_to_show)
                self.context = source.context
                super().__init__(source)
                return

            # get the caller's locals()
            if context is None:
                import inspect
                frame = inspect.currentframe()
                try:
                    context = frame.f_back.f_locals
                finally:
                    del frame
            self.context = context

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

            self = ERD(self)  # copy
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

        def _make_graph(self):
            """
            Make the self.graph - a graph object ready for drawing
            """
            # include aliased nodes
            gaps = set(nx.algorithms.boundary.node_boundary(self, self.nodes_to_show)).intersection(
                nx.algorithms.boundary.node_boundary(nx.DiGraph(self).reverse(), self.nodes_to_show))
            nodes = self.nodes_to_show.union(a for a in gaps if a.isdigit)
            # construct subgraph and rename nodes to class names
            graph = nx.DiGraph(self).subgraph(nodes)
            nx.set_node_attributes(graph, 'node_type', {n: _get_tier(n) for n in graph})
            # relabel nodes to class names
            mapping = {node: (lookup_class_name(node, self.context) or node) for node in graph.nodes()}
            new_names = [mapping.values()]
            if len(new_names) > len(set(new_names)):
                raise DataJointError('Some classes have identical names. The ERD cannot be plotted.')
            nx.relabel_nodes(graph, mapping, copy=False)
            return graph

        def make_dot(self):
            import networkx as nx

            graph = self._make_graph()
            graph.nodes()

            scale = 1.2   # scaling factor for fonts and boxes
            label_props = {  # http://matplotlib.org/examples/color/named_colors.html
                None: dict(shape='circle', color="#FFFF0040", fontcolor='yellow', fontsize=round(scale*8),
                           size=0.4*scale, fixed=False),
                _AliasNode: dict(shape='circle', color="#FF880080", fontcolor='white', fontsize=round(scale*6),
                                 size=0.15*scale, fixed=True),
                Manual: dict(shape='box', color="#00FF0030", fontcolor='darkgreen', fontsize=round(scale*10),
                             size=0.4*scale, fixed=False),
                Lookup: dict(shape='plaintext', color='#00000020', fontcolor='black', fontsize=round(scale*8),
                             size=0.4*scale, fixed=False),
                Computed: dict(shape='ellipse', color='#FF000020', fontcolor='#7F0000A0', fontsize=round(scale*10),
                               size=0.3*scale, fixed=True),
                Imported: dict(shape='ellipse', color='#00007F40', fontcolor='#00007FA0', fontsize=round(scale*10),
                               size=0.4*scale, fixed=False),
                Part: dict(shape='plaintext', color='#0000000', fontcolor='black', fontsize=round(scale*8),
                           size=0.1*scale, fixed=False)}
            node_props = {node: label_props[d['node_type']] for node, d in dict(graph.nodes(data=True)).items()}

            dot = nx.drawing.nx_pydot.to_pydot(graph)
            for node in dot.get_nodes():
                node.set_shape('circle')
                name = node.get_name().strip('"')
                props = node_props[name]
                node.set_fontsize(props['fontsize'])
                node.set_fontcolor(props['fontcolor'])
                node.set_shape(props['shape'])
                node.set_fontname('arial')
                node.set_fixedsize('shape' if props['fixed'] else False)
                node.set_width(props['size'])
                node.set_height(props['size'])
                node.set_label(name)
                # node.set_margin(0.05)
                node.set_color(props['color'])
                node.set_style('filled')

            for edge in dot.get_edges():
                # see http://www.graphviz.org/content/attrs
                src = edge.get_source().strip('"')
                dest = edge.get_destination().strip('"')
                props = graph.get_edge_data(src, dest)
                edge.set_color('#00000040')
                edge.set_style('solid' if props['primary'] else 'dashed')
                master_part = graph.node[dest]['node_type'] is Part and dest.startswith(src+'.')
                edge.set_weight(3 if master_part else 1)
                edge.set_arrowhead('none')
                edge.set_penwidth(.75 if props['multi'] else 2)

            return dot

        def make_svg(self):
            from IPython.display import SVG
            return SVG(self.make_dot().create_svg())

        def make_png(self):
            return io.BytesIO(self.make_dot().create_png())

        def make_image(self):
            return plt.imread(self.make_png())

        def _repr_svg_(self):
            return self.make_svg()._repr_svg_()

        def draw(self):
            plt.imshow(self.make_image())
            plt.gca().axis('off')
            plt.show()

        def save(self, filename, format=None):
            if format is None:
                if filename.lower().endswith('.png'):
                    format = 'png'
                elif filename.lower().endswith('.svg'):
                    format = 'svg'
            if format.lower() == 'png':
                with open(filename, 'wb') as f:
                    f.write(self.make_png().getbuffer().tobytes())
            elif format.lower() == 'svg':
                with open(filename, 'w') as f:
                    f.write(self.make_svg().data)
            else:
                raise DataJointError('Unsupported file format')

        @staticmethod
        def _layout(graph, **kwargs):
            return pydot_layout(graph, prog='dot', **kwargs)
