import networkx as nx
import re
import functools
import io
import logging
import inspect
from .table import Table
from .dependencies import unite_master_parts
from .user_tables import Manual, Imported, Computed, Lookup, Part
from .errors import DataJointError
from .table import lookup_class_name


try:
    from matplotlib import pyplot as plt

    plot_active = True
except:
    plot_active = False

try:
    from networkx.drawing.nx_pydot import pydot_layout

    diagram_active = True
except:
    diagram_active = False


logger = logging.getLogger(__name__.split(".")[0])
user_table_classes = (Manual, Lookup, Computed, Imported, Part)


class _AliasNode:
    """
    special class to indicate aliased foreign keys
    """

    pass


def _get_tier(table_name):
    if not table_name.startswith("`"):
        return _AliasNode
    else:
        try:
            return next(
                tier
                for tier in user_table_classes
                if re.fullmatch(tier.tier_regexp, table_name.split("`")[-2])
            )
        except StopIteration:
            return None


if not diagram_active:

    class Diagram:
        """
        Entity relationship diagram, currently disabled due to the lack of required packages: matplotlib and pygraphviz.

        To enable Diagram feature, please install both matplotlib and pygraphviz. For instructions on how to install
        these two packages, refer to http://docs.datajoint.io/setup/Install-and-connect.html#python and
        http://tutorials.datajoint.io/setting-up/datajoint-python.html
        """

        def __init__(self, *args, **kwargs):
            logger.warning(
                "Please install matplotlib and pygraphviz libraries to enable the Diagram feature."
            )

else:

    class Diagram(nx.DiGraph):
        """
        Entity relationship diagram.

        Usage:

        >>>  diag = Diagram(source)

        source can be a base table object, a base table class, a schema, or a module that has a schema.

        >>> diag.draw()

        draws the diagram using pyplot

        diag1 + diag2  - combines the two diagrams.
        diag + n   - expands n levels of successors
        diag - n   - expands n levels of predecessors
        Thus dj.Diagram(schema.Table)+1-1 defines the diagram of immediate ancestors and descendants of schema.Table

        Note that diagram + 1 - 1  may differ from diagram - 1 + 1 and so forth.
        Only those tables that are loaded in the connection object are displayed
        """

        def __init__(self, source, context=None):
            if isinstance(source, Diagram):
                # copy constructor
                self.nodes_to_show = set(source.nodes_to_show)
                self.context = source.context
                super().__init__(source)
                return

            # get the caller's context
            if context is None:
                frame = inspect.currentframe().f_back
                self.context = dict(frame.f_globals, **frame.f_locals)
                del frame
            else:
                self.context = context

            # find connection in the source
            try:
                connection = source.connection
            except AttributeError:
                try:
                    connection = source.schema.connection
                except AttributeError:
                    raise DataJointError(
                        "Could not find database connection in %s" % repr(source[0])
                    )

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
                        raise DataJointError(
                            "Cannot plot Diagram for %s" % repr(source)
                        )
                for node in self:
                    if node.startswith("`%s`" % database):
                        self.nodes_to_show.add(node)

        @classmethod
        def from_sequence(cls, sequence):
            """
            The join Diagram for all objects in sequence

            :param sequence: a sequence (e.g. list, tuple)
            :return: Diagram(arg1) + ... + Diagram(argn)
            """
            return functools.reduce(lambda x, y: x + y, map(Diagram, sequence))

        def add_parts(self):
            """
            Adds to the diagram the part tables of tables already included in the diagram
            :return:
            """

            def is_part(part, master):
                """
                :param part:  `database`.`table_name`
                :param master:   `database`.`table_name`
                :return: True if part is part of master.
                """
                part = [s.strip("`") for s in part.split(".")]
                master = [s.strip("`") for s in master.split(".")]
                return (
                    master[0] == part[0]
                    and master[1] + "__" == part[1][: len(master[1]) + 2]
                )

            self = Diagram(self)  # copy
            self.nodes_to_show.update(
                n
                for n in self.nodes()
                if any(is_part(n, m) for m in self.nodes_to_show)
            )
            return self

        def topological_sort(self):
            """:return:  list of nodes in topological order"""
            return unite_master_parts(
                list(
                    nx.algorithms.dag.topological_sort(
                        nx.DiGraph(self).subgraph(self.nodes_to_show)
                    )
                )
            )

        def __add__(self, arg):
            """
            :param arg: either another Diagram or a positive integer.
            :return: Union of the diagrams when arg is another Diagram
                     or an expansion downstream when arg is a positive integer.
            """
            self = Diagram(self)  # copy
            try:
                self.nodes_to_show.update(arg.nodes_to_show)
            except AttributeError:
                try:
                    self.nodes_to_show.add(arg.full_table_name)
                except AttributeError:
                    for i in range(arg):
                        new = nx.algorithms.boundary.node_boundary(
                            self, self.nodes_to_show
                        )
                        if not new:
                            break
                        # add nodes referenced by aliased nodes
                        new.update(
                            nx.algorithms.boundary.node_boundary(
                                self, (a for a in new if a.isdigit())
                            )
                        )
                        self.nodes_to_show.update(new)
            return self

        def __sub__(self, arg):
            """
            :param arg: either another Diagram or a positive integer.
            :return: Difference of the diagrams when arg is another Diagram or
                     an expansion upstream when arg is a positive integer.
            """
            self = Diagram(self)  # copy
            try:
                self.nodes_to_show.difference_update(arg.nodes_to_show)
            except AttributeError:
                try:
                    self.nodes_to_show.remove(arg.full_table_name)
                except AttributeError:
                    for i in range(arg):
                        graph = nx.DiGraph(self).reverse()
                        new = nx.algorithms.boundary.node_boundary(
                            graph, self.nodes_to_show
                        )
                        if not new:
                            break
                        # add nodes referenced by aliased nodes
                        new.update(
                            nx.algorithms.boundary.node_boundary(
                                graph, (a for a in new if a.isdigit())
                            )
                        )
                        self.nodes_to_show.update(new)
            return self

        def __mul__(self, arg):
            """
            Intersection of two diagrams
            :param arg: another Diagram
            :return: a new Diagram comprising nodes that are present in both operands.
            """
            self = Diagram(self)  # copy
            self.nodes_to_show.intersection_update(arg.nodes_to_show)
            return self

        def _make_graph(self):
            """
            Make the self.graph - a graph object ready for drawing
            """
            # mark "distinguished" tables, i.e. those that introduce new primary key
            # attributes
            for name in self.nodes_to_show:
                foreign_attributes = set(
                    attr
                    for p in self.in_edges(name, data=True)
                    for attr in p[2]["attr_map"]
                    if p[2]["primary"]
                )
                self.nodes[name]["distinguished"] = (
                    "primary_key" in self.nodes[name]
                    and foreign_attributes < self.nodes[name]["primary_key"]
                )
            # include aliased nodes that are sandwiched between two displayed nodes
            gaps = set(
                nx.algorithms.boundary.node_boundary(self, self.nodes_to_show)
            ).intersection(
                nx.algorithms.boundary.node_boundary(
                    nx.DiGraph(self).reverse(), self.nodes_to_show
                )
            )
            nodes = self.nodes_to_show.union(a for a in gaps if a.isdigit)
            # construct subgraph and rename nodes to class names
            graph = nx.DiGraph(nx.DiGraph(self).subgraph(nodes))
            nx.set_node_attributes(
                graph, name="node_type", values={n: _get_tier(n) for n in graph}
            )
            # relabel nodes to class names
            mapping = {
                node: lookup_class_name(node, self.context) or node
                for node in graph.nodes()
            }
            new_names = [mapping.values()]
            if len(new_names) > len(set(new_names)):
                raise DataJointError(
                    "Some classes have identical names. The Diagram cannot be plotted."
                )
            nx.relabel_nodes(graph, mapping, copy=False)
            return graph

        @staticmethod
        def _encapsulate_edge_attributes(graph):
            """
            Modifies the `nx.Graph`'s edge attribute `attr_map` to be a string representation
            of the attribute map, and encapsulates the string in double quotes.
            Changes the graph in place.

            Implements workaround described in
            https://github.com/pydot/pydot/issues/258#issuecomment-795798099
            """
            for u, v, *_, edgedata in graph.edges(data=True):
                if "attr_map" in edgedata:
                    graph.edges[u, v]["attr_map"] = '"{0}"'.format(edgedata["attr_map"])

        @staticmethod
        def _encapsulate_node_names(graph):
            """
            Modifies the `nx.Graph`'s node names string representations encapsulated in
            double quotes.
            Changes the graph in place.

            Implements workaround described in
            https://github.com/datajoint/datajoint-python/pull/1176
            """
            nx.relabel_nodes(
                graph,
                {node: '"{0}"'.format(node) for node in graph.nodes()},
                copy=False,
            )

        def make_dot(self):
            graph = self._make_graph()
            graph.nodes()

            scale = 1.2  # scaling factor for fonts and boxes
            label_props = {  # http://matplotlib.org/examples/color/named_colors.html
                None: dict(
                    shape="circle",
                    color="#FFFF0040",
                    fontcolor="yellow",
                    fontsize=round(scale * 8),
                    size=0.4 * scale,
                    fixed=False,
                ),
                _AliasNode: dict(
                    shape="circle",
                    color="#FF880080",
                    fontcolor="#FF880080",
                    fontsize=round(scale * 0),
                    size=0.05 * scale,
                    fixed=True,
                ),
                Manual: dict(
                    shape="box",
                    color="#00FF0030",
                    fontcolor="darkgreen",
                    fontsize=round(scale * 10),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Lookup: dict(
                    shape="plaintext",
                    color="#00000020",
                    fontcolor="black",
                    fontsize=round(scale * 8),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Computed: dict(
                    shape="ellipse",
                    color="#FF000020",
                    fontcolor="#7F0000A0",
                    fontsize=round(scale * 10),
                    size=0.3 * scale,
                    fixed=True,
                ),
                Imported: dict(
                    shape="ellipse",
                    color="#00007F40",
                    fontcolor="#00007FA0",
                    fontsize=round(scale * 10),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Part: dict(
                    shape="plaintext",
                    color="#0000000",
                    fontcolor="black",
                    fontsize=round(scale * 8),
                    size=0.1 * scale,
                    fixed=False,
                ),
            }
            node_props = {
                node: label_props[d["node_type"]]
                for node, d in dict(graph.nodes(data=True)).items()
            }

            self._encapsulate_node_names(graph)
            self._encapsulate_edge_attributes(graph)
            dot = nx.drawing.nx_pydot.to_pydot(graph)
            for node in dot.get_nodes():
                node.set_shape("circle")
                name = node.get_name().strip('"')
                props = node_props[name]
                node.set_fontsize(props["fontsize"])
                node.set_fontcolor(props["fontcolor"])
                node.set_shape(props["shape"])
                node.set_fontname("arial")
                node.set_fixedsize("shape" if props["fixed"] else False)
                node.set_width(props["size"])
                node.set_height(props["size"])
                if name.split(".")[0] in self.context:
                    cls = eval(name, self.context)
                    assert issubclass(cls, Table)
                    description = cls().describe(context=self.context).split("\n")
                    description = (
                        (
                            "-" * 30
                            if q.startswith("---")
                            else (
                                q.replace("->", "&#8594;")
                                if "->" in q
                                else q.split(":")[0]
                            )
                        )
                        for q in description
                        if not q.startswith("#")
                    )
                    node.set_tooltip("&#13;".join(description))
                node.set_label(
                    "<<u>" + name + "</u>>"
                    if node.get("distinguished") == "True"
                    else name
                )
                node.set_color(props["color"])
                node.set_style("filled")

            for edge in dot.get_edges():
                # see https://graphviz.org/doc/info/attrs.html
                src = edge.get_source()
                dest = edge.get_destination()
                props = graph.get_edge_data(src, dest)
                if props is None:
                    raise DataJointError(
                        "Could not find edge with source "
                        "'{}' and destination '{}'".format(src, dest)
                    )
                edge.set_color("#00000040")
                edge.set_style("solid" if props["primary"] else "dashed")
                master_part = graph.nodes[dest][
                    "node_type"
                ] is Part and dest.startswith(src + ".")
                edge.set_weight(3 if master_part else 1)
                edge.set_arrowhead("none")
                edge.set_penwidth(0.75 if props["multi"] else 2)

            return dot

        def make_svg(self):
            from IPython.display import SVG

            return SVG(self.make_dot().create_svg())

        def make_png(self):
            return io.BytesIO(self.make_dot().create_png())

        def make_image(self):
            if plot_active:
                return plt.imread(self.make_png())
            else:
                raise DataJointError("pyplot was not imported")

        def _repr_svg_(self):
            return self.make_svg()._repr_svg_()

        def draw(self):
            if plot_active:
                plt.imshow(self.make_image())
                plt.gca().axis("off")
                plt.show()
            else:
                raise DataJointError("pyplot was not imported")

        def save(self, filename, format=None):
            if format is None:
                if filename.lower().endswith(".png"):
                    format = "png"
                elif filename.lower().endswith(".svg"):
                    format = "svg"
            if format.lower() == "png":
                with open(filename, "wb") as f:
                    f.write(self.make_png().getbuffer().tobytes())
            elif format.lower() == "svg":
                with open(filename, "w") as f:
                    f.write(self.make_svg().data)
            else:
                raise DataJointError("Unsupported file format")

        @staticmethod
        def _layout(graph, **kwargs):
            return pydot_layout(graph, prog="dot", **kwargs)
